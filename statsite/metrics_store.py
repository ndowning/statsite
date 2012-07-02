"""
Contains the base metrics store class and default metrics store class.
"""

import socket
import threading
import logging
import fnmatch

class MetricsStore(object):
    """
    This is the base class for all metric stores. There is only one
    main method that metric stores must implement: :meth:`flush()`
    which is called from time to time to flush data out to the
    backing store.

    Metrics stores _must_ be threadsafe, since :meth:`flush()` could
    potentially be called by multiple flushing aggregators.
    """

    def __init__(self):

        # List of functions called for each metric value sent to this store.
        # The (key, value, timestamp) tuple is passed to the filter function,
        # which returns true to allow the metric through.
        self.filters = []

    def _filter_metrics(self, metrics):
        if len(self.filters) == 0:
            return metrics

        return [m for m in metrics if any(f(m) for f in self.filters)]

    def _flush(self, metrics):
        """
        This method is called by aggregators when flushing data after the
        store's filters have been applied.
        This must be thread-safe.

       :Parameters:
        - `metrics` : A list of (key,value,timestamp) tuples.
        """
        raise NotImplementedError("flush not implemented")

    def flush(self, metrics):
        """
        This method is called by aggregators when flushing data.
        This must be thread-safe.

       :Parameters:
        - `metrics` : A list of (key,value,timestamp) tuples.
        """
        filtered_metrics = self._filter_metrics(metrics)
        if len(filtered_metrics) > 0: self._flush(filtered_metrics)

    def load(self, settings):
        filters = settings.pop('filters', '').strip()

        self.filters = []

        for filter in [f.strip() for f in filters.split(',')]:
            if len(filter) > 0:
                self.filters.append(self._create_filter(filter))

    def _create_filter(self, pattern):
        return lambda name: fnmatch.fnmatch(name, pattern)

class GraphiteStore(MetricsStore):
    def __init__(self, host="localhost", port=2003, prefix="statsite", attempts=3):
        """
        Implements a metrics store interface that allows metrics to
        be persisted to Graphite. Raises a :class:`ValueError` on bad arguments.

        :Parameters:
            - `host` : The hostname of the graphite server.
            - `port` : The port of the graphite server
            - `prefix` (optional) : A prefix to add to the keys. Defaults to 'statsite'
            - `attempts` (optional) : The number of re-connect retries before failing.
        """
        super(GraphiteStore, self).__init__()

        if port <= 0: raise ValueError, "Port must be positive!"
        if attempts <= 1: raise ValueError, "Must have at least 1 attempt!"

        self.host = host
        self.port = port
        self.prefix = prefix
        self.attempts = attempts
        self.sock_lock = threading.Lock()
        self.sock = None
        self.logger = logging.getLogger("statsite.graphitestore")

    def load(self, settings):
        super(GraphiteStore, self).load(settings)

        self.host = settings.pop('host', self.host)
        self.prefix = settings.pop('prefix', self.prefix)

        port = int(settings.pop('port', self.port))
        if port <= 0: raise ValueError, "Port must be positive!"
        self.port = port

        attempts = int(settings.pop('attempts', self.attempts))
        if attempts <= 1: raise ValueError, "Must have at least 1 attempt!"
        self.attempts = attempts

    def open(self):
        if self.sock is not None: raise Error, "Store is already open"
        self.sock = self._create_socket()

    def _create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        return sock

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def _flush(self, metrics):
        """
        Flushes the metrics provided to Graphite.

       :Parameters:
        - `metrics` : A list of (key,value,timestamp) tuples.
        """
        # Construct the output
        data = "\n".join(["%s.%s %s %d" % (self.prefix,k,v,ts) for k,v,ts in metrics]) + "\n"

        # Serialize writes to the socket
        self.sock_lock.acquire()
        try:
            self._write_metric(data)
        except:
            self.logger.exception("Failed to write out the metrics!")
        finally:
            self.sock_lock.release()

    def _write_metric(self, metric):
        """Tries to write a string to the socket, reconnecting on any errors"""
        for attempt in xrange(self.attempts):
            try:
                self.sock.sendall(metric)
                return
            except socket.error:
                self.logger.exception("Error while flushing to graphite. Reattempting...")
                self.sock = self._create_socket()

        self.logger.critical("Failed to flush to Graphite! Gave up after %d attempts." % self.attempts)

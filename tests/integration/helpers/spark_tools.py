import logging

import pyspark


class ResilientSparkSession:
    """Wrapper around SparkSession that automatically restarts on JVM/py4j failures.

    Under LLVM coverage instrumentation, Spark/JVM operations run significantly
    slower and can timeout or crash.  When the JVM dies, py4j raises errors like
    ``Py4JNetworkError`` or ``AttributeError: 'NoneType' ...``.  A dead session
    poisons every subsequent test that shares it (module/package scope).

    This wrapper detects those failures and transparently recreates the session.
    """

    def __init__(self, create_session_fn):
        self._create = create_session_fn
        self._session = create_session_fn()

    def _restart(self):
        logging.warning("Spark session is dead, restarting...")
        try:
            self._session.stop()
        except Exception:
            pass
        # Clear any cached singleton so getOrCreate builds a fresh one
        pyspark.sql.SparkSession.builder._options = {}
        try:
            pyspark.sql.SparkSession._instantiatedSession = None
        except Exception:
            pass
        self._session = self._create()
        logging.warning("Spark session restarted successfully")

    def _is_alive(self):
        try:
            self._session.sparkContext._jsc.sc().defaultParallelism()
            return True
        except Exception:
            return False

    def __getattr__(self, name):
        if not self._is_alive():
            self._restart()
        return getattr(self._session, name)

    def stop(self):
        try:
            self._session.stop()
        except Exception:
            pass

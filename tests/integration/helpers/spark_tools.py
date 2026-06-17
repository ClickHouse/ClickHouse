import logging
import os

import pyspark


def write_spark_log_config(log_dir):
    """Create a log4j2 properties file that writes Spark logs to a file.

    Returns the path to the properties file so it can be passed to Spark via
    spark.driver.extraJavaOptions.
    """
    os.makedirs(log_dir, exist_ok=True)
    spark_log_path = os.path.join(log_dir, "spark.log")
    props_path = os.path.join(log_dir, "log4j2-spark.properties")
    with open(props_path, "w") as f:
        f.write(
            f"""\
rootLogger.level = info
rootLogger.appenderRef.file.ref = file
rootLogger.appenderRef.console.ref = console

appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{{yy/MM/dd HH:mm:ss}} %p %c{{1}}: %m%n%ex
appender.console.filter.threshold.type = ThresholdFilter
appender.console.filter.threshold.level = warn

appender.file.type = File
appender.file.name = file
appender.file.fileName = {spark_log_path}
appender.file.layout.type = PatternLayout
appender.file.layout.pattern = %d{{yy/MM/dd HH:mm:ss}} %p %c{{1}}: %m%n%ex
"""
        )
    return props_path


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

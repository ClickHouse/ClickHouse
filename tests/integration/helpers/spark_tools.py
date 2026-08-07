import logging
import os

import pyspark
import pyspark.java_gateway
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def _make_gateway_launch_fork_safe():
    """Give pyspark's gateway launch ``start_new_session`` instead of ``preexec_fn``.

    ``preexec_fn`` rules out both of CPython's exec-only launch paths
    (``posix_spawn`` and ``vfork`` each require it to be ``None``), so a plain
    ``fork()`` runs Python in the child. A lock a sibling thread held at fork
    time is then held in the child forever, so it never execs and the parent
    blocks in ``os.read(errpipe_read)`` until the test times out.

    ``start_new_session`` keeps SIGINT away from the JVM without running Python
    in the child: the signal is delivered to the foreground process group, which
    the child is no longer part of.

    ``launch_gateway`` resolves ``Popen`` through its own module global, so only
    that name is rebound. It is done once at import, because restoring a
    process-wide default around each launch would let concurrent launches strip
    each other's protection and leave the wrapper installed for good.
    """
    if getattr(pyspark.java_gateway.Popen, "_clickhouse_fork_safe", False):
        return

    launcher = pyspark.java_gateway.Popen

    def popen(*args, **kwargs):
        if kwargs.pop("preexec_fn", None) is not None:
            kwargs.setdefault("start_new_session", True)
        return launcher(*args, **kwargs)

    popen._clickhouse_fork_safe = True
    pyspark.java_gateway.Popen = popen


_make_gateway_launch_fork_safe()


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


def _gateway_is_live():
    """Round-trip one trivial call against the class-cached py4j gateway.

    Returns False when nothing is cached (nothing to reuse) or when the cached
    handle no longer answers.
    """
    gateway = SparkContext._gateway
    if gateway is None:
        return False
    try:
        gateway.jvm.System.currentTimeMillis()
        return True
    except Exception:
        return False


def _reset_pyspark_class_state():
    """Drop the cached gateway so ``_ensure_initialized`` relaunches the JVM.

    ``_ensure_initialized`` skips the relaunch while ``SparkContext._gateway`` is
    truthy, and neither ``SparkContext.stop()`` nor ``SparkSession.stop()`` clears
    it. Each clear is tolerant so a pyspark upgrade cannot break the harness.
    """
    for owner, attr in (
        (SparkContext, "_gateway"),
        (SparkContext, "_jvm"),
        (SparkContext, "_active_spark_context"),
        (SparkSession, "_instantiatedSession"),
        (SparkSession, "_activeSession"),
    ):
        try:
            setattr(owner, attr, None)
        except Exception:
            pass


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
        # Set before creating so a factory failure cannot leave the attribute
        # missing, which would make __getattr__ recurse through _is_alive.
        self._session = None
        self._session = self._prepare_and_create()

    def _prepare_and_create(self):
        """Create a session, first discarding a cached gateway that is dead.

        The reset is conditional: a live gateway must be reused, otherwise the
        still-running JVM is orphaned and a needless one is launched.
        """
        if SparkContext._gateway is not None and not _gateway_is_live():
            logging.warning("Cached py4j gateway is dead, discarding it")
            _reset_pyspark_class_state()
        return self._create()

    def _restart(self):
        logging.warning("Spark session is dead, restarting...")
        try:
            self._session.stop()
        except Exception:
            pass
        try:
            pyspark.sql.SparkSession._instantiatedSession = None
        except Exception:
            pass
        self._session = self._prepare_and_create()
        logging.warning("Spark session restarted successfully")

    def _is_alive(self):
        if self.__dict__.get("_session") is None:
            return False
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

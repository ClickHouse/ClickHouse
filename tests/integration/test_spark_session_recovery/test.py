import os
import signal

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from helpers import spark_tools
from helpers.spark_tools import ResilientSparkSession


def _jvm_proc():
    return getattr(SparkContext._gateway, "proc", None)


def _jvm_pid():
    return getattr(_jvm_proc(), "pid", None)


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _kill_jvm_and_reap(proc):
    """Kill the gateway JVM and wait until reaped, not merely signalled: nothing
    in pyspark waits on ``gateway.proc``, so a killed JVM stays a zombie whose
    ``os.kill(pid, 0)`` keeps succeeding until ``wait()`` reaps it."""
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=30)
    assert not _alive(proc.pid)


def _create():
    return (
        pyspark.sql.SparkSession.builder.appName("spark_session_recovery")
        .master("local[1]")
        .getOrCreate()
    )


def test_recovers_dead_gateway_and_reuses_live_one():
    """pyspark caches the py4j gateway in class state and no stop() clears it, so
    a session built after the JVM died must discard it, while one built while it
    still answers must reuse it."""
    try:
        session = ResilientSparkSession(_create)
        first_proc = _jvm_proc()
        first_pid = _jvm_pid()
        assert session.range(3).count() == 3
        assert first_pid is not None

        # A stopped session leaves the gateway cached and the JVM running.
        session.stop()
        assert SparkContext._gateway is not None
        assert _alive(first_pid)

        # No leak: the next session must reuse that live JVM, not launch another.
        reused = ResilientSparkSession(_create)
        assert _jvm_pid() == first_pid
        assert reused.range(2).count() == 2

        # _restart() on a live gateway must also reuse it.
        reused._restart()
        assert _jvm_pid() == first_pid
        assert reused.range(4).count() == 4

        # Recovery: with the JVM dead, the next session must relaunch it. Before
        # the fix __init__ raised "... does not exist in the JVM" -- naming
        # SparkSession$, not CI's SparkConf, because _restart left
        # _instantiatedSession set and getOrCreate asks for that first.
        _kill_jvm_and_reap(first_proc)

        recovered = ResilientSparkSession(_create)
        second_proc = _jvm_proc()
        second_pid = _jvm_pid()
        assert second_pid != first_pid
        assert recovered.range(5).count() == 5

        # Attribute access on a wrapper whose JVM died goes through __getattr__
        # -> _restart, which must recover the same way __init__ does.
        _kill_jvm_and_reap(second_proc)

        assert recovered.range(6).count() == 6
        assert _jvm_pid() not in (first_pid, second_pid)
        recovered.stop()
    finally:
        # A failure leaves a live session or a dead gateway; both poison the next
        # module here. getOrCreate reuses a live session via
        # applyModifiableSettings, which cannot apply a static conf: stop it.
        active = SparkSession._instantiatedSession
        if active is not None:
            try:
                active.stop()  # raises once the gateway is dead; tolerated
            except Exception:
                pass
        # Conditional: dropping a live gateway would orphan its JVM.
        if SparkContext._gateway is not None and not spark_tools._gateway_is_live():
            spark_tools._reset_pyspark_class_state()

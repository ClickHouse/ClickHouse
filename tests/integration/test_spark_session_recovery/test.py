import os
import signal
import time

import pyspark
from pyspark.context import SparkContext

from helpers.spark_tools import ResilientSparkSession


def _jvm_pid():
    gateway = SparkContext._gateway
    proc = getattr(gateway, "proc", None) if gateway is not None else None
    return proc.pid if proc is not None else None


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _create():
    return (
        pyspark.sql.SparkSession.builder.appName("spark_session_recovery")
        .master("local[1]")
        .getOrCreate()
    )


def test_recovers_dead_gateway_and_reuses_live_one():
    """A jointly ordered scenario: pyspark caches the py4j gateway in class state
    and no stop() clears it, so a session built after the JVM died must discard
    that handle, while a session built while it still answers must reuse it."""
    session = ResilientSparkSession(_create)
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

    # Recovery: with the JVM dead, the next session must relaunch it. Before the
    # fix this raised "SparkConf does not exist in the JVM" from __init__.
    os.kill(first_pid, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(first_pid) and time.monotonic() < deadline:
        time.sleep(0.2)

    recovered = ResilientSparkSession(_create)
    second_pid = _jvm_pid()
    assert second_pid != first_pid
    assert recovered.range(5).count() == 5

    # Attribute access on a wrapper whose JVM has since died goes through
    # __getattr__ -> _restart, which must recover the same way __init__ does.
    os.kill(second_pid, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(second_pid) and time.monotonic() < deadline:
        time.sleep(0.2)

    assert recovered.range(6).count() == 6
    assert _jvm_pid() not in (first_pid, second_pid)
    recovered.stop()

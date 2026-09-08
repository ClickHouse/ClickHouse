import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["config/udf.xml"],
)

MiB = 1024 * 1024

# Every script allocates this much; the resident size of its process is the
# ballast plus interpreter overhead, so deltas are asserted as BALLAST +/- TOLERANCE.
BALLAST = 128 * MiB
TOLERANCE = 25 * MiB

SCRIPTS = (
    "udf_async_mem_pool.py",
    "udf_async_child_pool.py",
    "udf_async_mem_kill_pool.py",
    "udf_async_mem_sleep.py",
    "udf_async_hang.py",
    "nonudf_exec_mem_sleep.py",
)


def _skip_msan():
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.exec_in_container(["bash", "-c", "mkdir -p /etc/clickhouse-server/functions"])
        node.exec_in_container(["bash", "-c", "mkdir -p /var/lib/clickhouse/user_scripts"])

        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "functions/test_udf_async_metrics.xml"),
            "/etc/clickhouse-server/functions/test_udf_async_metrics.xml",
        )
        for script in SCRIPTS:
            node.copy_file_to_container(
                os.path.join(SCRIPT_DIR, "user_scripts", script),
                f"/var/lib/clickhouse/user_scripts/{script}",
            )
            node.exec_in_container(["bash", "-c", f"chmod +x /var/lib/clickhouse/user_scripts/{script}"])

        node.restart_clickhouse()

        assert _wait_for(lambda: _memory_resident() is not None)

        yield cluster
    finally:
        cluster.shutdown()


def _async_metric(name):
    """Current value of an async metric, or None if it is not published."""
    raw = node.query(
        f"SELECT value FROM system.asynchronous_metrics WHERE metric = '{name}'"
    ).strip()
    return float(raw) if raw else None


def _memory_resident():
    return _async_metric("ExecutableUserDefinedFunctionMemoryResidentBytes")


def _processes():
    return _async_metric("ExecutableUserDefinedFunctionProcesses")


def _wait_for(predicate, timeout=30, interval=0.5):
    """Poll until predicate() is truthy; async metrics update once per second,
    so assertions on them must tolerate at least one period of lag."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_metrics_published_and_zero_before_first_call(started_cluster):
    _skip_msan()
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    assert _memory_resident() == 0
    assert _processes() == 0


def test_idle_pool_worker_memory_is_visible(started_cluster):
    _skip_msan()
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    node.query("SELECT test_udf_async_mem_pool(1)")

    # The idle pool worker keeps holding its ~128 MiB.
    assert _wait_for(
        lambda: BALLAST - TOLERANCE <= _memory_resident() - mem_before <= BALLAST + TOLERANCE
    ), f"memory increase {_memory_resident() - mem_before} outside one-worker band"
    assert _processes() - procs_before == 1


def test_two_udfs_memory_is_summed(started_cluster):
    _skip_msan()
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    node.query("SELECT test_udf_async_mem_pool(1)")
    node.query("SELECT test_udf_async_mem_kill_pool(1)")

    # Two idle workers of distinct functions, ~128 MiB each: the gauge must
    # report their sum, not either one alone.
    assert _wait_for(
        lambda: 2 * (BALLAST - TOLERANCE) <= _memory_resident() - mem_before <= 2 * (BALLAST + TOLERANCE)
    ), f"memory increase {_memory_resident() - mem_before} outside two-worker band"
    assert _processes() - procs_before == 2


def test_descendant_processes_are_counted(started_cluster):
    _skip_msan()
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    node.query("SELECT test_udf_async_child_pool(1)")

    # The ~128 MiB lives in the worker's grandchild, so the increase proves
    # the sweep walks the process subtree recursively, not just direct children.
    # The band is one BALLAST wide but three processes deep (worker, child,
    # grandchild interpreters all contribute overhead).
    assert _wait_for(lambda: _processes() - procs_before == 3), (
        f"process increase {_processes() - procs_before} is not 3"
    )
    assert _wait_for(
        lambda: BALLAST - TOLERANCE <= _memory_resident() - mem_before <= BALLAST + 3 * TOLERANCE
    ), f"memory increase {_memory_resident() - mem_before} outside subtree band"


def test_inflight_executable_invocation_is_visible(started_cluster):
    _skip_msan()
    # Restart for a clean 0/0 baseline: earlier tests leave idle pool workers
    # whose resident memory would otherwise contaminate this non-pool delta.
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    errors = []

    def run_query():
        try:
            node.query("SELECT test_udf_async_mem_sleep(1)")
        except Exception as e:
            errors.append(e)

    thread = threading.Thread(target=run_query)
    thread.start()
    try:
        # The script allocates ~128 MiB at startup, then sleeps ~30 s per row.
        assert _wait_for(
            lambda: BALLAST - TOLERANCE <= _memory_resident() - mem_before <= BALLAST + TOLERANCE,
            timeout=25,
            interval=0.5,
        ), f"in-flight memory increase {_memory_resident() - mem_before} outside one-worker band"
        assert _processes() - procs_before == 1
    finally:
        thread.join(timeout=120)

    assert not errors, f"query failed: {errors}"
    assert not thread.is_alive(), "query did not finish"

    # The non-pool child is reaped at query end and must leave both metrics.
    assert _wait_for(lambda: _memory_resident() - mem_before < 30 * MiB), (
        f"memory did not return to baseline, increase {_memory_resident() - mem_before}"
    )
    assert _wait_for(lambda: _processes() - procs_before == 0)


def test_timed_out_executable_is_killed_and_leaves_metrics(started_cluster):
    _skip_msan()
    # Covers cleanup after a FAILED invocation: the script hangs after
    # allocating, command_read_timeout (10 s) fails the query, and the error
    # path must tear the stuck process down and release its registry entry.
    # Restart for a clean 0/0 baseline: earlier tests leave idle pool workers
    # whose resident memory would otherwise contaminate this non-pool delta.
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    errors = []

    def run_query():
        try:
            node.query("SELECT test_udf_async_hang(1)")
        except Exception as e:
            errors.append(e)

    thread = threading.Thread(target=run_query)
    thread.start()
    try:
        # Both deltas in one predicate: a separate count snapshot could land
        # after the timeout kill when the memory check passes late.
        assert _wait_for(
            lambda: BALLAST - TOLERANCE <= _memory_resident() - mem_before <= BALLAST + TOLERANCE
            and _processes() - procs_before == 1,
            timeout=25,
            interval=0.3,
        ), f"hanging UDF not observed, memory increase {_memory_resident() - mem_before}"
    finally:
        thread.join(timeout=60)

    assert not thread.is_alive(), "query did not finish"
    assert errors, "query unexpectedly succeeded, expected a read timeout"
    assert "TIMEOUT_EXCEEDED" in str(errors[0]), f"unexpected error: {errors[0]}"

    assert _wait_for(lambda: _memory_resident() - mem_before < 30 * MiB), (
        f"memory did not return to baseline, increase {_memory_resident() - mem_before}"
    )
    assert _wait_for(lambda: _processes() - procs_before == 0)


def test_killed_pool_worker_leaves_metrics(started_cluster):
    _skip_msan()
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    node.query("SELECT test_udf_async_mem_kill_pool(1)")
    assert _wait_for(
        lambda: BALLAST - TOLERANCE <= _memory_resident() - mem_before <= BALLAST + TOLERANCE
    )

    # Kill the idle worker behind ClickHouse's back, so it must leave both metrics
    # on the next sweep.
    node.exec_in_container(["pkill", "-9", "-f", "udf_async_mem_kill_pool.py"])
    assert _wait_for(lambda: _memory_resident() - mem_before < 30 * MiB), (
        f"killed worker still counted, increase {_memory_resident() - mem_before}"
    )
    assert _wait_for(lambda: _processes() - procs_before == 0)

    # The next borrow hands out the dead worker: the query fails on a broken
    # pipe and the wrapper is destroyed. 
    try:
        node.query("SELECT test_udf_async_mem_kill_pool(2)")
    except Exception:
        pass

    # The pool replaces the dead worker, and the fresh one is registered again.
    assert node.query("SELECT test_udf_async_mem_kill_pool(2)").strip() == "2"
    assert _wait_for(
        lambda: BALLAST - TOLERANCE <= _memory_resident() - mem_before <= BALLAST + TOLERANCE
    ), f"respawned worker not counted, increase {_memory_resident() - mem_before}"


def test_non_udf_shell_commands_are_not_counted(started_cluster):
    _skip_msan()
    # Restart for a clean 0/0 baseline: earlier tests leave idle pool workers
    # whose resident memory would otherwise contaminate this delta.
    node.restart_clickhouse()
    assert _wait_for(lambda: _memory_resident() is not None)
    mem_before = _memory_resident()
    procs_before = _processes()

    node.exec_in_container(["bash", "-c", "rm -f /tmp/nonudf_running"])
    node.query("DROP TABLE IF EXISTS non_udf_executable")
    node.query(
        "CREATE TABLE non_udf_executable (value UInt64) "
        "ENGINE = Executable('nonudf_exec_mem_sleep.py', 'TabSeparated') "
        "SETTINGS command_read_timeout = 60000"
    )

    errors = []

    def run_query():
        try:
            node.query("SELECT * FROM non_udf_executable")
        except Exception as e:
            errors.append(e)

    thread = threading.Thread(target=run_query)
    thread.start()
    try:
        # Let a few sweeps pass and verify the metrics ignored the process.
        assert _wait_for(
            lambda: node.exec_in_container(
                ["bash", "-c", "test -f /tmp/nonudf_running && echo yes || echo no"]
            ).strip()
            == "yes",
            timeout=25,
        ), "Executable table engine process did not start"
        time.sleep(4)
        assert _memory_resident() - mem_before < 30 * MiB, (
            f"non-UDF process leaked into the memory metric, increase {_memory_resident() - mem_before}"
        )
        assert _processes() - procs_before == 0, (
            f"non-UDF process leaked into the process count, increase {_processes() - procs_before}"
        )
    finally:
        thread.join(timeout=120)
        node.query("DROP TABLE IF EXISTS non_udf_executable")

    assert not errors, f"query failed: {errors}"

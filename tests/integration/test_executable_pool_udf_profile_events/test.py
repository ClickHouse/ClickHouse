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


def _skip_msan():
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


def _copy_into_container(local_path, container_path):
    os.system(f"docker cp {local_path} {node.docker_id}:{container_path}")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.exec_in_container(["bash", "-c", "mkdir -p /etc/clickhouse-server/functions"])
        node.exec_in_container(["bash", "-c", "mkdir -p /var/lib/clickhouse/user_scripts"])

        _copy_into_container(
            os.path.join(SCRIPT_DIR, "functions/test_pool_udf_echo.xml"),
            "/etc/clickhouse-server/functions/test_pool_udf_echo.xml",
        )
        for script in (
            "pool_udf_echo.py",
            "pool_udf_sleep.py",
            "pool_udf_cpu.py",
            "pool_udf_mem.py",
            "pool_udf_syscall.py",
            "pool_udf_persistent_helper.py",
            "pool_udf_helper_reaped.py",
            "pool_udf_per_row_child.py",
            "pool_udf_multi_layer.py",
            "pool_udf_lazy_pool.py",
        ):
            _copy_into_container(
                os.path.join(SCRIPT_DIR, "user_scripts", script),
                f"/var/lib/clickhouse/user_scripts/{script}",
            )
            node.exec_in_container(["bash", "-c", f"chmod +x /var/lib/clickhouse/user_scripts/{script}"])

        node.restart_clickhouse()

        yield cluster
    finally:
        cluster.shutdown()


def _profile_event_value(query_id, event_name):
    """Read a single ProfileEvent for a query from system.query_log.

    Filters to the QueryFinish row so we get the populated counters.
    """
    node.query("SYSTEM FLUSH LOGS")
    raw = node.query(
        "SELECT ProfileEvents[{name}] FROM system.query_log "
        "WHERE query_id = '{qid}' AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds DESC LIMIT 1".format(
            name=repr(event_name), qid=query_id
        )
    ).strip()
    return int(raw) if raw else 0


def _run(query, query_id):
    return node.query(query, query_id=query_id)


def test_invocations(started_cluster):
    _skip_msan()
    qid = "invocations-1"
    rows = 5000
    # Use `sum` (not `count`) so the optimizer can't prune the UDF column;
    # otherwise `SELECT count() FROM (SELECT udf(number) FROM numbers(N))`
    # is rewritten to `SELECT count() FROM numbers(N)` and the UDF never runs.
    _run(
        f"SELECT sum(test_pool_udf_echo(number)) FROM numbers({rows})",
        qid,
    )
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    # 5000 rows fits comfortably inside the default block size, so we expect
    # one Invocation. Allow >= 1 to absorb planner-driven block splits.
    assert invocations >= 1, f"Expected at least one Invocation, got {invocations}"


def test_elapsed_microseconds(started_cluster):
    _skip_msan()
    qid = "elapsed-1"
    # Sleep argument is interpreted by pool_udf_sleep.py as seconds per row.
    _run(
        "SELECT test_pool_udf_sleep(0.2) SETTINGS max_block_size = 1",
        qid,
    )
    elapsed = _profile_event_value(qid, "ExecutableUserDefinedFunctionElapsedMicroseconds")
    # 200 ms sleep with 10% slack gives 180_000 us.
    assert elapsed >= 180_000, f"Elapsed={elapsed} below 180_000 us"


def test_pool_wait_microseconds(started_cluster):
    _skip_msan()
    # pool_size=1 + 2 concurrent queries → the second must wait on the first.
    qids = ["poolwait-a", "poolwait-b"]
    threads = []
    errors = []

    def runner(qid):
        try:
            _run("SELECT test_pool_udf_sleep(0.5) SETTINGS max_block_size = 1", qid)
        except Exception as exc:
            errors.append(exc)

    for qid in qids:
        t = threading.Thread(target=runner, args=(qid,))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent runners failed: {errors}"

    waits = [
        _profile_event_value(qid, "ExecutableUserDefinedFunctionPoolWaitMicroseconds")
        for qid in qids
    ]
    assert max(waits) > 0, f"Expected PoolWait > 0 on at least one query, got {waits}"


def test_cpu_user_microseconds(started_cluster):
    _skip_msan()
    qid = "cpu-1"
    # `sum` keeps the UDF column live so the optimizer can't drop the call.
    _run(
        "SELECT sum(test_pool_udf_cpu(number)) FROM numbers(2000)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTimeMicroseconds > 0, got {cpu}"


def test_system_time_microseconds(started_cluster):
    _skip_msan()
    qid = "syscall-1"
    _run(
        "SELECT sum(test_pool_udf_syscall(number)) FROM numbers(64)",
        qid,
    )
    sys_time = _profile_event_value(qid, "ExecutableUserDefinedFunctionSystemTimeMicroseconds")
    assert sys_time > 0, f"Expected SystemTimeMicroseconds > 0, got {sys_time}"


def test_memory_usage_byte_seconds(started_cluster):
    _skip_msan()
    qid = "mem-1"
    _run(
        "SELECT sum(test_pool_udf_mem(number)) FROM numbers(64)",
        qid,
    )
    mem = _profile_event_value(qid, "ExecutableUserDefinedFunctionPeakMemoryByteSeconds")
    # 32 MiB peak RSS, touched page by page over 64 rows: borrow wall takes
    # well over 30 ms in CPython. 32 MiB * 0.03 s ~= 10^6 byte-seconds floor.
    assert mem >= 1_000_000, f"Expected PeakMemoryByteSeconds >= 1e6, got {mem}"


def test_input_bytes(started_cluster):
    _skip_msan()
    qid = "in-1"
    rows = 1000
    _run(
        f"SELECT sum(test_pool_udf_echo(number)) FROM numbers({rows})",
        qid,
    )
    input_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionInputBytes")
    # Each row is one ascii decimal + newline: numbers 0..999 average ≈ 4 bytes
    # per line plus the newline → roughly 5000 bytes total. Allow generous slack.
    assert 2000 <= input_bytes <= 20000, f"InputBytes out of expected range: {input_bytes}"


def test_output_bytes(started_cluster):
    _skip_msan()
    qid = "out-1"
    rows = 1000
    _run(
        f"SELECT sum(test_pool_udf_echo(number)) FROM numbers({rows})",
        qid,
    )
    output_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionOutputBytes")
    assert 2000 <= output_bytes <= 20000, f"OutputBytes out of expected range: {output_bytes}"


# -----------------------------------------------------------------------------
# Reaping-scenario coverage: each test exercises one way a UDF can move CPU
# between processes. The sampler should attribute every variant correctly via
# the subtree walk + per-pid `c{u,s}time` plus the 3-bucket post-walk dispatch.
# -----------------------------------------------------------------------------


def test_persistent_helper_alive(started_cluster):
    """(a) Helper alive across the borrow → captured via per-pid delta."""
    _skip_msan()
    qid = "persistent-helper-1"
    _run(
        "SELECT sum(test_pool_udf_persistent_helper(number)) FROM numbers(64)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTime > 0 (persistent helper alive), got {cpu}"
    # Sanity upper bound — 64 rows of light CPU should not exceed 60 s.
    assert cpu < 60 * 1_000_000, f"UserTime suspiciously high: {cpu}"


def test_persistent_helper_reaped(started_cluster):
    """(b) Helper reaped mid-borrow → captured via parent's `cutime` delta."""
    _skip_msan()
    qid = "helper-reaped-1"
    _run(
        "SELECT sum(test_pool_udf_helper_reaped(number)) FROM numbers(10)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTime > 0 (reaped helper via cutime), got {cpu}"
    assert cpu < 60 * 1_000_000, f"UserTime suspiciously high: {cpu}"


def test_per_row_child_reaped(started_cluster):
    """(c) Short-lived per-row child, reaped → captured via `cutime` delta."""
    _skip_msan()
    qid = "per-row-child-1"
    _run(
        "SELECT sum(test_pool_udf_per_row_child(number)) FROM numbers(20)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTime > 0 (per-row reaped children via cutime), got {cpu}"
    assert cpu < 120 * 1_000_000, f"UserTime suspiciously high: {cpu}"


def test_multi_layer_reaping_chain(started_cluster):
    """(d) UDF → python helper → `sort` grandchild → reaped: cutime propagates."""
    _skip_msan()
    qid = "multi-layer-1"
    _run(
        "SELECT sum(test_pool_udf_multi_layer(number)) FROM numbers(20)",
        qid,
    )
    user_cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    sys_cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionSystemTimeMicroseconds")
    # `sort` work shows up in the helper's `c{u,s}time` delta.
    assert user_cpu + sys_cpu > 0, (
        f"Expected UserTime + SystemTime > 0 (multi-layer reaping), got "
        f"{user_cpu} + {sys_cpu}"
    )


def test_lazy_pool_spawned_in_borrow(started_cluster):
    """(e) `multiprocessing.Pool` created mid-borrow → bucket-3 dispatch."""
    _skip_msan()
    qid = "lazy-pool-1"
    _run(
        "SELECT sum(test_pool_udf_lazy_pool(number)) FROM numbers(20)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    # Pool workers spawned after `recordPidAcquired` → not in `pre_walk_pids`,
    # so the three-bucket dispatch counts their full post value.
    assert cpu > 0, f"Expected UserTime > 0 (lazy Pool workers via bucket 3), got {cpu}"
    assert cpu < 60 * 1_000_000, f"UserTime suspiciously high: {cpu}"

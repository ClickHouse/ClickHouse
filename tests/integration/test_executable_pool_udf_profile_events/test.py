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
            "test_pool_udf_echo.py",
            "test_pool_udf_sleep.py",
            "test_pool_udf_cpu.py",
            "test_pool_udf_mem.py",
            "test_pool_udf_syscall.py",
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
    _run(
        f"SELECT count() FROM (SELECT test_pool_udf_echo(number) FROM numbers({rows}))",
        qid,
    )
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    # 5000 rows fits comfortably inside the default block size, so we expect
    # one Invocation. Allow >= 1 to absorb planner-driven block splits.
    assert invocations >= 1, f"Expected at least one Invocation, got {invocations}"


def test_elapsed_microseconds(started_cluster):
    _skip_msan()
    qid = "elapsed-1"
    # Sleep argument is interpreted by test_pool_udf_sleep.py as seconds per row.
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
    _run(
        "SELECT count() FROM (SELECT test_pool_udf_cpu(number) FROM numbers(2000))",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTimeMicroseconds > 0, got {cpu}"


def test_system_time_microseconds(started_cluster):
    _skip_msan()
    qid = "syscall-1"
    _run(
        "SELECT count() FROM (SELECT test_pool_udf_syscall(number) FROM numbers(64))",
        qid,
    )
    sys_time = _profile_event_value(qid, "ExecutableUserDefinedFunctionSystemTimeMicroseconds")
    assert sys_time > 0, f"Expected SystemTimeMicroseconds > 0, got {sys_time}"


def test_memory_usage_byte_seconds(started_cluster):
    _skip_msan()
    qid = "mem-1"
    _run(
        "SELECT count() FROM (SELECT test_pool_udf_mem(number) FROM numbers(64))",
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
        f"SELECT count() FROM (SELECT test_pool_udf_echo(number) FROM numbers({rows}))",
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
        f"SELECT count() FROM (SELECT test_pool_udf_echo(number) FROM numbers({rows}))",
        qid,
    )
    output_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionOutputBytes")
    assert 2000 <= output_bytes <= 20000, f"OutputBytes out of expected range: {output_bytes}"

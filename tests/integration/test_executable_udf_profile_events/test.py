import os
import sys

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
            os.path.join(SCRIPT_DIR, "functions/test_udf_echo.xml"),
            "/etc/clickhouse-server/functions/test_udf_echo.xml",
        )
        for script in (
            "udf_echo.py",
            "udf_sleep.py",
            "udf_cpu.py",
            "udf_mem.py",
            "udf_syscall.py",
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
    qid = "exec-invocations-1"
    rows = 5000
    # `sum` (not `count`) keeps the UDF column live so the optimizer can't prune it.
    # max_block_size=1000 over 5000 rows yields exactly 5 blocks → 5 executeImpl calls,
    # so Invocations is deterministically 5 — this catches both double-counting and
    # missing-increment regressions, unlike a loose `>= 1`.
    _run(
        f"SELECT sum(test_udf_echo(number)) FROM numbers({rows}) SETTINGS max_block_size = 1000",
        qid,
    )
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    assert invocations == 5, f"Expected exactly 5 Invocations, got {invocations}"


def test_elapsed_microseconds(started_cluster):
    _skip_msan()
    qid = "exec-elapsed-1"
    # Single-row scalar query: the UDF receives the literal 0.2 and sleeps
    # for that many seconds. `max_block_size` is irrelevant for a scalar
    # expression with no `FROM` clause.
    _run(
        "SELECT test_udf_sleep(0.2)",
        qid,
    )
    elapsed = _profile_event_value(qid, "ExecutableUserDefinedFunctionElapsedMicroseconds")
    # 200 ms sleep with 10% slack gives 180_000 us.
    assert elapsed >= 180_000, f"Elapsed={elapsed} below 180_000 us"
    # Executable UDFs are non-deterministic by default, so the scalar call is not
    # constant-folded; if that ever regresses, fail loudly here rather than via the floor.
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    assert invocations >= 1, f"UDF did not run (Invocations={invocations}); elapsed floor is meaningless"


def test_cpu_user_microseconds(started_cluster):
    _skip_msan()
    qid = "exec-cpu-1"
    # `sum` keeps the UDF column live so the optimizer can't drop the call.
    _run(
        "SELECT sum(test_udf_cpu(number)) FROM numbers(2000)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTimeMicroseconds > 0, got {cpu}"


def test_system_time_microseconds(started_cluster):
    _skip_msan()
    qid = "exec-syscall-1"
    _run(
        "SELECT sum(test_udf_syscall(number)) FROM numbers(64)",
        qid,
    )
    sys_time = _profile_event_value(qid, "ExecutableUserDefinedFunctionSystemTimeMicroseconds")
    assert sys_time > 0, f"Expected SystemTimeMicroseconds > 0, got {sys_time}"


def test_peak_memory_uses_rusage_units(started_cluster):
    _skip_msan()
    qid = "exec-mem-rusage-1"
    # Use a small row count; `wait4` populates `ru_maxrss` once per child exit,
    # so even a single-row block will show the 32 MiB allocation from `udf_mem.py`.
    _run(
        "SELECT sum(test_udf_mem(number)) FROM numbers(4)",
        qid,
    )
    mem = _profile_event_value(qid, "ExecutableUserDefinedFunctionPeakMemoryByteSeconds")
    # 32 MiB peak RSS. Even a sub-millisecond run: 32 * 1024^2 * 0.001 s ≈ 33_554.
    # 100_000 is 3x below that floor — a value this small could only arise if
    # wait4 ru_maxrss were misread (e.g. treated as KiB on a Linux platform
    # where it is already in bytes, which would deflate the result by 1024).
    # The bound is intentionally loose to avoid flaking on fast machines.
    assert mem >= 100_000, f"Expected PeakMemoryByteSeconds >= 100_000, got {mem}"


def test_input_bytes(started_cluster):
    _skip_msan()
    qid = "exec-in-1"
    rows = 1000
    _run(
        f"SELECT sum(test_udf_echo(number)) FROM numbers({rows})",
        qid,
    )
    input_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionInputBytes")
    # Each row is one ascii decimal + newline: numbers 0..999 average ~4 bytes
    # per line plus the newline → roughly 5000 bytes total. Allow generous slack.
    assert 2000 <= input_bytes <= 20000, f"InputBytes out of expected range: {input_bytes}"


def test_output_bytes(started_cluster):
    _skip_msan()
    qid = "exec-out-1"
    rows = 1000
    _run(
        f"SELECT sum(test_udf_echo(number)) FROM numbers({rows})",
        qid,
    )
    output_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionOutputBytes")
    assert 2000 <= output_bytes <= 20000, f"OutputBytes out of expected range: {output_bytes}"


def test_pool_wait_is_zero_on_executable_path(started_cluster):
    _skip_msan()
    qid = "exec-poolwait-zero-1"
    _run("SELECT sum(test_udf_echo(number)) FROM numbers(1000)", qid)
    # Executable (non-pool) UDFs never borrow from a pool: recordPoolWaitDone is
    # only reached on the pool path (ShellCommandSource.cpp), so this event must be 0.
    pool_wait = _profile_event_value(qid, "ExecutableUserDefinedFunctionPoolWaitMicroseconds")
    assert pool_wait == 0, f"Expected PoolWait==0 on executable path, got {pool_wait}"
    # Guard against a false pass from a missing QueryFinish row (the helper returns 0
    # for both an absent row and a genuine zero):
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    assert invocations >= 1, f"UDF did not run (Invocations={invocations}); PoolWait==0 is meaningless"



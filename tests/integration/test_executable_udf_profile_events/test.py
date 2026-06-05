import os
import sys
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
            os.path.join(SCRIPT_DIR, "functions/test_udf_echo.xml"),
            "/etc/clickhouse-server/functions/test_udf_echo.xml",
        )
        for script in (
            "udf_echo.py",
            "udf_sleep.py",
            "udf_cpu.py",
            "udf_mem.py",
            "udf_syscall.py",
            "udf_child_wait.py",
            "udf_child_detach.py",
            "udf_child_mem.py",
            "udf_two_mem_concurrent.py",
            "udf_two_mem_sequential.py",
            "udf_nonzero_exit.py",
            "udf_stdout_close_linger.py",
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


# ---------------------------------------------------------------------------
# T1: Shell launch path
# ---------------------------------------------------------------------------


def test_shell_launch_collects_usage(started_cluster):
    _skip_msan()
    qid = "exec-shell-launch-1"
    # execute_direct=false routes the command through `/bin/sh -c`; for a
    # simple single-token command bash exec-optimises the fork, so wait4 still
    # reaps the actual script process and its rusage rolls up normally.
    _run(
        "SELECT sum(test_udf_cpu_shell(number)) FROM numbers(2000)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTimeMicroseconds > 0 on shell path, got {cpu}"
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    assert invocations >= 1, f"UDF did not run (Invocations={invocations}); UserTime floor is meaningless"


# ---------------------------------------------------------------------------
# T2 + T3: Reaped child CPU rolls up; unwaited child CPU is excluded
# ---------------------------------------------------------------------------


def test_reaped_child_cpu_rolls_up(started_cluster):
    _skip_msan()
    qid = "exec-child-wait-1"
    # The child burns ~24 M integer additions; the parent is nearly idle.
    # UserTimeMicroseconds must reflect the child's cutime (via waitpid + wait4).
    # 200_000 us = 0.2 s, well below the actual burn (~1 s on a normal CI box,
    # ~0.4 s on a fast one); a missing cutime rollup would give ~11_000 us
    # (parent overhead only), so 200_000 is a safe discriminating floor.
    _run(
        "SELECT sum(test_udf_child_wait(number)) FROM numbers(1)",
        qid,
    )
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu >= 200_000, f"Expected UserTimeMicroseconds >= 200_000 (child CPU rolled up), got {cpu}"


def test_unwaited_child_cpu_excluded(started_cluster):
    _skip_msan()
    qid_detach = "exec-child-detach-1"
    qid_wait = "exec-child-wait-2"
    # Both scripts fork a child that does the same CPU burn.
    # child_detach: parent never calls waitpid → cutime stays 0.
    # child_wait: parent calls waitpid → cutime rolls up.
    # Ratio assertion is robust to machine speed: a missing-cutime bug on the
    # wait path would flip the comparison; a spurious-cutime bug on the detach
    # path would narrow it below 2x.
    _run(
        "SELECT sum(test_udf_child_detach(number)) FROM numbers(1)",
        qid_detach,
    )
    _run(
        "SELECT sum(test_udf_child_wait(number)) FROM numbers(1)",
        qid_wait,
    )
    detach_cpu = _profile_event_value(qid_detach, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    wait_cpu = _profile_event_value(qid_wait, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert wait_cpu > 0, f"child_wait CPU is 0 — child cutime not captured (wait_cpu={wait_cpu})"
    assert detach_cpu < wait_cpu / 2, (
        f"Unwaited child CPU should be < half of waited: detach={detach_cpu} wait={wait_cpu}"
    )


# ---------------------------------------------------------------------------
# T4: Reaped child memory rolls up
# ---------------------------------------------------------------------------


def test_reaped_child_memory_rolls_up(started_cluster):
    _skip_msan()
    qid = "exec-child-mem-1"
    # The child allocates ~64 MiB and exits; the parent reaps it with waitpid
    # so its ru_maxrss appears in wait4 rusage for the top-level script.
    # 100_000 byte·s floor: 64 MiB * 1 ms ≈ 67_108, so 100_000 is ~1.5x above
    # the absolute minimum at sub-millisecond elapsed; a missing ru_maxrss
    # rollup would give a value at least 64x smaller.
    _run(
        "SELECT sum(test_udf_child_mem(number)) FROM numbers(1)",
        qid,
    )
    mem = _profile_event_value(qid, "ExecutableUserDefinedFunctionPeakMemoryByteSeconds")
    assert mem >= 100_000, f"Expected PeakMemoryByteSeconds >= 100_000 (child RSS rolled up), got {mem}"


# ---------------------------------------------------------------------------
# T5: Peak memory is max-of-peaks, not a concurrent aggregate
# ---------------------------------------------------------------------------


def test_peak_memory_is_max_not_sum(started_cluster):
    _skip_msan()
    qid_concurrent = "exec-two-mem-concurrent-1"
    qid_sequential = "exec-two-mem-sequential-1"
    # concurrent: two 100 MiB children coexist (~200 MiB live simultaneously).
    # sequential: two 100 MiB children never coexist (~100 MiB peak each).
    # ru_maxrss is max-of-per-process-peaks, so both report ~100 MiB.
    # implied_peak = PeakMemoryByteSeconds * 1_000_000 / ElapsedMicroseconds.
    # A true concurrent-sum implementation would make concurrent ~2x sequential.
    # We assert concurrent <= 1.6 * sequential: measured ~1.0, bug threshold ~2.0.
    _run(
        "SELECT sum(test_udf_two_mem_concurrent(number)) FROM numbers(1)",
        qid_concurrent,
    )
    _run(
        "SELECT sum(test_udf_two_mem_sequential(number)) FROM numbers(1)",
        qid_sequential,
    )

    def _implied_peak(qid):
        mem_bs = _profile_event_value(qid, "ExecutableUserDefinedFunctionPeakMemoryByteSeconds")
        elapsed_us = _profile_event_value(qid, "ExecutableUserDefinedFunctionElapsedMicroseconds")
        if elapsed_us == 0:
            return 0
        return mem_bs * 1_000_000 / elapsed_us

    concurrent_peak = _implied_peak(qid_concurrent)
    sequential_peak = _implied_peak(qid_sequential)
    assert sequential_peak > 0, f"sequential peak is 0 — memory not captured (sequential={sequential_peak})"
    assert concurrent_peak <= 1.6 * sequential_peak, (
        f"concurrent peak {concurrent_peak:.0f} is more than 1.6x sequential {sequential_peak:.0f}; "
        f"suggests ru_maxrss is summing concurrent allocations instead of taking the max"
    )


# ---------------------------------------------------------------------------
# T6: check_exit_code=false
# ---------------------------------------------------------------------------


def test_check_exit_code_false_succeeds_and_meters(started_cluster):
    _skip_msan()
    qid = "exec-nonzero-exit-1"
    # The UDF exits with code 3; check_exit_code=false means the source takes
    # the tryReapWithoutStatusCheck path, so the query must succeed and rusage
    # must still be populated.
    result = _run(
        f"SELECT sum(test_udf_nonzero_exit(number)) FROM numbers(20)",
        qid,
    )
    # If check_exit_code=false is not honoured the query raises; reaching here
    # means the query completed.
    assert result is not None
    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    assert invocations >= 1, f"UDF did not run (Invocations={invocations})"
    cpu = _profile_event_value(qid, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    assert cpu > 0, f"Expected UserTimeMicroseconds > 0 after non-zero exit, got {cpu}"


def test_check_exit_code_false_no_spurious_log(started_cluster):
    _skip_msan()
    # Run the UDF once to produce a non-zero exit, then scan the server log.
    # The CHILD_WAS_NOT_EXITED_NORMALLY message must not appear: skipping status
    # validation means we never reach the code path that emits it.
    qid = "exec-nonzero-exit-log-1"
    _run(
        "SELECT sum(test_udf_nonzero_exit(number)) FROM numbers(10)",
        qid,
    )
    count_raw = node.exec_in_container(
        [
            "bash",
            "-c",
            "grep -c CHILD_WAS_NOT_EXITED_NORMALLY /var/log/clickhouse-server/clickhouse-server.log || true",
        ]
    ).strip()
    count = int(count_raw) if count_raw.isdigit() else 0
    assert count == 0, (
        f"Found {count} occurrence(s) of CHILD_WAS_NOT_EXITED_NORMALLY in the server log; "
        f"check_exit_code=false must suppress that message"
    )


def test_check_exit_code_false_lingering_child_is_bounded_and_flushes_bytes(started_cluster):
    _skip_msan()
    # Invariant: with check_exit_code=false a child that closes stdout and lingers
    # does not block cleanup (teardown is bounded by command_termination_timeout,
    # ~3 s, not by the child's 30 s sleep), and its already-observed stdin/stdout
    # byte counters are still reported even though no wait4 rusage was captured.
    qid = "exec-stdout-linger-1"
    t0 = time.monotonic()
    _run("SELECT sum(test_udf_stdout_close_linger(number)) FROM numbers(50)", qid)
    elapsed = time.monotonic() - t0
    assert elapsed < 25, (
        f"Query took {elapsed:.1f}s — must be bounded by command_termination_timeout (~3s) "
        f"+ teardown, not the child's 30s sleep"
    )

    invocations = _profile_event_value(qid, "ExecutableUserDefinedFunctionInvocations")
    assert invocations >= 1, f"UDF did not run (Invocations={invocations}); byte-counter assertions are meaningless"

    input_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionInputBytes")
    assert input_bytes > 0, (
        f"InputBytes={input_bytes}; byte counters must flush even when the child was never reaped"
    )

    output_bytes = _profile_event_value(qid, "ExecutableUserDefinedFunctionOutputBytes")
    assert output_bytes > 0, (
        f"OutputBytes={output_bytes}; byte counters must flush even when the child was never reaped"
    )

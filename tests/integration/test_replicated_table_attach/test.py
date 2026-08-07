import os
import re
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,  # Disable with_remote_database_disk as test_startup_with_small_bg_pool_partitioned drops Keeper connection
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def start_clean_clickhouse():
    # remove fault injection if present
    if "fault_injection.xml" in node.exec_in_container(
        ["bash", "-c", "ls /etc/clickhouse-server/config.d"]
    ):
        print("Removing fault injection")
        node.exec_in_container(
            ["bash", "-c", "rm /etc/clickhouse-server/config.d/fault_injection.xml"]
        )
        node.restart_clickhouse()


def test_startup_with_small_bg_pool(started_cluster):
    start_clean_clickhouse()
    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    node.query(
        "CREATE TABLE replicated_table (k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/replicated_table', 'r1') ORDER BY k"
    )

    node.query("INSERT INTO replicated_table VALUES(20, 30)")

    def assert_values():
        assert node.query("SELECT * FROM replicated_table") == "20\t30\n"

    assert_values()
    node.restart_clickhouse(stop_start_wait_sec=15)
    assert_values()


def test_startup_with_small_bg_pool_partitioned(started_cluster):
    start_clean_clickhouse()
    node.query("DROP TABLE IF EXISTS replicated_table_partitioned SYNC")
    node.query(
        "CREATE TABLE replicated_table_partitioned (k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/replicated_table_partitioned', 'r1') ORDER BY k"
    )

    node.query("INSERT INTO replicated_table_partitioned VALUES(20, 30)")

    def assert_values():
        assert node.query("SELECT * FROM replicated_table_partitioned") == "20\t30\n"

    assert_values()
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.stop_clickhouse(stop_wait_sec=150)
        node.copy_file_to_container(
            os.path.join(CONFIG_DIR, "fault_injection.xml"),
            "/etc/clickhouse-server/config.d/fault_injection.xml",
        )
        node.start_clickhouse(start_wait_sec=150)
        assert_values()

    # Check that the table re-activates in the end.
    #
    # Fault injection stays enabled for the rest of this test, and every
    # injected fault throws `ZSESSIONEXPIRED`, tearing down the whole ClickHouse
    # Keeper session (not just one request). Combined with
    # `background_schedule_pool_size=1` - so the attach/restarting task shares a
    # single thread - and sanitizer/coverage slowdown, re-activation is a
    # heavy-tailed random process: usually a few seconds, occasionally much
    # longer. The table becomes writable only once it threads a complete clean
    # activation sequence between two session-expiry faults.
    #
    # Previously we retried the blocking INSERT itself, but an INSERT to a
    # not-yet-active replica blocks server-side, so each attempt burned its
    # whole `timeout` even when the table was nowhere near ready, and the retry
    # budget was occasionally exhausted before activation (issue #101103). Poll
    # the cheap, local `is_readonly` flag instead - it needs no Keeper
    # round-trip and returns instantly - so the time budget is spent waiting for
    # activation rather than blocking inside doomed writes.
    is_readonly = node.query_with_retry(
        "SELECT is_readonly FROM system.replicas WHERE table = 'replicated_table_partitioned'",
        check_callback=lambda x: x.strip() == "0",
        retry_count=120,
        sleep_time=3,
    )
    assert (
        is_readonly.strip() == "0"
    ), f"table did not re-activate after fault injection: is_readonly={is_readonly!r}"

    # Once active, the INSERT should go through. Keep a short retry because a
    # fault may still transiently expire the session between the check and the
    # write.
    node.query_with_retry(
        "INSERT INTO replicated_table_partitioned VALUES(20, 30)",
        retry_count=20,
        sleep_time=3,
        timeout=15,
    )


# The restarting task is deactivated by the cleanup of a failed attach-path startup
# (FP1). The next attempt reaches the attach branch with the task still deactivated, so
# the inline run() cannot re-arm itself; FP2 keeps that run failing so the replica stays
# readonly. Without the fix nothing re-arms the task and it is owned by no pool
# collection, so it never retries.
def test_restarting_task_rearmed_after_failed_attach_startup(started_cluster):
    start_clean_clickhouse()
    # The restart above re-attaches every other ReplicatedMergeTree on this instance, and each
    # one reaches the same startupImpl that carries the one-shot FP1 below. Drop them so they
    # cannot consume it: DROP resolves the table through DatabaseCatalog, which waits for its
    # startup job, so this quiesces the instance instead of racing it.
    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    node.query("DROP TABLE IF EXISTS replicated_table_partitioned SYNC")
    assert node.query("SELECT count() FROM system.replicas").strip() == "0"

    # A fresh name per run: the log-line waits below must not match an earlier run's lines.
    table = f"replicated_table_rearm_{uuid.uuid4().hex}"
    node.query(
        f"CREATE TABLE {table} (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY k"
    )
    node.query(f"DETACH TABLE {table}")

    try:
        node.query("SYSTEM ENABLE FAILPOINT rmt_startup_fail_after_being_leader")
        node.query("SYSTEM ENABLE FAILPOINT rmt_restarting_thread_fail_startup")
        node.query(f"ATTACH TABLE {table}")

        # FP1 fired and its cleanup deactivated the task, and a later attempt then reached
        # the attach branch. Both must be observed, or the two-attempt precondition is
        # absent and the sampling below would race the retry instead of measuring it.
        node.wait_for_log_line(
            f"{table} .ReplicatedMergeTreeAttachThread.: Initialization failed. Error: Code: 999",
            timeout=60,
        )
        node.wait_for_log_line(
            f"{table} .*: Trying to startup table from right now", timeout=60
        )
        # Wait for startupImpl to return as well: the trace above is emitted just before the
        # inline run() and the re-arm, so sampling on it alone can precede them.
        node.wait_for_log_line(
            f"{table} .ReplicatedMergeTreeAttachThread.: Table is initialized",
            timeout=60,
        )
        assert (
            node.query(
                f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'"
            ).strip()
            == "1"
        )

        # FP2 is REGULAR, so every retry keeps failing and the state stays stable: with the
        # fix the task is re-armed and pool-owned on every sample, without it on none.
        pool_owned_query = f"""
            SELECT count() FROM system.background_schedule_pool
            WHERE log_name LIKE '%{table} (ReplicatedMergeTreeRestartingThread)%'
              AND (scheduled OR delayed OR executing)
        """
        owned = [node.query(pool_owned_query).strip() for _ in range(5)]
        assert owned == ["1"] * 5, f"restarting task is not owned by the pool: {owned}"

        node.query("SYSTEM DISABLE FAILPOINT rmt_restarting_thread_fail_startup")
        assert (
            node.query_with_retry(
                f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'",
                check_callback=lambda x: x.strip() == "0",
                retry_count=30,
                sleep_time=1,
            ).strip()
            == "0"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_startup_fail_after_being_leader")
        node.query("SYSTEM DISABLE FAILPOINT rmt_restarting_thread_fail_startup")
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


# The other half of the same window: a replica that DOES activate through the attach path
# must still end up with its periodic ZooKeeper session check armed. Only FP1 is enabled
# here, so the retry's inline run() succeeds while the task is still deactivated - that
# run's own scheduleAfter is therefore refused, and the periodic timer exists only if the
# ensureArmed after it re-arms the task.
def test_periodic_check_armed_after_successful_attach_startup_retry(started_cluster):
    start_clean_clickhouse()
    # Same reason as the previous test: any other ReplicatedMergeTree re-attached by a
    # restart reaches the same startupImpl and would consume the one-shot FP1.
    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    node.query("DROP TABLE IF EXISTS replicated_table_partitioned SYNC")
    assert node.query("SELECT count() FROM system.replicas").strip() == "0"

    # A fresh name per run: the log-line waits below must not match an earlier run's lines.
    table = f"replicated_table_rearm_ok_{uuid.uuid4().hex}"
    # A short check period makes the periodic timer observable in seconds. The restarting
    # thread reads it in its constructor, which runs at ATTACH, so a per-table value works.
    check_period_s = 2
    node.query(
        f"CREATE TABLE {table} (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/{table}', 'r1') "
        f"ORDER BY k SETTINGS zookeeper_session_expiration_check_period = {check_period_s}"
    )
    node.query(f"DETACH TABLE {table}")

    try:
        node.query("SYSTEM ENABLE FAILPOINT rmt_startup_fail_after_being_leader")
        node.query(f"ATTACH TABLE {table}")

        # FP1 failed the first attempt and its cleanup deactivated the task; the retry then
        # reached the attach branch and startupImpl returned, so its inline run() succeeded.
        # All three must be observed, or this is not the shape being measured below.
        node.wait_for_log_line(
            f"{table} .ReplicatedMergeTreeAttachThread.: Initialization failed. Error: Code: 999",
            timeout=60,
        )
        node.wait_for_log_line(
            f"{table} .*: Trying to startup table from right now", timeout=60
        )
        node.wait_for_log_line(
            f"{table} .ReplicatedMergeTreeAttachThread.: Table is initialized",
            timeout=60,
        )

        # Positive control: the replica really did activate, so a green result below cannot
        # come from it having stayed readonly.
        assert (
            node.query(
                f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'"
            ).strip()
            == "0"
        )

        state_query = f"""
            SELECT countIf(scheduled OR delayed OR executing), countIf(delayed)
            FROM system.background_schedule_pool
            WHERE log_name LIKE '%{table} (ReplicatedMergeTreeRestartingThread)%'
        """
        # Sample across several check periods: an un-armed task is owned by no pool
        # collection and so has no row here at all. `delayed` names the timer itself; it is
        # only asserted to occur, because a sample can land while the task is executing.
        owned = []
        delayed = []
        for _ in range(5):
            row = node.query(state_query).strip().split("\t")
            owned.append(row[0])
            delayed.append(row[1])
            time.sleep(check_period_s * 0.8)
        assert (
            owned == ["1"] * 5
        ), f"restarting task is not owned by the pool: {owned}, delayed={delayed}"
        assert (
            "1" in delayed
        ), f"restarting task has no periodic session check armed: {delayed}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_startup_fail_after_being_leader")
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


# The task is never deactivated here (no FP1), so the inline run() on the attach path fails
# and arms its own 100 ms first-failure backoff. Re-arming must keep that delay: cancelling
# it retries immediately, which is what an unconditional activateAndSchedule does.
def test_attach_startup_keeps_the_retry_delay_chosen_by_the_restarting_thread(
    started_cluster,
):
    start_clean_clickhouse()
    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    node.query("DROP TABLE IF EXISTS replicated_table_partitioned SYNC")

    table = f"replicated_table_keep_delay_{uuid.uuid4().hex}"
    node.query(
        f"CREATE TABLE {table} (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY k"
    )
    node.query(f"DETACH TABLE {table}")

    try:
        node.query("SYSTEM ENABLE FAILPOINT rmt_restarting_thread_fail_startup")
        node.query(f"ATTACH TABLE {table}")
        node.wait_for_log_line(
            f"{table} .*: Trying to startup table from right now", timeout=60
        )
        # Enough for the first few backoff steps (100, 300, 600 ms) to be logged.
        time.sleep(4)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_restarting_thread_fail_startup")

    # Only the attempts made after ATTACH: the CREATE above ran its own startup, and the gap
    # between the two is user-driven rather than an interval run() chose.
    def timestamps_of(marker):
        # grep_in_log can repeat a matching line, so key on the timestamp.
        return sorted(
            set(
                re.findall(
                    rf"(\d{{2}}:\d{{2}}:\d{{2}}\.\d+).*{marker}",
                    node.grep_in_log(f"{table} .*{marker}") or "",
                )
            )
        )

    attach = timestamps_of("Trying to startup table from right now")
    assert attach, "the attach path was never reached"
    stamps = [s for s in timestamps_of("Trying to start replica up") if s >= attach[0]]
    assert (
        len(stamps) >= 3
    ), f"too few startup attempts after ATTACH to measure a delay: {len(stamps)}"

    def to_seconds(stamp):
        h, m, s = stamp.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)

    seconds = [to_seconds(s) for s in stamps]
    gaps_ms = [round((b - a) * 1000, 1) for a, b in zip(seconds, seconds[1:])]
    # The first gap is the 100 ms first-failure delay run() chose plus the time the single pool
    # worker took to pick the task up; cancelling that delay leaves only the latter. The later
    # gaps measure that latency directly, since their nominal values are known (300, 600, 1000,
    # 1500 ms): it stayed under 16 ms across every arm measured here.
    assert (
        gaps_ms[0] > 60
    ), f"the retry delay chosen by run() was not preserved: {gaps_ms}"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")

import os
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

import os

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

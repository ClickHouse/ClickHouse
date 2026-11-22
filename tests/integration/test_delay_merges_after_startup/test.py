import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# The merge selector does not merge three fresh tiny parts on its own.
MERGE_AT_ONCE = "min_age_to_force_merge_seconds = 1"


def active_parts(table):
    return node.query(
        f"SELECT count() FROM system.parts WHERE database = 'default' AND table = '{table}' AND active"
    )


def wait_active_parts(table, expected):
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.parts WHERE database = 'default' AND table = '{table}' AND active",
        expected,
        retry_count=240,
        sleep_time=0.5,
    )


def insert_parts(table, count=3):
    for i in range(count):
        node.query(f"INSERT INTO {table} VALUES ({i})")


def create_with_parts(table, engine, settings, partition_by="", count=3):
    node.query(
        f"CREATE TABLE {table} (x UInt64) ENGINE = {engine} ORDER BY x {partition_by} "
        f"SETTINGS {MERGE_AT_ONCE}, {settings}"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    insert_parts(table, count)


def wait_merges_delayed_in_log(table):
    # The table UUID in the logger name matches only this instance of the table.
    uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()
    assert_logs_contain_with_retry(
        node,
        f"{uuid}).*merges are delayed after the table startup",
        retry_count=240,
        sleep_time=0.5,
    )


def test_delay_merges_after_startup(started_cluster):
    # The harness pins the server default to 0, so every table sets the setting explicitly.
    create_with_parts(
        "t_delayed", "MergeTree", "delay_merges_after_startup_seconds = 3600"
    )
    create_with_parts(
        "t_expire", "MergeTree", "delay_merges_after_startup_seconds = 10"
    )
    create_with_parts(
        "t_optimize", "MergeTree", "delay_merges_after_startup_seconds = 3600"
    )
    create_with_parts(
        "t_control", "MergeTree", "delay_merges_after_startup_seconds = 0"
    )
    create_with_parts(
        "t_bypass",
        "MergeTree",
        "delay_merges_after_startup_seconds = 3600, parts_to_delay_insert = 3",
    )
    # 4 parts in total, 2 in each partition.
    create_with_parts(
        "t_partitions",
        "MergeTree",
        "delay_merges_after_startup_seconds = 3600, parts_to_delay_insert = 3",
        partition_by="PARTITION BY x % 2",
        count=4,
    )
    create_with_parts(
        "r_delayed",
        "ReplicatedMergeTree('/clickhouse/tables/r_delayed', 'node')",
        "delay_merges_after_startup_seconds = 3600",
    )

    # `SYSTEM STOP MERGES` is not persisted, so after the restart only the setting holds the merges back.
    node.restart_clickhouse()

    # Merges run in this window when they are not delayed.
    wait_active_parts("t_control", "1\n")
    wait_merges_delayed_in_log("t_delayed")
    assert active_parts("t_delayed") == "3\n"

    # Mutations are not delayed.
    node.query(
        "ALTER TABLE t_delayed DELETE WHERE x = 100 SETTINGS mutations_sync = 1",
        timeout=120,
    )
    assert active_parts("t_delayed") == "3\n"

    # The delay ends on its own, not earlier than its seconds after the server start.
    wait_active_parts("t_expire", "1\n")
    assert (
        node.query(
            "SELECT dateDiff('second', now() - uptime(), min(modification_time)) >= 9 "
            "FROM system.parts WHERE database = 'default' AND table = 't_expire' AND active"
        )
        == "1\n"
    )

    # OPTIMIZE is not delayed.
    node.query("OPTIMIZE TABLE t_optimize FINAL")
    assert active_parts("t_optimize") == "1\n"

    # A partition with `parts_to_delay_insert` parts ends the delay.
    wait_active_parts("t_bypass", "1\n")

    # The parts threshold is per partition, not for the whole table.
    wait_merges_delayed_in_log("t_partitions")
    assert active_parts("t_partitions") == "4\n"

    # A replica postpones the merges of its queue, and the setting is applied at once.
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM system.replication_queue WHERE database = 'default' AND table = 'r_delayed' "
        "AND type = 'MERGE_PARTS' AND postpone_reason LIKE '%delay_merges_after_startup_seconds%'",
        "1\n",
        retry_count=240,
        sleep_time=0.5,
    )
    assert active_parts("r_delayed") == "3\n"
    node.query(
        "ALTER TABLE r_delayed MODIFY SETTING delay_merges_after_startup_seconds = 0"
    )
    wait_active_parts("r_delayed", "1\n")

    # A runtime ATTACH does not delay merges.
    node.query("DETACH TABLE t_delayed")
    node.query("ATTACH TABLE t_delayed")
    wait_active_parts("t_delayed", "1\n")

    # CREATE TABLE does not delay merges.
    node.query(
        "CREATE TABLE t_new (x UInt64) ENGINE = MergeTree ORDER BY x "
        f"SETTINGS {MERGE_AT_ONCE}, delay_merges_after_startup_seconds = 3600"
    )
    insert_parts("t_new")
    wait_active_parts("t_new", "1\n")

    for table in [
        "t_delayed",
        "t_expire",
        "t_optimize",
        "t_control",
        "t_bypass",
        "t_partitions",
        "r_delayed",
        "t_new",
    ]:
        node.query(f"DROP TABLE {table} SYNC")

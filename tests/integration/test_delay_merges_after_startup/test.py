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
LIGHTWEIGHT_UPDATE = "enable_block_number_column = 1, enable_block_offset_column = 1"
# Patch parts of lightweight updates are not counted.
ACTIVE_PARTS = (
    "SELECT count() FROM system.parts WHERE database = 'default' AND table = '{}' "
    "AND active AND NOT startsWith(partition_id, 'patch-')"
)


def active_parts(table):
    return node.query(ACTIVE_PARTS.format(table))


def wait_active_parts(table, expected):
    assert_eq_with_retry(
        node,
        ACTIVE_PARTS.format(table),
        expected,
        retry_count=240,
        sleep_time=0.5,
    )


def insert_parts(table, count=3):
    for i in range(count):
        node.query(f"INSERT INTO {table} (x) VALUES ({i})")


def create_with_parts(table, engine, settings, partition_by="", count=3):
    node.query(
        f"CREATE TABLE {table} (x UInt64, y UInt64) ENGINE = {engine} ORDER BY x {partition_by} "
        f"SETTINGS {MERGE_AT_ONCE}, {settings}"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    insert_parts(table, count)


def merges_delayed_log_line(table):
    # The table UUID in the logger name matches only this instance of the table.
    uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()
    return f"{uuid}).*merges are delayed after the table startup"


def wait_merges_delayed_in_log(table):
    assert_logs_contain_with_retry(
        node, merges_delayed_log_line(table), retry_count=240, sleep_time=0.5
    )


def first_delay_log_time(table):
    line = node.grep_in_log(merges_delayed_log_line(table), only_latest=True)
    # "2026.01.31 12:34:56.789012 ..." -> "2026-01-31 12:34:56"
    return line.splitlines()[0][:19].replace(".", "-", 2)


def wait_merges_postponed(table):
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM system.replication_queue WHERE database = 'default' "
        f"AND table = '{table}' AND type = 'MERGE_PARTS' "
        "AND postpone_reason LIKE '%delay_merges_after_startup_seconds%'",
        "1\n",
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
        "t_insert", "MergeTree", "delay_merges_after_startup_seconds = 3600"
    )
    create_with_parts(
        "t_mutation", "MergeTree", "delay_merges_after_startup_seconds = 3600"
    )
    create_with_parts(
        "t_alter", "MergeTree", "delay_merges_after_startup_seconds = 3600"
    )
    create_with_parts(
        "t_update",
        "MergeTree",
        f"delay_merges_after_startup_seconds = 3600, {LIGHTWEIGHT_UPDATE}",
    )
    # 4 parts in total, 2 in each partition.
    create_with_parts(
        "t_optimize",
        "MergeTree",
        "delay_merges_after_startup_seconds = 3600",
        partition_by="PARTITION BY x % 2",
        count=4,
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
    replicated = ["r_delayed", "r_insert", "r_optimize", "r_mutation", "r_alter"]
    for table in replicated:
        create_with_parts(
            table,
            f"ReplicatedMergeTree('/clickhouse/tables/{table}', 'node')",
            "delay_merges_after_startup_seconds = 3600",
        )
    create_with_parts(
        "r_update",
        "ReplicatedMergeTree('/clickhouse/tables/r_update', 'node')",
        f"delay_merges_after_startup_seconds = 3600, {LIGHTWEIGHT_UPDATE}",
    )
    replicated.append("r_update")

    # `SYSTEM STOP MERGES` is not persisted, so after the restart only the setting holds the merges back.
    node.restart_clickhouse()

    # Merges run in this window when they are not delayed.
    wait_active_parts("t_control", "1\n")
    for table in [
        "t_delayed",
        "t_expire",
        "t_insert",
        "t_mutation",
        "t_alter",
        "t_update",
        "t_optimize",
    ]:
        wait_merges_delayed_in_log(table)
    assert active_parts("t_delayed") == "3\n"

    # An INSERT, a mutation and OPTIMIZE end the delay.
    assert active_parts("t_insert") == "3\n"
    node.query("INSERT INTO t_insert (x) VALUES (100)")
    wait_active_parts("t_insert", "1\n")

    assert active_parts("t_mutation") == "3\n"
    node.query(
        "ALTER TABLE t_mutation DELETE WHERE x = 100 SETTINGS mutations_sync = 1",
        timeout=120,
    )
    wait_active_parts("t_mutation", "1\n")

    # So do the mutation of a column type change and a lightweight update.
    assert active_parts("t_alter") == "3\n"
    node.query("ALTER TABLE t_alter MODIFY COLUMN y String", timeout=120)
    wait_active_parts("t_alter", "1\n")

    assert active_parts("t_update") == "3\n"
    node.query("UPDATE t_update SET y = 1 WHERE x = 0")
    wait_active_parts("t_update", "1\n")

    # Only partition 0 is merged by OPTIMIZE, partition 1 by a background merge.
    assert active_parts("t_optimize") == "4\n"
    node.query("OPTIMIZE TABLE t_optimize PARTITION 0 FINAL")
    wait_active_parts("t_optimize", "2\n")

    # The delay ends on its own, its seconds after the table startup, when the table first logs
    # that its merges are delayed.
    wait_active_parts("t_expire", "1\n")
    startup_time = first_delay_log_time("t_expire")
    assert (
        node.query(
            f"SELECT dateDiff('second', toDateTime('{startup_time}'), min(modification_time)) BETWEEN 8 AND 30 "
            "FROM system.parts WHERE database = 'default' AND table = 't_expire' AND active"
        )
        == "1\n"
    )

    # A partition with `parts_to_delay_insert` parts ends the delay.
    wait_active_parts("t_bypass", "1\n")

    # The parts threshold is per partition, not for the whole table.
    wait_merges_delayed_in_log("t_partitions")
    assert active_parts("t_partitions") == "4\n"

    # A replica postpones the merges of its queue, and the setting is applied at once.
    for table in replicated:
        wait_merges_postponed(table)
        assert active_parts(table) == "3\n"
    node.query(
        "ALTER TABLE r_delayed MODIFY SETTING delay_merges_after_startup_seconds = 0"
    )
    wait_active_parts("r_delayed", "1\n")

    # On a replica too an INSERT, OPTIMIZE and a mutation end the delay. The postponed merge
    # runs at once; merging the inserted part as well waits for the next merge assignment.
    node.query("INSERT INTO r_insert (x) VALUES (100)")
    assert_eq_with_retry(
        node,
        "SELECT count() < 4 FROM system.parts WHERE database = 'default' AND table = 'r_insert' AND active",
        "1\n",
        retry_count=240,
        sleep_time=0.5,
    )
    node.query("OPTIMIZE TABLE r_optimize FINAL", timeout=120)
    assert active_parts("r_optimize") == "1\n"
    node.query(
        "ALTER TABLE r_mutation DELETE WHERE x = 100 SETTINGS mutations_sync = 1",
        timeout=120,
    )
    node.query("ALTER TABLE r_alter MODIFY COLUMN y String", timeout=120)
    node.query("UPDATE r_update SET y = 1 WHERE x = 0")
    wait_active_parts("r_update", "1\n")

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

    # Nor does UNDROP TABLE.
    node.query("SYSTEM STOP MERGES t_new")
    insert_parts("t_new")
    node.query("DROP TABLE t_new")
    node.query("UNDROP TABLE t_new")
    wait_active_parts("t_new", "1\n")

    for table in [
        "t_delayed",
        "t_expire",
        "t_insert",
        "t_mutation",
        "t_alter",
        "t_update",
        "t_optimize",
        "t_control",
        "t_bypass",
        "t_partitions",
        "t_new",
    ] + replicated:
        node.query(f"DROP TABLE {table} SYNC")

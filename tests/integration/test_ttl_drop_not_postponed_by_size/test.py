import logging
import re
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/storage_configuration.xml",
        "configs/fast_background_pool.xml",
    ],
    tmpfs=[
        "/ttl_drop_gate_a:size=64M",
        "/ttl_drop_gate_b:size=64M",
        "/ttl_drop_gate_c:size=64M",
        "/ttl_drop_gate_d:size=64M",
    ],
    with_zookeeper=True,
)

# Two inserts of this many 1 KiB incompressible strings occupy roughly 35 MiB of a 64 MiB
# disk. That puts the data above unreserved_space / 2 (the gate's threshold) while still
# leaving room for a result part. assert_gate_would_fire checks the actual numbers.
ROWS_PER_INSERT = 18000

# Next to one insert of ROWS_PER_INSERT rows, this many 1 KiB strings leave less free space
# than that part holds, so a merge that writes its rows back cannot fit until the filler is
# dropped.
FILLER_ROWS = 34000

# Each case gets its own disk and policy so that dropping one table's data cannot raise the
# other table's threshold.
CASES = {"t_a": "gate_a", "t_b": "gate_b", "t_c": "gate_c", "t_d": "gate_d"}

SIZE_UNITS = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query_int(query):
    return int(node.query(query).strip())


def assert_gate_would_fire(table, partition_id):
    """Fail unless the source-parts-size gate is reachable in the current state.

    Checks both conditions the gate depends on, with the measured numbers in the message:
    the refused comparison (data larger than unreserved_space / 2) and the absence of the
    roomy-disk exemption (that half being below max_bytes_to_merge_at_max_space_in_pool).
    Without this a case could pass without any entry ever reaching the gate.
    """
    disk = CASES[table]
    parts_bytes = query_int(
        f"SELECT sum(bytes_on_disk) FROM system.parts WHERE active "
        f"AND database = currentDatabase() AND table = '{table}' "
        f"AND partition_id = '{partition_id}'"
    )
    unreserved = query_int(
        f"SELECT unreserved_space FROM system.disks WHERE name = '{disk}'"
    )
    pool_limit = query_int(
        "SELECT value FROM system.merge_tree_settings "
        "WHERE name = 'max_bytes_to_merge_at_max_space_in_pool'"
    )
    threshold = unreserved // 2

    logging.info(
        "%s partition %s: %d bytes of parts, disk %s has %d bytes unreserved, gate threshold %d bytes",
        table,
        partition_id,
        parts_bytes,
        disk,
        unreserved,
        threshold,
    )
    assert parts_bytes > threshold, (
        f"gate unreachable: parts of {table} partition {partition_id} hold {parts_bytes} bytes, "
        f"which does not exceed unreserved_space / 2 = {threshold} bytes on disk {disk}"
    )
    assert threshold < pool_limit, (
        f"gate unreachable: unreserved_space / 2 = {threshold} bytes on disk {disk} is not below "
        f"max_bytes_to_merge_at_max_space_in_pool = {pool_limit} bytes, so the size check is "
        f"skipped for every merge"
    )


def ttl_merge_entry(table):
    return node.query(
        "SELECT merge_type, num_postponed, postpone_reason FROM system.replication_queue "
        f"WHERE database = currentDatabase() AND table = '{table}' AND type = 'MERGE_PARTS' "
        "FORMAT Vertical"
    ).strip()


def wait_for_entry_postponed_on_size(table, timeout=60):
    """Wait for a merge entry of `table` to be refused by the source-parts-size gate."""
    deadline = time.monotonic() + timeout
    entry = ""
    while time.monotonic() < deadline:
        entry = ttl_merge_entry(table)
        if "source parts size" in entry:
            return entry
        time.sleep(1)
    raise AssertionError(
        f"no merge entry of {table} was postponed on source parts size within {timeout}s; "
        f"last queue state:\n{entry}"
    )


def wait_for_queued_drop(table, timeout=90):
    """Wait for a whole-part drop of `table` to be queued without having been tried."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        queued = query_int(
            "SELECT count() FROM system.replication_queue "
            f"WHERE database = currentDatabase() AND table = '{table}' "
            "AND type = 'MERGE_PARTS' AND merge_type = 'TTLDrop' AND num_postponed = 0"
        )
        if queued > 0:
            return
        time.sleep(1)
    raise AssertionError(
        f"no whole-part drop of {table} was queued within {timeout}s; last queue state:\n"
        + ttl_merge_entry(table)
    )


def part_bytes(table):
    return query_int(
        "SELECT sum(bytes_on_disk) FROM system.parts WHERE active "
        f"AND database = currentDatabase() AND table = '{table}'"
    )


def insert_filler(policy):
    """Occupy most of the free space of the disk behind `policy` with a table of its own."""
    node.query("DROP TABLE IF EXISTS filler SYNC")
    node.query(
        "CREATE TABLE filler (s String) ENGINE = MergeTree ORDER BY tuple() "
        f"SETTINGS storage_policy = '{policy}'"
    )
    node.query(
        f"INSERT INTO filler SELECT randomString(1024) FROM numbers({FILLER_ROWS})"
    )


def part_log_rows(table, condition="1"):
    """part_log rows of the current incarnation of `table` that match `condition`."""
    node.query("SYSTEM FLUSH LOGS part_log")
    return node.query(
        "SELECT event_type, merge_reason, error, exception FROM system.part_log "
        f"WHERE database = currentDatabase() AND table = '{table}' AND table_uuid = "
        f"(SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '{table}') "
        f"AND {condition} ORDER BY event_time_microseconds FORMAT Vertical"
    ).strip()


def wait_for_part_log(table, condition, timeout=90):
    """Wait for a part_log row of `table` that matches `condition`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if part_log_rows(table, condition):
            return
        time.sleep(1)
    raise AssertionError(
        f"no part_log row of {table} with {condition} within {timeout}s; all its rows:\n"
        + part_log_rows(table)
    )


def max_reserved_bytes_since(disk, since):
    """The largest space reservation made on `disk` after `since`, read from the server log."""
    node.query("SYSTEM FLUSH LOGS text_log")
    messages = node.query(
        "SELECT message FROM system.text_log "
        f"WHERE event_time_microseconds > '{since}' "
        f"AND message LIKE 'Reserved % on local disk `{disk}`%'"
    )
    sizes = [
        float(value) * SIZE_UNITS[unit]
        for value, unit in re.findall(r"Reserved ([0-9.]+) (B|KiB|MiB|GiB) ", messages)
    ]
    logging.info("reservations on disk %s since %s: %s bytes", disk, since, sizes)
    return max(sizes, default=0)


def not_enough_space_errors():
    return query_int(
        "SELECT sum(value) FROM system.errors WHERE name = 'NOT_ENOUGH_SPACE'"
    )


def test_fully_expired_drop_is_not_postponed_by_source_size(started_cluster):
    """A whole-part TTL drop of fully expired parts must run however small the disk is.

    It deletes every row, so its result part is empty and the source bytes are not a space
    requirement. Refusing it on source size is self-sustaining: the refused entry occupies
    the single max_replicated_merges_with_ttl_in_queue slot, so no further TTL merge is
    assigned, no space is freed, and the threshold the entry is refused against never rises.
    """
    node.query("DROP TABLE IF EXISTS t_a SYNC")
    node.query(
        """
        CREATE TABLE t_a (id UInt64, s String, event_time DateTime)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/ttl_drop_gate/t_a', 'r1')
        ORDER BY id
        TTL event_time + INTERVAL 1 DAY
        SETTINGS storage_policy = 'only_a',
                 ttl_only_drop_parts = 1,
                 merge_with_ttl_timeout = 0,
                 max_replicated_merges_with_ttl_in_queue = 1,
                 min_bytes_for_wide_part = 1
        """
    )
    # Stopping TTL merges also stops TTL merge *assignment*, so the log entry is created
    # only once the disk holds the data that assert_gate_would_fire measures.
    node.query("SYSTEM STOP TTL MERGES t_a")

    for _ in range(2):
        node.query(
            "INSERT INTO t_a SELECT number, randomString(1024), now() - INTERVAL 10 DAY "
            f"FROM numbers({ROWS_PER_INSERT})"
        )

    assert_gate_would_fire("t_a", "all")

    node.query("SYSTEM START TTL MERGES t_a")

    assert_eq_with_retry(
        node, "SELECT count() FROM t_a", "0", retry_count=120, sleep_time=1
    )

    node.query("SYSTEM FLUSH LOGS part_log")
    assert (
        query_int(
            "SELECT count() FROM system.part_log WHERE database = currentDatabase() "
            "AND table = 't_a' AND event_type = 'MergeParts' "
            "AND merge_reason = 'TTLDropMerge'"
        )
        > 0
    ), "the data was removed by something other than a TTL drop merge"

    node.query("DROP TABLE t_a SYNC")


def test_row_retaining_drop_is_still_postponed_by_source_size(started_cluster):
    """A whole-part drop that keeps rows must keep the gate and reserve room for its rows.

    A GROUP BY TTL expires a part as a whole, so the merge is selected and tagged the same
    way as the case above, but it aggregates the rows instead of deleting them and writes
    them back. The drop is queued while the data still fits, and the disk is filled before
    it runs: the replica must postpone it on source parts size, and once there is room
    again it must reserve at least what the part holds.
    """
    node.query("DROP TABLE IF EXISTS t_b SYNC")
    node.query("DROP TABLE IF EXISTS filler SYNC")
    node.query(
        """
        CREATE TABLE t_b (id UInt64, v UInt64, s String, event_time DateTime)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/ttl_drop_gate/t_b', 'r1')
        ORDER BY id
        TTL event_time + INTERVAL 1 DAY GROUP BY id SET v = max(v)
        SETTINGS storage_policy = 'only_b',
                 ttl_only_drop_parts = 1,
                 merge_with_ttl_timeout = 0,
                 max_replicated_merges_with_ttl_in_queue = 1,
                 min_bytes_for_wide_part = 1
        """
    )
    node.query("SYSTEM STOP TTL MERGES t_b")
    node.query(
        "INSERT INTO t_b SELECT number, number, randomString(1024), now() - INTERVAL 10 DAY "
        f"FROM numbers({ROWS_PER_INSERT})"
    )
    rows_before = query_int("SELECT count() FROM t_b")
    bytes_before = part_bytes("t_b")

    # Stops only the execution of queue entries, so the drop is still assigned and queued.
    node.query("SYSTEM STOP REPLICATION QUEUES t_b")
    node.query("SYSTEM START TTL MERGES t_b")
    wait_for_queued_drop("t_b")

    insert_filler("only_b")
    assert_gate_would_fire("t_b", "all")

    node.query("SYSTEM START REPLICATION QUEUES t_b")
    entry = wait_for_entry_postponed_on_size("t_b")
    assert "TTLDrop" in entry, (
        "the postponed entry is not the whole-part drop this case is about, so it does not "
        f"discriminate the exempted class:\n{entry}"
    )
    assert query_int("SELECT count() FROM t_b") == rows_before, (
        "rows were removed although the merge was supposed to stay postponed:\n" + entry
    )

    since = node.query("SELECT now64(6)").strip()
    node.query("DROP TABLE filler SYNC")
    wait_for_part_log(
        "t_b",
        "event_type = 'MergeParts' AND merge_reason = 'TTLDropMerge' AND error = 0",
    )
    reserved = max_reserved_bytes_since("gate_b", since)
    assert reserved >= bytes_before, (
        f"the drop of t_b reserved {reserved:.0f} bytes on disk gate_b, less than the "
        f"{bytes_before} bytes of rows it writes back"
    )

    node.query("DROP TABLE t_b SYNC")


@pytest.mark.parametrize(
    "ttl",
    [
        "event_time + INTERVAL 1 DAY GROUP BY id SET v = max(v)",
        "delete_at DELETE, event_time + INTERVAL 1 DAY GROUP BY id SET v = max(v)",
    ],
    ids=["group_by", "delete_without_value"],
)
def test_row_retaining_drop_waits_for_free_space(started_cluster, ttl):
    """A whole-part drop that keeps rows must not start while the disk cannot hold them.

    Same GROUP BY TTL as above on a non-replicated table, also next to a DELETE TTL that has
    no value for any row. With less free space than the expired part holds, the merge must
    not be attempted: neither started, nor selected and then refused for lack of space. Once
    there is room, the rollup runs, reserves at least what the part holds and keeps every
    row, since all ids are distinct.
    """
    node.query("DROP TABLE IF EXISTS t_c SYNC")
    node.query("DROP TABLE IF EXISTS filler SYNC")
    node.query(
        f"""
        CREATE TABLE t_c (id UInt64, v UInt64, s String, event_time DateTime, delete_at DateTime DEFAULT 0)
        ENGINE = MergeTree
        ORDER BY id
        TTL {ttl}
        SETTINGS storage_policy = 'only_c',
                 ttl_only_drop_parts = 1,
                 merge_with_ttl_timeout = 0,
                 min_bytes_for_wide_part = 1
        """
    )
    node.query("SYSTEM STOP TTL MERGES t_c")
    node.query(
        "INSERT INTO t_c (id, v, s, event_time) "
        "SELECT number, number, randomString(1024), now() - INTERVAL 10 DAY "
        f"FROM numbers({ROWS_PER_INSERT})"
    )
    bytes_before = part_bytes("t_c")
    insert_filler("only_c")
    assert_gate_would_fire("t_c", "all")

    refused_before = not_enough_space_errors()
    node.query("SYSTEM START TTL MERGES t_c")
    # Many merge selection rounds: a drop that ignores free space starts within a second.
    time.sleep(15)
    started = part_log_rows("t_c", "event_type IN ('MergePartsStart', 'MergeParts')")
    assert not started, (
        f"a merge of t_c started although the disk cannot hold its rows:\n{started}"
    )
    refused = not_enough_space_errors() - refused_before
    assert refused == 0, (
        f"a merge of t_c was selected and then refused for lack of space {refused} times"
    )
    assert query_int("SELECT count() FROM t_c") == ROWS_PER_INSERT

    part_name = node.query(
        "SELECT name FROM system.parts WHERE active "
        "AND database = currentDatabase() AND table = 't_c'"
    ).strip()
    error = node.query_and_get_error(f"OPTIMIZE TABLE t_c DRY RUN PARTS '{part_name}'")
    assert "NOT_ENOUGH_SPACE" in error, (
        "the dry run of t_c was not refused at reservation although the disk cannot hold "
        f"its rows:\n{error}"
    )

    since = node.query("SELECT now64(6)").strip()
    node.query("DROP TABLE filler SYNC")
    wait_for_part_log(
        "t_c",
        "event_type = 'MergeParts' AND merge_reason = 'TTLDropMerge' AND error = 0",
    )
    reserved = max_reserved_bytes_since("gate_c", since)
    assert reserved >= bytes_before, (
        f"the drop of t_c reserved {reserved:.0f} bytes on disk gate_c, less than the "
        f"{bytes_before} bytes of rows it writes back"
    )
    assert query_int("SELECT count() FROM t_c") == ROWS_PER_INSERT

    node.query("DROP TABLE t_c SYNC")


def test_drop_under_delete_ttl_ignores_free_space(started_cluster):
    """A DELETE TTL next to another TTL still empties a fully expired part.

    So its drop needs no free space and must run however full the disk is.
    """
    node.query("DROP TABLE IF EXISTS t_d SYNC")
    node.query("DROP TABLE IF EXISTS filler SYNC")
    node.query(
        """
        CREATE TABLE t_d (id UInt64, s String, event_time DateTime)
        ENGINE = MergeTree
        ORDER BY id
        TTL event_time + INTERVAL 10 YEAR RECOMPRESS CODEC(ZSTD(1)),
            event_time + INTERVAL 1 DAY DELETE
        SETTINGS storage_policy = 'only_d',
                 ttl_only_drop_parts = 1,
                 merge_with_ttl_timeout = 0,
                 min_bytes_for_wide_part = 1
        """
    )
    node.query("SYSTEM STOP TTL MERGES t_d")
    node.query(
        "INSERT INTO t_d SELECT number, randomString(1024), now() - INTERVAL 10 DAY "
        f"FROM numbers({ROWS_PER_INSERT})"
    )
    insert_filler("only_d")
    assert_gate_would_fire("t_d", "all")

    node.query("SYSTEM START TTL MERGES t_d")

    assert_eq_with_retry(
        node, "SELECT count() FROM t_d", "0", retry_count=60, sleep_time=1
    )
    assert part_log_rows(
        "t_d",
        "event_type = 'MergeParts' AND merge_reason = 'TTLDropMerge' AND error = 0",
    ), "the data was removed by something other than a TTL drop merge"

    node.query("DROP TABLE filler SYNC")
    node.query("DROP TABLE t_d SYNC")

import logging
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
    tmpfs=["/ttl_drop_gate_a:size=64M", "/ttl_drop_gate_b:size=64M"],
    with_zookeeper=True,
)

# Two inserts of this many 1 KiB incompressible strings occupy roughly 35 MiB of a 64 MiB
# disk. That puts the data above unreserved_space / 2 (the gate's threshold) while still
# leaving room for a result part. assert_gate_would_fire checks the actual numbers.
ROWS_PER_INSERT = 18000

# Each case gets its own disk and policy so that dropping one table's data cannot raise the
# other table's threshold.
CASES = {"t_a": "gate_a", "t_b": "gate_b"}


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
    """A whole-part drop that keeps rows must keep the gate.

    A GROUP BY TTL expires a part as a whole, so the merge is selected and tagged the same
    way as the case above, but it aggregates the rows instead of deleting them and writes
    them back. Its result is as large as its sources, so it does need the headroom the gate
    checks for.
    """
    node.query("DROP TABLE IF EXISTS t_b SYNC")
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

    for _ in range(2):
        node.query(
            "INSERT INTO t_b SELECT number, number, randomString(1024), now() - INTERVAL 10 DAY "
            f"FROM numbers({ROWS_PER_INSERT})"
        )
    rows_before = query_int("SELECT count() FROM t_b")

    assert_gate_would_fire("t_b", "all")

    node.query("SYSTEM START TTL MERGES t_b")

    entry = wait_for_entry_postponed_on_size("t_b")
    assert "TTLDrop" in entry, (
        "the postponed entry is not the whole-part drop this case is about, so it does not "
        f"discriminate the exempted class:\n{entry}"
    )
    assert query_int("SELECT count() FROM t_b") == rows_before, (
        "rows were removed although the merge was supposed to stay postponed:\n" + entry
    )

    node.query("DROP TABLE t_b SYNC")

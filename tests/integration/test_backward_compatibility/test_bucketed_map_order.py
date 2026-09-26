"""
Test backward compatibility for bucketed Map serialization with MapBucketIndexes fix.

Parts written by older ClickHouse versions (with bucketed Map from PR #99200 but without
the MapBucketIndexes fix) have no bucket_indexes stream. The new code must detect this
via check_stream_exists_callback and fall back to the unordered collectMapFromBuckets path.
"""

import pytest

from helpers.cluster import ClickHouseCluster

# First stable release with bucketed Map (PR #99200), before the bucket index fix.
# Pinned to exact patch tag because the fix will be backported; a floating minor
# tag like "26.4" could resolve to a patched release that already includes it.
OLD_VERSION = "26.4.1.1141"

TABLE_SETTINGS_WIDE = """
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types'
"""

TABLE_SETTINGS_COMPACT = """
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    serialization_info_version = 'with_types'
"""

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=OLD_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_bucketed_map_backward_compatibility(start_cluster):
    """
    All backward compatibility scenarios in a single test to avoid redundant restarts.
    Creates all tables and inserts data on the old version, upgrades once, then verifies.
    """

    # --- Phase 1: Create tables and insert data on old version ---

    # Wide parts table
    node.query(
        f"""
        CREATE TABLE t_wide (id UInt64, m Map(String, UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS {TABLE_SETTINGS_WIDE}
        """
    )
    node.query(
        """
        INSERT INTO t_wide VALUES
            (1, {'z':1, 'a':2, 'm':3}),
            (2, {'dog':10, 'ant':20, 'cat':30})
        """
    )

    # Compact parts table
    node.query(
        f"""
        CREATE TABLE t_compact (id UInt64, m Map(String, UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS {TABLE_SETTINGS_COMPACT}
        """
    )
    node.query(
        """
        INSERT INTO t_compact VALUES
            (1, {'z':1, 'a':2, 'm':3}),
            (2, {'dog':10, 'ant':20, 'cat':30})
        """
    )

    # Table for merge test (old part will be merged with a new part after upgrade)
    node.query(
        f"""
        CREATE TABLE t_merge (id UInt64, m Map(String, UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS {TABLE_SETTINGS_WIDE}
        """
    )
    node.query("INSERT INTO t_merge VALUES (1, {'z':1, 'a':2})")

    # Table for subcolumn test (wide parts)
    node.query(
        f"""
        CREATE TABLE t_sub (id UInt64, m Map(String, UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS {TABLE_SETTINGS_WIDE}
        """
    )
    node.query("INSERT INTO t_sub VALUES (1, {'z':1, 'a':2, 'm':3})")

    # Table for subcolumn test (compact parts)
    node.query(
        f"""
        CREATE TABLE t_sub_compact (id UInt64, m Map(String, UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS {TABLE_SETTINGS_COMPACT}
        """
    )
    node.query("INSERT INTO t_sub_compact VALUES (1, {'z':1, 'a':2, 'm':3})")

    # --- Phase 2: Upgrade to latest version (single restart) ---

    node.restart_with_latest_version()

    # --- Phase 3: Verify old parts have NO bucket_indexes stream ---

    # Old parts were written before the fix, so they must not have the bucket_indexes stream.
    # This confirms we're actually exercising the check_stream_exists_callback fallback path.
    for table in ["t_wide", "t_compact", "t_merge", "t_sub", "t_sub_compact"]:
        result = node.query(
            f"""
            SELECT has(substreams, 'm.bucket_indexes')
            FROM system.parts_columns
            WHERE database = currentDatabase() AND table = '{table}'
                AND column = 'm' AND active = 1
            LIMIT 1
            """
        ).strip()
        assert result == "0", f"Old part in {table} should not have bucket_indexes stream"

    # --- Phase 4a: Verify old wide parts are readable ---

    assert node.query("SELECT count() FROM t_wide").strip() == "2"
    assert (
        node.query("SELECT id, length(m) FROM t_wide ORDER BY id").strip()
        == "1\t3\n2\t3"
    )
    # Subcolumn access on old wide parts
    assert (
        node.query("SELECT id, m['z'], m['dog'] FROM t_wide ORDER BY id").strip()
        == "1\t1\t0\n2\t0\t10"
    )

    # --- Phase 4b: Verify old compact parts are readable ---

    assert node.query("SELECT count() FROM t_compact").strip() == "2"
    assert (
        node.query("SELECT id, length(m) FROM t_compact ORDER BY id").strip()
        == "1\t3\n2\t3"
    )
    assert (
        node.query("SELECT id, m['z'], m['dog'] FROM t_compact ORDER BY id").strip()
        == "1\t1\t0\n2\t0\t10"
    )

    # --- Phase 5: Insert new data and merge with old parts ---

    # New part has bucket_indexes stream
    node.query("INSERT INTO t_merge VALUES (2, {'x':10, 'b':20})")

    # Verify the new part has bucket_indexes
    assert (
        node.query(
            """
            SELECT has(substreams, 'm.bucket_indexes')
            FROM system.parts_columns
            WHERE database = currentDatabase() AND table = 't_merge'
                AND column = 'm' AND active = 1 AND level = 0
            ORDER BY name DESC
            LIMIT 1
            """
        ).strip()
        == "1"
    )

    # Merge old (no bucket_indexes) + new (has bucket_indexes) parts
    node.query("OPTIMIZE TABLE t_merge FINAL")

    # Merged part has bucket_indexes
    assert (
        node.query(
            """
            SELECT has(substreams, 'm.bucket_indexes')
            FROM system.parts_columns
            WHERE database = currentDatabase() AND table = 't_merge'
                AND column = 'm' AND active = 1
            LIMIT 1
            """
        ).strip()
        == "1"
    )

    assert node.query("SELECT count() FROM t_merge").strip() == "2"

    # New data has correct key order after merge
    assert (
        node.query("SELECT id, mapKeys(m) FROM t_merge WHERE id = 2").strip()
        == "2\t['x','b']"
    )

    # ORDER BY on merged data produces a deterministic result
    result = node.query("SELECT id FROM t_merge ORDER BY m").strip()
    assert len(result.split("\n")) == 2

    # --- Phase 6: Verify subcolumns on old wide parts ---

    # map.keys subcolumn (uses SerializationMapKeysOrValues path)
    result = node.query("SELECT m.keys FROM t_sub").strip()
    assert len(result) > 0

    # map.values subcolumn
    result = node.query("SELECT m.values FROM t_sub").strip()
    assert len(result) > 0

    # map.size0 subcolumn
    assert node.query("SELECT m.size0 FROM t_sub").strip() == "3"

    # --- Phase 7: Verify subcolumns on old compact parts ---
    # This exercises the enumerateStreams check_stream_exists_callback path in
    # initSubcolumnsDeserializationOrder — old compact parts lack the bucket_indexes
    # stream and enumerating it unconditionally would cause "Unexpected substream" errors.

    # map.keys subcolumn on compact part
    result = node.query("SELECT m.keys FROM t_sub_compact").strip()
    assert len(result) > 0

    # map.values subcolumn on compact part
    result = node.query("SELECT m.values FROM t_sub_compact").strip()
    assert len(result) > 0

    # map.size0 subcolumn on compact part
    assert node.query("SELECT m.size0 FROM t_sub_compact").strip() == "3"

    # --- Cleanup ---

    node.query("DROP TABLE t_wide")
    node.query("DROP TABLE t_compact")
    node.query("DROP TABLE t_merge")
    node.query("DROP TABLE t_sub")
    node.query("DROP TABLE t_sub_compact")

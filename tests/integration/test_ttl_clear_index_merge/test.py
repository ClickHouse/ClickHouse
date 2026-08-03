import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    with_minio=True,
    main_configs=["configs/storage.xml"],
    user_configs=["configs/enable_ttl_clear_index.xml"],
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    with_minio=True,
    main_configs=["configs/storage.xml"],
    user_configs=["configs/enable_ttl_clear_index.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def event_value(node, event):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def create_table(node, replica):
    node.query(
        f"""
        CREATE TABLE ttl_clear_index
        (
            d Date,
            k UInt64,
            v UInt64,
            INDEX idx v TYPE minmax GRANULARITY 1
        )
        ENGINE = ReplicatedMergeTree(
            '/clickhouse/tables/ttl_clear_index_merge',
            '{replica}')
        ORDER BY k
        TTL d + INTERVAL 1 DAY CLEAR INDEX idx
        SETTINGS
            always_fetch_merged_part = 1,
            index_granularity = 2,
            index_granularity_bytes = '10Mi',
            merge_with_ttl_timeout = 1,
            min_bytes_for_wide_part = 0,
            min_rows_for_wide_part = 0,
            storage_policy = 's3_only'
        """
    )


def test_alter_ttl_recalculates_stale_metadata_before_index_clear(started_cluster):
    node1.query("SYSTEM STOP TTL MERGES")
    node1.query(
        """
        CREATE TABLE ttl_clear_index_stale
        (
            delete_at Date,
            clear_at Date,
            k UInt64,
            INDEX idx k TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY k
        TTL clear_at + INTERVAL 1 DAY CLEAR INDEX idx
        SETTINGS
            index_granularity = 2,
            index_granularity_bytes = '10Mi',
            merge_with_ttl_timeout = 1,
            min_bytes_for_wide_part = 0,
            min_rows_for_wide_part = 0
        """
    )
    node1.query(
        "INSERT INTO ttl_clear_index_stale VALUES "
        "('2000-01-01', '2100-01-01', 1), "
        "('2100-01-01', '2100-01-01', 2)"
    )
    node1.query(
        """
        ALTER TABLE ttl_clear_index_stale MODIFY TTL
            delete_at + INTERVAL 1 DAY DELETE,
            clear_at + INTERVAL 1 DAY CLEAR INDEX idx
        SETTINGS materialize_ttl_after_modify = 0
        """
    )

    node1.query("SYSTEM START TTL MERGES")
    assert_eq_with_retry(
        node1,
        "SELECT count() FROM ttl_clear_index_stale",
        "1",
        retry_count=60,
    )
    assert node1.query("SELECT k FROM ttl_clear_index_stale") == "2\n"
    assert (
        node1.query(
            """
            SELECT sum(secondary_indices_compressed_bytes) > 0
            FROM system.parts
            WHERE database = currentDatabase()
              AND table = 'ttl_clear_index_stale'
              AND active
            """
        )
        == "1\n"
    )

    node1.query("SYSTEM FLUSH LOGS")
    assert_eq_with_retry(
        node1,
        """
        SELECT count() > 0
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 'ttl_clear_index_stale'
          AND event_type = 'MergeParts'
          AND merge_reason = 'RegularMerge'
          AND error = 0
        """,
        "1",
    )
    assert (
        node1.query(
            """
            SELECT count()
            FROM system.part_log
            WHERE database = currentDatabase()
              AND table = 'ttl_clear_index_stale'
              AND event_type = 'MergeParts'
              AND merge_reason = 'TTLClearIndexMerge'
            """
        )
        == "0\n"
    )
    node1.query("DROP TABLE ttl_clear_index_stale SYNC")


def test_source_replica_produces_ttl_clear_index_merge(started_cluster):
    create_table(node1, "r1")
    create_table(node2, "r2")

    node1.query("SYSTEM STOP TTL MERGES ttl_clear_index")
    node2.query("SYSTEM STOP TTL MERGES ttl_clear_index")
    node1.query(
        "INSERT INTO ttl_clear_index VALUES "
        "('2000-01-01', 1, 1), ('2000-01-01', 2, 2)"
    )
    node2.query("SYSTEM SYNC REPLICA ttl_clear_index")

    index_size_query = """
        SELECT sum(secondary_indices_compressed_bytes) > 0
        FROM system.parts
        WHERE database = currentDatabase()
          AND table = 'ttl_clear_index'
          AND active
    """
    assert node1.query(index_size_query) == "1\n"
    assert node2.query(index_size_query) == "1\n"

    source_merges_before = event_value(node1, "TTLClearIndexMetadataOnlyMerges")
    source_mismatches_before = event_value(node1, "DataAfterMergeDiffersFromReplica")
    follower_mismatches_before = event_value(node2, "DataAfterMergeDiffersFromReplica")
    follower_fetches_before = event_value(node2, "ReplicatedPartFetches")

    node1.query("SYSTEM START TTL MERGES ttl_clear_index")
    assert_eq_with_retry(
        node1,
        "SELECT sum(value) > {} FROM system.events "
        "WHERE event = 'TTLClearIndexMetadataOnlyMerges'".format(source_merges_before),
        "1",
        retry_count=60,
    )
    assert_eq_with_retry(node1, index_size_query, "0", retry_count=60)

    node1.query("SYSTEM FLUSH LOGS")
    assert_eq_with_retry(
        node1,
        """
        SELECT count() > 0
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 'ttl_clear_index'
          AND event_type = 'MergeParts'
          AND merge_reason = 'TTLClearIndexMerge'
          AND error = 0
        """,
        "1",
    )

    node2.query("SYSTEM START TTL MERGES ttl_clear_index")
    node2.query("SYSTEM SYNC REPLICA ttl_clear_index")
    assert event_value(node2, "ReplicatedPartFetches") > follower_fetches_before
    assert node2.query(index_size_query) == "0\n"

    assert event_value(node1, "DataAfterMergeDiffersFromReplica") == source_mismatches_before
    assert event_value(node2, "DataAfterMergeDiffersFromReplica") == follower_mismatches_before

    part_identity_query = """
        SELECT
            name,
            hex(hash_of_all_files),
            hex(hash_of_uncompressed_files),
            hex(uncompressed_hash_of_compressed_files)
        FROM system.parts
        WHERE database = currentDatabase()
          AND table = 'ttl_clear_index'
          AND active
        ORDER BY name
    """
    assert node1.query(part_identity_query) == node2.query(part_identity_query)
    assert node1.query("SELECT sum(v), count() FROM ttl_clear_index") == "3\t2\n"
    assert node2.query("SELECT sum(v), count() FROM ttl_clear_index") == "3\t2\n"
    assert node1.query("CHECK TABLE ttl_clear_index") == "1\n"
    assert node2.query("CHECK TABLE ttl_clear_index") == "1\n"

    node2.query("DROP TABLE ttl_clear_index SYNC")
    node1.query("DROP TABLE ttl_clear_index SYNC")

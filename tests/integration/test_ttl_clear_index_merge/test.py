import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    with_minio=True,
    main_configs=["configs/storage.xml"],
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    with_minio=True,
    main_configs=["configs/storage.xml"],
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


def create_table(node, table, replica, columns="", settings=""):
    node.query(
        f"""
        CREATE TABLE {table}
        (
            d Date,
            k UInt64,
            v UInt64,
            INDEX idx v TYPE minmax GRANULARITY 1,
            PROJECTION by_v (SELECT k, v ORDER BY v)
            {columns}
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}')
        ORDER BY k
        TTL d + INTERVAL 1 DAY CLEAR INDEX idx
        SETTINGS
            ttl_clear_index_merges = 1,
            always_fetch_merged_part = 0,
            index_granularity = 2,
            min_bytes_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0
            {settings}
        """
    )
    node.query(f"SYSTEM STOP TTL MERGES {table}")


def query_both(query):
    return [node.query(query) for node in (node1, node2)]


def active_parts_query(table, columns):
    return (
        f"SELECT {columns} FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' AND active"
    )


def wait_cleared(table):
    for node in (node1, node2):
        assert_eq_with_retry(
            node,
            active_parts_query(table, "sum(secondary_indices_compressed_bytes)"),
            "0",
            retry_count=60,
        )
        node.query(f"SYSTEM SYNC REPLICA {table}")


def merge_events(node, table, part):
    node.query("SYSTEM FLUSH LOGS part_log")
    return node.query(
        "SELECT event_type, merge_reason FROM system.part_log "
        f"WHERE database = currentDatabase() AND table = '{table}' "
        f"AND part_name = '{part}' AND event_type IN ('MergeParts', 'DownloadPart')"
    )


def test_replica_with_leftover_entries_clears_locally(started_cluster):
    """A replica whose part directory holds entries that are not part of the part, like a
    leftover `.tmp_proj`, still produces the same part as the other replica."""
    table = "ttl_clear_index_leftovers"
    create_table(node1, table, "r1")
    create_table(node2, table, "r2")
    node1.query(f"INSERT INTO {table} VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2)")
    node2.query(f"SYSTEM SYNC REPLICA {table}")

    part_path = node1.query(active_parts_query(table, "path")).strip()
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f'mkdir -p "{part_path}by_v.tmp_proj" "{part_path}by_v.proj/nested" '
            f'&& echo junk > "{part_path}stray.txt"',
        ],
        privileged=True,
        user="root",
    )

    fetches_before = [event_value(node, "ReplicatedPartFetches") for node in (node1, node2)]
    for node in (node1, node2):
        node.query(f"SYSTEM START TTL MERGES {table}")
    wait_cleared(table)

    result_part = node1.query(active_parts_query(table, "name")).strip()
    for node, fetches in zip((node1, node2), fetches_before):
        assert event_value(node, "ReplicatedPartFetches") == fetches
        assert merge_events(node, table, result_part) == "MergeParts\tTTLClearIndexMerge\n"

    identity = active_parts_query(table, "name, hash_of_all_files, hash_of_uncompressed_files")
    assert node1.query(identity) == node2.query(identity)

    result_path = node1.query(active_parts_query(table, "path")).strip()
    leftovers = node1.exec_in_container(
        [
            "bash",
            "-c",
            f'ls -A "{result_path}" | grep -c -e "tmp_proj" -e "stray.txt"; '
            f'ls -A "{result_path}by_v.proj" | grep -c -x "nested"; true',
        ],
        privileged=True,
        user="root",
    )
    assert leftovers == "0\n0\n"

    assert query_both(f"CHECK TABLE {table} SETTINGS check_query_single_value_result = 1") == ["1\n"] * 2
    assert query_both(f"SELECT sum(v), count() FROM {table}") == ["3\t2\n"] * 2

    node2.query(f"DROP TABLE {table} SYNC")
    node1.query(f"DROP TABLE {table} SYNC")


def test_s3_clear_after_alter_hardlinks_files(started_cluster):
    """On S3 each replica hardlinks its own objects into the cleared part. `ADD COLUMN` raises the
    table metadata version without rewriting the part, and the `TTLClearIndex` merge still hardlinks the part's files."""
    table = "ttl_clear_index_s3"
    for node, replica in ((node1, "r1"), (node2, "r2")):
        create_table(node, table, replica, settings=", storage_policy = 's3_only'")
    node1.query(f"INSERT INTO {table} VALUES ('2000-01-01', 1, 2), ('2000-01-01', 2, 1)")
    node2.query(f"SYSTEM SYNC REPLICA {table}")

    source_part = node1.query(active_parts_query(table, "name")).strip()
    node1.query(f"ALTER TABLE {table} ADD COLUMN extra UInt64 DEFAULT 7 SETTINGS alter_sync = 2")
    assert query_both(active_parts_query(table, "name")) == [source_part + "\n"] * 2

    def remote_path_of(node, part, file_name):
        return node.query(
            "SELECT remote_path FROM system.remote_data_paths "
            f"WHERE disk_name = 's3_remote' AND local_path LIKE '%/{part}/{file_name}'"
        ).strip()

    source_blobs = [remote_path_of(node, source_part, "k.bin") for node in (node1, node2)]
    assert "" not in source_blobs

    fetches_before = [event_value(node, "ReplicatedPartFetches") for node in (node1, node2)]
    for node in (node1, node2):
        node.query(f"SYSTEM START TTL MERGES {table}")
    wait_cleared(table)

    result_part = node1.query(active_parts_query(table, "name")).strip()
    assert result_part != source_part
    for node, blob, fetches in zip((node1, node2), source_blobs, fetches_before):
        # Fails if the replica copied, rewrote, or fetched the column file.
        assert remote_path_of(node, result_part, "k.bin") == blob
        assert event_value(node, "ReplicatedPartFetches") == fetches
        assert merge_events(node, table, result_part) == "MergeParts\tTTLClearIndexMerge\n"

    identity = active_parts_query(table, "name, hash_of_all_files, hash_of_uncompressed_files")
    assert node1.query(identity) == node2.query(identity)
    assert query_both(active_parts_query(table, "projections")) == ["['by_v']\n"] * 2
    projection_query = (
        f"SELECT k FROM {table} ORDER BY v "
        "SETTINGS optimize_use_projections = 1, force_optimize_projection = 1"
    )
    assert query_both(projection_query) == ["2\n1\n"] * 2
    assert query_both(f"SELECT sum(v), sum(extra), count() FROM {table}") == ["3\t14\t2\n"] * 2
    assert query_both(f"CHECK TABLE {table} SETTINGS check_query_single_value_result = 1") == ["1\n"] * 2

    node2.query(f"DROP TABLE {table} SYNC")
    node1.query(f"DROP TABLE {table} SYNC")


def test_replica_that_cannot_hardlink_copies(started_cluster):
    """`node2` can't hardlink files, so `node1` selects the `TTLClearIndex` merge and `node2` produces the same part by
    copying."""
    table = "ttl_clear_index_copy"
    create_table(node1, table, "r1")
    create_table(node2, table, "r2", settings=", always_use_copy_instead_of_hardlinks = 1")
    node1.query(f"INSERT INTO {table} VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2)")
    node2.query(f"SYSTEM SYNC REPLICA {table}")

    for node in (node1, node2):
        node.query(f"SYSTEM START TTL MERGES {table}")
    wait_cleared(table)

    result_part = node1.query(active_parts_query(table, "name")).strip()
    assert merge_events(node1, table, result_part) == "MergeParts\tTTLClearIndexMerge\n"
    assert merge_events(node2, table, result_part) == "MergeParts\tTTLClearIndexMerge\n"
    identity = active_parts_query(table, "name, hash_of_all_files, hash_of_uncompressed_files")
    assert node1.query(identity) == node2.query(identity)
    assert query_both(f"SELECT sum(v), count() FROM {table}") == ["3\t2\n"] * 2

    node2.query(f"DROP TABLE {table} SYNC")
    node1.query(f"DROP TABLE {table} SYNC")

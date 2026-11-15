import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Servers before the fix wrote `invalidated_system_columns.txt` with `_block_number`
# and `_block_offset` on ATTACH PARTITION and RESTORE into every part, including
# patch parts, where these columns are the payload. A patch part with such file
# returned a null `_block_number` column and crashed reads after every reload of
# the part. Verify that the part-loading path ignores the stale file for patch
# parts: plant the file into an active patch part and restart the server.
@pytest.mark.parametrize("version", ["v1", "v2"])
def test_stale_invalidated_columns_file(started_cluster, version):
    table = f"t_patch_stale_{version}"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"""
        CREATE TABLE {table} (id UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                 patch_parts_version = '{version}', min_bytes_for_full_part_storage = 0
        """)

    node.query(f"INSERT INTO {table} SELECT number, 0 FROM numbers(1000)")
    node.query(f"SYSTEM STOP MERGES {table}")
    node.query(
        f"UPDATE {table} SET v = 1 WHERE id < 400",
        settings={"enable_lightweight_update": 1},
    )

    patch_part_path = node.query(f"""
        SELECT any(path) FROM system.parts
        WHERE database = 'default' AND table = '{table}'
          AND active AND startsWith(partition_id, 'patch-')
        """).strip()

    assert patch_part_path

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '_block_number\\n_block_offset\\n' > {patch_part_path}invalidated_system_columns.txt",
        ]
    )

    node.restart_clickhouse()

    assert node.query(f"SELECT count(), countIf(v = 1) FROM {table}") == "1000\t400\n"

    node.query(f"SYSTEM START MERGES {table}")
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    assert node.query(f"SELECT count(), countIf(v = 1) FROM {table}") == "1000\t400\n"

    node.query(f"DROP TABLE {table} SYNC")

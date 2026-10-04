import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/decode_pool.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_nested_iceberg_read_with_single_pool_thread(started_cluster):
    """A pruning subquery reading another Iceberg table must not deadlock the decode
    pool: the sets are built on the producer thread before any pool task runs, so the
    single pool thread is never held by a task that waits for a nested read."""
    node.query(
        """
        CREATE TABLE ice_main (id Int64, part Int64)
        ENGINE = IcebergLocal(concat(getServerSetting('user_files_path'), '/ice_main/'))
        """
    )
    node.query(
        """
        CREATE TABLE ice_parts (part Int64)
        ENGINE = IcebergLocal(concat(getServerSetting('user_files_path'), '/ice_parts/'))
        """
    )
    # One commit per insert, so both tables have several manifests to decode.
    for part in range(8):
        node.query(
            f"INSERT INTO ice_main SELECT number + {part} * 10, {part} FROM numbers(10) "
            "SETTINGS allow_insert_into_iceberg = 1"
        )
    for part in [1, 3, 6]:
        node.query(
            f"INSERT INTO ice_parts SELECT {part} "
            "SETTINGS allow_insert_into_iceberg = 1"
        )

    result = node.query(
        "SELECT count() FROM ice_main WHERE part IN (SELECT part FROM ice_parts)",
        settings={
            "iceberg_manifest_decode_concurrency": 4,
            "use_iceberg_metadata_files_cache": 0,
        },
        timeout=120,
    )
    assert int(result.strip()) == 30

    node.query("DROP TABLE ice_main")
    node.query("DROP TABLE ice_parts")


def test_join_of_two_iceberg_tables_with_single_pool_thread(started_cluster):
    node.query(
        """
        CREATE TABLE ice_left (id Int64)
        ENGINE = IcebergLocal(concat(getServerSetting('user_files_path'), '/ice_left/'))
        """
    )
    node.query(
        """
        CREATE TABLE ice_right (id Int64)
        ENGINE = IcebergLocal(concat(getServerSetting('user_files_path'), '/ice_right/'))
        """
    )
    # One commit per insert, so each side has several data files to hand over.
    for i in range(8):
        node.query(
            f"INSERT INTO ice_left SELECT number + {i} * 10 FROM numbers(10) "
            "SETTINGS allow_insert_into_iceberg = 1"
        )
        node.query(
            f"INSERT INTO ice_right SELECT number + {i} * 10 FROM numbers(10) "
            "SETTINGS allow_insert_into_iceberg = 1"
        )

    result = node.query(
        "SELECT count() FROM ice_left AS l INNER JOIN ice_right AS r ON l.id = r.id",
        settings={
            "iceberg_manifest_decode_concurrency": 4,
            # The smallest queue: the second data file of a side already has to wait for the
            # query to consume the first one, which is what used to park a pool thread.
            "iceberg_file_entries_queue_size": 1,
            "use_iceberg_metadata_files_cache": 0,
        },
        timeout=120,
    )
    assert int(result.strip()) == 80

    node.query("DROP TABLE ice_left")
    node.query("DROP TABLE ice_right")

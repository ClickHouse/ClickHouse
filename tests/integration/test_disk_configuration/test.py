import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

TABLE_NAME = "test"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/storage_configuration.xml",
            ],
            with_zookeeper=True,
            stay_alive=True,
            with_minio=True,
        )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_merge_tree_disk_setting(start_cluster):
    node1 = cluster.instances["node1"]

    assert (
        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time"
        in node1.query_and_get_error(
            f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 'disk_local', storage_policy = 's3';
    """
        )
    )

    node1.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3';
    """
    )

    minio = cluster.minio_client
    count = len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))

    node1.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))) > count

    node1.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME}_2;
        CREATE TABLE {TABLE_NAME}_2 (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3';
    """
    )

    node1.query(f"INSERT INTO {TABLE_NAME}_2 SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}_2")) == 100

    assert "__s3" in node1.query(f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}'").strip()
    assert "__s3" in node1.query(f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}_2'").strip()

    node1.query("SYSTEM RELOAD CONFIG")
    assert not node1.contains_in_log("An error has occurred while reloading storage policies, storage policies were not applied")
    assert "['s3']" in node1.query("SELECT disks FROM system.storage_policies WHERE policy_name = '__s3'").strip()

    node1.restart_clickhouse()

    assert "_s3" in node1.query(f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}'").strip()
    assert "['s3']" in node1.query("SELECT disks FROM system.storage_policies WHERE policy_name = '__s3'").strip()
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

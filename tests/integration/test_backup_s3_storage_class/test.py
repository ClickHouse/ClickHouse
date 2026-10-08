import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_backup_s3_storage_class(started_cluster):
    node.query(
        """
            CREATE TABLE test_s3_storage_class
            (
                `id` UInt64,
                `value` String
            )
            ENGINE = MergeTree
            ORDER BY id;
        """,
    )
    node.query(
        """
            INSERT INTO test_s3_storage_class VALUES (1, 'a');
        """,
    )
    result = node.query(
        f"""
            BACKUP TABLE test_s3_storage_class TO S3('http://minio1:9001/root/data', 'minio', '{minio_secret_key}')
            SETTINGS s3_storage_class='STANDARD';
        """
    )

    minio = cluster.minio_client
    lst = list(minio.list_objects(cluster.minio_bucket, "data/.backup"))
    assert lst[0].storage_class == "STANDARD"

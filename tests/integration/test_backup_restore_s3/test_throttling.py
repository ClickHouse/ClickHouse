import pytest
from minio.deleteobjects import DeleteObject

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.s3_tools import list_s3_objects
from helpers.utility import random_string

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/throttling_disks.xml",
    ],
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


@pytest.mark.parametrize(
    "storage_policy", ["default", "s3_native_copy", "s3_no_native_copy"]
)
@pytest.mark.parametrize("allow_s3_native_copy", [0, 1])
def test_backup_scheduler_settings(
    started_cluster, storage_policy, allow_s3_native_copy
):

    minio = cluster.minio_client
    s3_objects = list_s3_objects(minio, cluster.minio_bucket, prefix="")
    assert (
        len(
            list(
                minio.remove_objects(
                    cluster.minio_bucket,
                    [DeleteObject(obj) for obj in s3_objects],
                )
            )
        )
        == 0
    )

    table_name = f"test_s3_storage_class_{storage_policy}_{allow_s3_native_copy}"

    node.query(
        f"CREATE OR REPLACE RESOURCE network_read (READ DISK {storage_policy}, WRITE DISK {storage_policy})"
    )
    node.query(f"CREATE OR REPLACE WORKLOAD backup SETTINGS max_io_requests = 10")

    query_id = f"{storage_policy}_{allow_s3_native_copy}_{random_string(10)}"
    node.query(
        f"""
            CREATE TABLE {table_name}
            (
                `id` UInt64,
                `value` String
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS storage_policy='{storage_policy}' SETTINGS workload='backup';
        """,
    )

    node.query(
        f"""
            INSERT INTO {table_name} SETTINGS workload='backup' VALUES (1, 'a');
        """,
    )

    result = node.query(
        f"""
            BACKUP TABLE {table_name} TO S3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/data', 'minio', '{minio_secret_key}')
            SETTINGS s3_storage_class='STANDARD', allow_s3_native_copy={allow_s3_native_copy} SETTINGS workload='backup';
        """,
        query_id=query_id,
    )

    lst = list(minio.list_objects(cluster.minio_bucket, "data/.backup"))
    assert lst[0].storage_class == "STANDARD"

    node.query(f"DROP TABLE {table_name} SYNC")

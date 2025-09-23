
import os
import logging

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import generate_random_string

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_s3_storage_class(started_cluster):
    node = started_cluster.instances["node"]
    table_name = f"test_s3_storage_class_{generate_random_string()}"
    bucket = started_cluster.minio_bucket

    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_name}"

    node.query(
        f"""
        CREATE TABLE {table_name} (a Int32, b Int32, c String) ENGINE = S3('{url}', access_key_id = 'minio', secret_access_key='ClickHouse_Minio_P@ssw0rd', format = 'Parquet', storage_class_name='STANDARD')
    """
    )

    node.query(f"INSERT INTO {table_name} VALUES (1, 2, '3')")

    assert node.query("SELECT b FROM {}".format(table_name)).strip() == "2"

    table_function_name = f"test_s3_storage_class_{generate_random_string()}_table_function"

    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_function_name}"

    table_function_def = f"s3('{url}', 'minio', 'ClickHouse_Minio_P@ssw0rd', '', 'Parquet', 'a Int32, b Int32, c String', 'zstd', 'WILDCARD', 0, 'STANDARD')"
    node.query(f"INSERT INTO TABLE FUNCTION {table_function_def} VALUES (1, 2, '3')")

    assert node.query("SELECT b FROM {}".format(table_function_def)).strip() == "2"

    with pytest.raises(Exception):
        node.query(
            f"""
        CREATE TABLE {table_name}_invalid (a Int32, b Int32, c String) ENGINE = S3('{url}', access_key_id = 'minio', secret_access_key='ClickHouse_Minio_P@ssw0rd', format = 'Parquet', storage_class_name='INVALID')
    """
        )

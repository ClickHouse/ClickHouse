import logging
import random
import string
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)


def gen_insert_values(size):
    return ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(size)
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node1",
        main_configs=["configs/storage_conf.xml"],
        with_minio=True,
        env_variables={"ENDPOINT_SUBPATH": "node1"},
        stay_alive=True,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/storage_conf.xml"],
        with_minio=True,
        env_variables={
            "ENDPOINT_SUBPATH": "node2",
            "ROTATED_REPLICA_ENDPOINT_SUBPATH": "node1",
        },
        stay_alive=True,
        instance_env_variables=True,
    )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test():
    node1 = cluster.instances["node1"]

    def create_insert(node, table_name, insert_values):
        node.query(
            f"""CREATE TABLE {table_name} (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            PARTITION BY id%10
            SETTINGS storage_policy='s3_plain_rewritable'
            """
        )

        node.query("INSERT INTO {} VALUES {}".format(table_name, insert_values))

    create_insert(node1, "test1", gen_insert_values(1000))

    assert int(node1.query("SELECT count(*) FROM test1")) == 1000

    uuid1 = node1.query("SELECT uuid FROM system.tables WHERE table='test1'").strip()

    node1.query("DETACH TABLE test1")
    node1.stop()

    node2 = cluster.instances["node2"]
    node2.query(
        f"""CREATE TABLE test2 (id Int64, data String)
        ENGINE=MergeTree()
        ORDER BY id
        PARTITION BY id%10
        SETTINGS disk=disk(
            name=custom_disk,
            type='s3_plain_rewritable',
            endpoint='http://minio1:9001/root/data/node1',
            access_key_id='minio',
            secret_access_key='ClickHouse_Minio_P@ssw0rd')
        """
    )

    node2.query(
        f"""ATTACH TABLE test_rotated_test1 UUID '{uuid1}' (id Int64, data String)
        ENGINE=MergeTree()
        ORDER BY id
        PARTITION BY id%10
        SETTINGS disk=disk(
            name=custom_disk,
            type='s3_plain_rewritable',
            endpoint='http://minio1:9001/root/data/node1',
            access_key_id='minio',
            secret_access_key='ClickHouse_Minio_P@ssw0rd')
        """
    )

    assert (
        int(
            node2.query(
                "SELECT count(*) FROM test_rotated_test1 WHERE _partition_id = '0'"
            )
        )
        == 100
    )

    node2.query(f"""ALTER TABLE test_rotated_test1 MOVE PARTITION '0' TO TABLE test2""")

    assert int(node2.query("SELECT count(*) FROM test_rotated_test1")) == 900
    assert int(node2.query("SELECT count(*) FROM test2")) == 100

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


def randomize_name(table_name, random_suffix_length=8):
    letters = string.ascii_letters
    return f"{table_name}_{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


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


def test_alter_partition_after_table_rotation():
    node1 = cluster.instances["node1"]

    def create_insert(node, table_name, insert_values):
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
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

    table1 = randomize_name("t1")
    create_insert(node1, table1, gen_insert_values(1000))

    assert int(node1.query(f"SELECT count(*) FROM {table1}")) == 1000

    uuid1 = node1.query(
        f"SELECT uuid FROM system.tables WHERE table='{table1}'"
    ).strip()

    node1.query(f"DETACH TABLE {table1} PERMANENTLY SYNC")

    disk_name = randomize_name("disk")
    node2 = cluster.instances["node2"]
    table2 = randomize_name("table2")
    node2.query(f"DROP TABLE IF EXISTS {table2} SYNC")
    node2.query(
        f"""CREATE TABLE {table2} (id Int64, data String)
        ENGINE=MergeTree()
        ORDER BY id
        PARTITION BY id%10
        SETTINGS disk=disk(
            name={disk_name},
            type='s3_plain_rewritable',
            endpoint='http://minio1:9001/root/data/node1',
            access_key_id='minio',
            secret_access_key='ClickHouse_Minio_P@ssw0rd')
        """
    )

    rotated_table = f"{table1}_rotated"
    node2.query(f"""DROP TABLE IF EXISTS {rotated_table} SYNC""")
    node2.query(
        f"""ATTACH TABLE {rotated_table} UUID '{uuid1}' (id Int64, data String)
        ENGINE=MergeTree()
        ORDER BY id
        PARTITION BY id%10
        SETTINGS disk=disk(
            name={disk_name},
            type='s3_plain_rewritable',
            endpoint='http://minio1:9001/root/data/node1',
            access_key_id='minio',
            secret_access_key='ClickHouse_Minio_P@ssw0rd')
        """
    )

    assert (
        int(
            node2.query(
                f"SELECT count(*) FROM {rotated_table} WHERE _partition_id = '0'"
            )
        )
        == 100
    )

    assert int(node2.query(f"SELECT count(*) FROM {rotated_table}")) == 1000

    node2.query(f"""ALTER TABLE {rotated_table} MOVE PARTITION '0' TO TABLE {table2}""")

    assert int(node2.query(f"SELECT count(*) FROM {rotated_table}")) == 900
    assert int(node2.query(f"SELECT count(*) FROM {table2}")) == 100

    node2.query(f"DROP TABLE {rotated_table} SYNC")
    node2.query(f"DROP TABLE {table2} SYNC")

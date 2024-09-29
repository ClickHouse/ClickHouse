import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

NODE1 = "node1"
NODE2 = "node2"
TABLE_NAME = "blob_storage_table"
CONTAINER_NAME = "cont"
CLUSTER_NAME = "test_cluster"

drop_table_statement = f"DROP TABLE {TABLE_NAME} ON CLUSTER {CLUSTER_NAME} SYNC"


def generate_cluster_def(port):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/storage_conf.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""<clickhouse>
    <storage_configuration>
        <disks>
            <blob_storage_disk>
                <type>azure_blob_storage</type>
                <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
                <container_name>cont</container_name>
                <skip_access_check>false</skip_access_check>
                <!-- default credentials for Azurite storage account -->
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
            </blob_storage_disk>
        </disks>
        <policies>
            <blob_storage_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_disk</disk>
                    </main>
                </volumes>
            </blob_storage_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""
        )
    return path


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_cluster_def(port)
        cluster.add_instance(
            NODE1,
            main_configs=[
                "configs/config.d/config.xml",
                path,
            ],
            macros={"replica": "1"},
            with_azurite=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            NODE2,
            main_configs=[
                "configs/config.d/config.xml",
                path,
            ],
            macros={"replica": "2"},
            with_azurite=True,
            with_zookeeper=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table_name, replica, **additional_settings):
    settings = {
        "storage_policy": "blob_storage_policy",
        "old_parts_lifetime": 1,
    }
    settings.update(additional_settings)

    create_table_statement = f"""
        CREATE TABLE {table_name} ON CLUSTER {CLUSTER_NAME} (
            id Int64,
            data String
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table_name}', '{{replica}}')
        ORDER BY id
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}"""

    azure_query(node, create_table_statement)
    assert node.query(f"SELECT COUNT(*) FROM {table_name} FORMAT Values") == "(0)"


def get_large_objects_count(blob_container_client, large_size_threshold=100):
    return sum(
        blob["size"] > large_size_threshold
        for blob in blob_container_client.list_blobs()
    )


def test_zero_copy_replication(cluster):
    node1 = cluster.instances[NODE1]
    node2 = cluster.instances[NODE2]
    create_table(node1, TABLE_NAME, 1)

    blob_container_client = cluster.blob_service_client.get_container_client(
        CONTAINER_NAME
    )

    values1 = "(0,'data'),(1,'data')"
    values2 = "(2,'data'),(3,'data')"

    azure_query(node1, f"INSERT INTO {TABLE_NAME} VALUES {values1}")
    node2.query(f"SYSTEM SYNC REPLICA {TABLE_NAME}")
    assert (
        azure_query(node1, f"SELECT * FROM {TABLE_NAME} order by id FORMAT Values")
        == values1
    )
    assert (
        azure_query(node2, f"SELECT * FROM {TABLE_NAME} order by id FORMAT Values")
        == values1
    )

    # Based on version 21.x - should be only one file with size 100+ (checksums.txt), used by both nodes
    assert get_large_objects_count(blob_container_client) == 1

    azure_query(node2, f"INSERT INTO {TABLE_NAME} VALUES {values2}")
    node1.query(f"SYSTEM SYNC REPLICA {TABLE_NAME}")

    assert (
        azure_query(node2, f"SELECT * FROM {TABLE_NAME} order by id FORMAT Values")
        == values1 + "," + values2
    )
    assert (
        azure_query(node1, f"SELECT * FROM {TABLE_NAME} order by id FORMAT Values")
        == values1 + "," + values2
    )

    assert get_large_objects_count(blob_container_client) == 2
    node1.query(drop_table_statement)

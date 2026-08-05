import logging
import os
import random
import string

import pytest
from azure.storage.blob import BlobServiceClient

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

NODE_NAME = "node"
OTHER_NODE = "other_node"


def generate_cluster_def(port, node_name):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/disk_storage_conf.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""<clickhouse>
    <storage_configuration>
        <disks>
            <blob_storage_disk>
                <type>object_storage</type>
                <object_storage_type>azure_blob_storage</object_storage_type>
                <metadata_type>plain_rewritable</metadata_type>
                <endpoint>http://azurite1:{port}/devstoreaccount1/cont</endpoint>
                <endpoint_subpath>{node_name}</endpoint_subpath>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                <max_single_part_upload_size>100000</max_single_part_upload_size>
                <min_upload_part_size>100000</min_upload_part_size>
                <max_single_download_retries>10</max_single_download_retries>
                <max_single_read_retries>10</max_single_read_retries>
            </blob_storage_disk>
            <blob_storage_cache_disk>
                <type>cache</type>
                <disk>blob_storage_disk</disk>
                <path>disks/blob_storage_cache/</path>
                <max_size>1000000000</max_size>
                <cache_on_write_operations>1</cache_on_write_operations>
            </blob_storage_cache_disk>
            <blob_storage_encrypted_disk>
                <type>encrypted</type>
                <disk>blob_storage_cache_disk</disk>
                <key>1234567812345678</key>
                <path></path>
            </blob_storage_encrypted_disk>
        </disks>
        <policies>
            <blob_storage_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_disk</disk>
                    </main>
                </volumes>
            </blob_storage_policy>
            <blob_storage_cache_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_cache_disk</disk>
                    </main>
                </volumes>
            </blob_storage_cache_policy>
            <blob_storage_encrypted_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_encrypted_disk</disk>
                    </main>
                </volumes>
            </blob_storage_encrypted_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""
        )
    return path


insert_values = [
    "(0,'data'),(1,'data')",
    ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(3000)
    ),
]


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_cluster_def(port, NODE_NAME)
        cluster.add_instance(
            NODE_NAME,
            main_configs=[
                path,
            ],
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            OTHER_NODE,
            with_azurite=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "storage_policy,min_bytes_for_wide_part",
    [
        pytest.param("blob_storage_policy", 0),
        pytest.param("blob_storage_cache_policy", 0),
        pytest.param("blob_storage_encrypted_policy", 0),
        pytest.param("blob_storage_policy", 1 << 20),
    ],
)
def test_insert_select(cluster, storage_policy, min_bytes_for_wide_part):
    node = cluster.instances[NODE_NAME]

    for index, value in enumerate(insert_values):
        azure_query(
            node,
            f"""
            CREATE TABLE test_{index} (
                id Int64,
                data String,
                empty String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy = '{storage_policy}',
            min_bytes_for_wide_part = {min_bytes_for_wide_part}
            """,
        )

        azure_query(
            node, "INSERT INTO test_{} (id, data) VALUES {}".format(index, value)
        )
        assert (
            azure_query(
                node,
                "SELECT id, data FROM test_{} ORDER BY id FORMAT Values".format(index),
            )
            == value
        )

        azure_query(node, "OPTIMIZE TABLE test_{} FINAL".format(index))

    for index, value in enumerate(insert_values):
        assert (
            azure_query(
                node,
                "SELECT id, data FROM test_{} ORDER BY id FORMAT Values".format(index),
            )
            == value
        )
    node.restart_clickhouse()

    for index, value in enumerate(insert_values):
        assert (
            azure_query(
                node,
                "SELECT id, data FROM test_{} ORDER BY id FORMAT Values".format(index),
            )
            == value
        )

        azure_query(node, "OPTIMIZE TABLE test_{} FINAL".format(index))

    other_node = cluster.instances[OTHER_NODE]
    port = cluster.env_variables["AZURITE_PORT"]

    azure_query(
        other_node,
        "SYSTEM ENABLE FAILPOINT plain_rewritable_object_storage_azure_not_found_on_init",
    )
    azure_query(
        other_node,
        """CREATE TABLE table (id Int64, data String) ENGINE=MergeTree() ORDER BY id SETTINGS disk=disk(
        type = object_storage,
        metadata_type = plain_rewritable,
        object_storage_type = azure_blob_storage,
        endpoint = 'http://azurite1:{port}/devstoreaccount1/cont',
        endpoint_subpath = '{node}',
        account_name = 'devstoreaccount1',
        account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')
        """.format(
            port=port,
            node=NODE_NAME,
        ),
    )
    azure_query(other_node, "DROP TABLE table SYNC")
    azure_query(
        other_node,
        "SYSTEM DISABLE FAILPOINT plain_rewritable_object_storage_azure_not_found_on_init",
    )

    for index, value in enumerate(insert_values):
        node.query("DROP TABLE IF EXISTS test_{} SYNC".format(index))

    port = cluster.env_variables["AZURITE_PORT"]
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    containers = blob_service_client.list_containers()
    for container in containers:
        container_client = blob_service_client.get_container_client(container)
        assert len(list(container_client.list_blobs())) == 0

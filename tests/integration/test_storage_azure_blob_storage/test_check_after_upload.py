import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from test_storage_azure_blob_storage.test import azure_query

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NODE_NAME = "node"
TABLE_NAME = "blob_storage_table"
AZURE_BLOB_STORAGE_DISK = "blob_storage_disk"
LOCAL_DISK = "hdd"
CONTAINER_NAME = "cont"


def generate_cluster_def(port):
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
                <type>azure_blob_storage</type>
                <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
                <container_name>cont</container_name>
                <skip_access_check>false</skip_access_check>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                <max_single_part_upload_size>100000</max_single_part_upload_size>
                <min_upload_part_size>100000</min_upload_part_size>
                <max_single_download_retries>10</max_single_download_retries>
                <max_single_read_retries>10</max_single_read_retries>
                <check_objects_after_upload>true</check_objects_after_upload>
            </blob_storage_disk>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <blob_storage_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_disk</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
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
            NODE_NAME,
            main_configs=[
                path,
            ],
            with_azurite=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


# Note: use azure_query for selects and inserts and create table queries.
# For inserts there is no guarantee that retries will not result in duplicates.
# But it is better to retry anyway because connection related errors
# happens in fact only for inserts because reads already have build-in retries in code.


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "blob_storage_policy",
        "old_parts_lifetime": 1,
        "index_granularity": 512,
        "temporary_directories_lifetime": 1,
    }
    settings.update(additional_settings)

    create_table_statement = f"""
        CREATE TABLE {table_name} (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}"""

    azure_query(node, f"DROP TABLE IF EXISTS {table_name}")
    azure_query(node, create_table_statement)
    assert (
        azure_query(node, f"SELECT COUNT(*) FROM {table_name} FORMAT Values") == "(0)"
    )


def test_simple(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    values = "('2021-11-13',3,'hello')"
    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values}")
    assert (
        azure_query(node, f"SELECT dt, id, data FROM {TABLE_NAME} FORMAT Values")
        == values
    )

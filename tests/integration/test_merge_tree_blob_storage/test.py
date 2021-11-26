import logging
import random
import string
import time
import threading
import os

import pytest
from helpers.cluster import ClickHouseCluster, get_instances_dir


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, './{}/node/configs/config.d/storage_conf.xml'.format(get_instances_dir()))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node",
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/bg_processing_pool_conf.xml"],
            with_azurite=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "blob_storage_policy",
        "index_granularity": 512
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

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(create_table_statement)
    assert node.query(f"SELECT COUNT(*) FROM {table_name}") == "0\n"
    node.query(f"INSERT INTO {table_name} VALUES ('2021-11-13', 3, 'hello')")


@pytest.mark.parametrize(
    "node_name",
    [
        ("node"),
    ]
)
def test_simple(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "blob_storage_test")
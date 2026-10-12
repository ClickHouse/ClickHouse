#!/usr/bin/env python3

import gzip
import io
import json
import logging
import os
import random
import threading
import time

import pytest
from azure.storage.blob import BlobServiceClient

import helpers.client
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.mock_servers import start_mock_servers
from helpers.network import PartitionManager
from helpers.test_tools import exec_query_with_retry
from helpers.database_disk import replace_text_in_metadata


RESOLVER_CONTAINER_NAME = "resolver"
RESOLVER_PORT = 8080


def generate_endpoint(port):
    read_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./azure_endpoint/endpoint.py",
    )
    with open(read_path, "r") as f:
        content = f.read()
    write_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/endpoint.py",
    )
    os.makedirs(os.path.dirname(write_path), exist_ok=True)
    with open(write_path, "w") as f:
        f.write(content.format(port=port))


def run_endpoint(cluster):
    logging.info("Starting custom Azure endpoint")
    script_dir = os.path.join(os.path.dirname(__file__), "_gen")
    start_mock_servers(
        cluster, script_dir, [("endpoint.py", RESOLVER_CONTAINER_NAME, RESOLVER_PORT)]
    )
    logging.info("Azure endpoint started")


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        generate_endpoint(port)
        cluster.add_instance(
            "node",
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
            main_configs=["configs/disable_async_loader.xml"],
        )
        cluster.start()

        run_endpoint(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def test_cluster_alive_after_restart(cluster):
    node = cluster.instances["node"]
    azurite_port = cluster.azurite_port
    node.query(
        f"""
        CREATE TABLE test_table (a UInt64, b String)
        ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:{azurite_port}/devstoreaccount1;', 'cont', 'test_table.csv', 'CSV')
        ORDER BY tuple()
        """
    )

    metadata_path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database='default' AND name='test_table'"
    ).strip()

    node.stop_clickhouse()
    replace_text_in_metadata(
        node,
        metadata_path,
        f"azurite1:{azurite_port}",
        f"{RESOLVER_CONTAINER_NAME}:{RESOLVER_PORT}",
    )
    node.start_clickhouse()
    node.query("DROP TABLE test_table")

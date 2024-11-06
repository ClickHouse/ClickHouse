import glob
import json
import logging
import os
import time
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.s3_tools import get_file_contents, list_s3_objects, prepare_s3_bucket

BASE_URL = "http://rest:8181/v1"
BASE_URL_LOCAL = "http://localhost:8181/v1"


def create_namespace(name):
    payload = {
        "namespace": [name],
        "properties": {"owner": "clickhouse", "description": "test namespace"},
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{BASE_URL_LOCAL}/namespaces", headers=headers, data=json.dumps(payload)
    )
    if response.status_code == 200:
        print(f"Namespace '{name}' created successfully.")
    else:
        raise Exception(
            f"Failed to create namespace. Status code: {response.status_code}, Response: {response.text}"
        )


def list_namespaces():
    response = requests.get(f"{BASE_URL_LOCAL}/namespaces")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list namespaces: {response.status_code}")


def create_table(name, namespace):
    payload = {
        "name": name,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "name", "type": "String", "required": True},
                {"id": 2, "name": "age", "type": "Int", "required": False},
            ],
        },
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{BASE_URL_LOCAL}/namespaces/{namespace}/tables",
        headers=headers,
        data=json.dumps(payload),
    )
    if response.status_code == 200:
        print(f"Table '{name}' created successfully.")
    else:
        raise Exception(
            f"Failed to create a table. Status code: {response.status_code}, Response: {response.text}"
        )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            with_minio=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        cluster.minio_client.make_bucket("warehouse")
        prepare_s3_bucket(cluster)

        yield cluster

    finally:
        cluster.shutdown()


def test_simple(started_cluster):
    # TODO: properly wait for container
    time.sleep(10)

    namespace = "kssenii.test.namespace"
    root_namespace = "kssenii"

    create_namespace(namespace)
    assert root_namespace in list_namespaces()["namespaces"][0][0]

    node = started_cluster.instances["node1"]
    node.query(
        f"""
CREATE DATABASE demo ENGINE = Iceberg('{BASE_URL}', 'minio', 'minio123')
SETTINGS catalog_type = 'rest', storage_endpoint = 'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/'
    """
    )

    table_name = "testtable"
    create_table(table_name, "kssenii")

    assert namespace in node.query("USE demo; SHOW TABLES")

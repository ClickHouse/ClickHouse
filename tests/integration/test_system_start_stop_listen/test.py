#!/usr/bin/env python3


import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import Client
import requests

cluster = ClickHouseCluster(__file__)
main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/cluster.xml", "configs/protocols.xml"],
    with_zookeeper=True,
)
backup_node = cluster.add_instance(
    "backup_node", main_configs=["configs/cluster.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def http_works(port=8123):
    try:
        response = requests.post(f"http://{main_node.ip_address}:{port}/ping")
        if response.status_code == 400:
            return True
    except:
        pass

    return False


def assert_everything_works():
    custom_client = Client(main_node.ip_address, 9001, command=cluster.client_bin_path)
    main_node.query(QUERY)
    main_node.query(MYSQL_QUERY)
    custom_client.query(QUERY)
    assert http_works()
    assert http_works(8124)


QUERY = "SELECT 1"
MYSQL_QUERY = "SELECT * FROM mysql('127.0.0.1:9004', 'system', 'one', 'default', '', SETTINGS connect_timeout = 100, connection_wait_timeout = 100)"


def test_default_protocols(started_cluster):
    # TCP
    assert_everything_works()
    main_node.query("SYSTEM STOP LISTEN TCP")
    assert "Connection refused" in main_node.query_and_get_error(QUERY)
    backup_node.query("SYSTEM START LISTEN ON CLUSTER default TCP")

    # HTTP
    assert_everything_works()
    main_node.query("SYSTEM STOP LISTEN HTTP")
    assert http_works() == False
    main_node.query("SYSTEM START LISTEN HTTP")

    # MySQL
    assert_everything_works()
    main_node.query("SYSTEM STOP LISTEN MYSQL")
    assert "Connections to mysql failed" in main_node.query_and_get_error(MYSQL_QUERY)
    main_node.query("SYSTEM START LISTEN MYSQL")

    assert_everything_works()


def test_custom_protocols(started_cluster):
    # TCP
    custom_client = Client(main_node.ip_address, 9001, command=cluster.client_bin_path)
    assert_everything_works()
    main_node.query("SYSTEM STOP LISTEN CUSTOM 'tcp'")
    assert "Connection refused" in custom_client.query_and_get_error(QUERY)
    main_node.query("SYSTEM START LISTEN CUSTOM 'tcp'")

    # HTTP
    assert_everything_works()
    main_node.query("SYSTEM STOP LISTEN CUSTOM 'http'")
    assert http_works(8124) == False
    main_node.query("SYSTEM START LISTEN CUSTOM 'http'")

    assert_everything_works()


def test_all_protocols(started_cluster):
    custom_client = Client(main_node.ip_address, 9001, command=cluster.client_bin_path)
    assert_everything_works()

    # STOP LISTEN QUERIES ALL
    main_node.query("SYSTEM STOP LISTEN QUERIES ALL")
    assert "Connection refused" in main_node.query_and_get_error(QUERY)
    assert "Connection refused" in custom_client.query_and_get_error(QUERY)
    assert http_works() == False
    assert http_works(8124) == False
    backup_node.query("SYSTEM START LISTEN ON CLUSTER default QUERIES ALL")

    # STOP LISTEN QUERIES DEFAULT
    assert_everything_works()

    main_node.query("SYSTEM STOP LISTEN QUERIES DEFAULT")
    assert "Connection refused" in main_node.query_and_get_error(QUERY)
    custom_client.query(QUERY)
    assert http_works() == False
    assert http_works(8124)
    backup_node.query("SYSTEM START LISTEN ON CLUSTER default QUERIES DEFAULT")

    # STOP LISTEN QUERIES CUSTOM
    assert_everything_works()

    main_node.query("SYSTEM STOP LISTEN QUERIES CUSTOM")
    main_node.query(QUERY)
    assert "Connection refused" in custom_client.query_and_get_error(QUERY)
    assert http_works()
    assert http_works(8124) == False
    backup_node.query("SYSTEM START LISTEN ON CLUSTER default QUERIES CUSTOM")

    # Disable all protocols, check first START LISTEN QUERIES DEFAULT then START LISTEN QUERIES CUSTOM
    assert_everything_works()

    main_node.query("SYSTEM STOP LISTEN QUERIES ALL")
    backup_node.query("SYSTEM START LISTEN ON CLUSTER default QUERIES DEFAULT")
    main_node.query(QUERY)
    assert "Connection refused" in custom_client.query_and_get_error(QUERY)
    assert http_works()
    assert http_works(8124) == False

    main_node.query("SYSTEM STOP LISTEN QUERIES ALL")
    backup_node.query("SYSTEM START LISTEN ON CLUSTER default QUERIES CUSTOM")
    assert "Connection refused" in main_node.query_and_get_error(QUERY)
    custom_client.query(QUERY)
    assert http_works() == False
    assert http_works(8124)

    backup_node.query("SYSTEM START LISTEN ON CLUSTER default QUERIES ALL")

    assert_everything_works()

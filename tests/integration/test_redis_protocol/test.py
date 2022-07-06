# -*- coding: utf-8 -*-

import pytest
from helpers.cluster import ClickHouseCluster
from redis import Redis

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/redis.xml"], user_configs=["configs/users.xml"]
)
node_ssl = cluster.add_instance(
    "node_ssl",
    main_configs=["configs/redis_ssl.xml", "configs/ssl_conf.xml"],
    user_configs=["configs/users.xml"],
)
server_port = 9006
ssl_server_port = 9007


@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip("node")
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def ssl_server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip("node_ssl")
    finally:
        cluster.shutdown()


def test_python_client(server_address):
    node.query(
        "CREATE TABLE test (key String, x UInt64, y String) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)"
    )

    redis = Redis(host=server_address, port=server_port, password="123")

    value = redis.select(2)
    assert value

    value = redis.get("key1")
    assert value is None

    node.query(
        "INSERT INTO test VALUES (\"key1\", 10, \"value\");"
    )

    value = redis.get("key1")
    assert value == "value"

    value = redis.hget("key1", "x")
    assert value == "10"

    values = redis.hmget("key1", "x", "y")
    assert values == ["10", "value"]


def test_python_client_ssl(ssl_server_address):
    node.query(
        "CREATE TABLE test (key String, x UInt64, y String) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)"
    )

    redis = Redis(
        host=ssl_server_address,
        port=ssl_server_port,
        ssl=True,
        ssl_cert_reqs=None,
        password="123",
    )

    value = redis.select(2)
    assert value

    value = redis.get("key1")
    assert value is None

    node.query(
        "INSERT INTO test VALUES (\"key1\", 10, \"value\");"
    )

    value = redis.get("key1")
    assert value == "value"

    value = redis.hget("key1", "x")
    assert value == "10"

    values = redis.hmget("key1", "x", "y")
    assert values == ["10", "value"]

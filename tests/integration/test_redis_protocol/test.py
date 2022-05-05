# -*- coding: utf-8 -*-

import pytest
from helpers.cluster import ClickHouseCluster
from redis import Redis

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/redis.xml"])
node_ssl = cluster.add_instance("node_ssl", main_configs=["configs/redis_ssl.xml", "configs/ssl_conf.xml"])
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
    redis = Redis(host=server_address, port=server_port, username="user", password="123")

    value = redis.select(2)
    assert value

    value = redis.get("key")
    assert value == b"Hello world"

    with pytest.raises(match=r".*Not implemented.*"):
        redis.set("key", "value")


def test_python_client_ssl(ssl_server_address):
    redis = Redis(host=ssl_server_address, port=ssl_server_port, ssl=True, ssl_cert_reqs=None)

    value = redis.get("key")
    assert value == b"Hello world"

    with pytest.raises(match=r".*Not implemented.*"):
        redis.set("key", "value")

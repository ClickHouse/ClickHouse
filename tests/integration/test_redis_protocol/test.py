# -*- coding: utf-8 -*-

import pytest
from helpers.cluster import ClickHouseCluster
from redis import Redis

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")
server_port = 9006


@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip("node")
    finally:
        cluster.shutdown()


def test_python_client(server_address):
    redis = Redis(host=server_address, port=server_port)

    value = redis.get("key")
    assert value == "Hello world"

    with pytest.raises(match=r".*Not implemented.*"):
        redis.set("key", "value")

#!/usr/bin/env python3

import os
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as keeper_utils

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def send_storage_request(
    node, method, path, data=None, params=None, expected_response_code=200
):
    url = "http://{host}:9182/api/v1/storage{path}".format(
        host=node.ip_address, path=path
    )
    response = requests.request(method, url, data=data, params=params)
    assert response.status_code == expected_response_code, (
        f"Expected {expected_response_code}, got {response.status_code}. "
        f"Response: {response.text}"
    )
    return response


def test_keeper_http_storage_create_get_exists(started_cluster):
    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    prefix = str(uuid.uuid4())

    test_content = b"test_data"
    send_storage_request(
        follower,
        "POST",
        f"/{prefix}test_storage_get",
        test_content,
        expected_response_code=201,
    )

    send_storage_request(follower, "HEAD", f"/{prefix}test_storage_get")
    response = send_storage_request(follower, "GET", f"/{prefix}test_storage_get")
    assert response.content == test_content

    send_storage_request(
        follower,
        "GET",
        f"/{prefix}test_storage_get/not_found",
        expected_response_code=404,
    )

    send_storage_request(
        follower,
        "HEAD",
        f"/{prefix}test_storage_get/not_found",
        expected_response_code=404,
    )


def test_keeper_http_storage_set(started_cluster):
    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    prefix = str(uuid.uuid4())

    send_storage_request(
        follower, "POST", f"/{prefix}test_storage_set", expected_response_code=201
    )

    response = send_storage_request(follower, "GET", f"/{prefix}test_storage_set")
    assert response.content == b""

    test_content = b"test_content"
    send_storage_request(
        follower,
        "PUT",
        f"/{prefix}test_storage_set",
        test_content,
        params={"version": 0},
    )

    response = send_storage_request(follower, "GET", f"/{prefix}test_storage_set")
    assert response.content == test_content

    send_storage_request(
        follower,
        "PUT",
        f"/{prefix}test_storage_set",
        test_content,
        expected_response_code=400,
    )

    send_storage_request(
        follower,
        "PUT",
        f"/{prefix}test_storage_set/not_found",
        test_content,
        params={"version": 0},
        expected_response_code=404,
    )


def test_keeper_http_storage_list_remove(started_cluster):
    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    prefix = str(uuid.uuid4())

    send_storage_request(
        follower, "POST", f"/{prefix}test_storage_list", expected_response_code=201
    )
    send_storage_request(
        follower, "POST", f"/{prefix}test_storage_list/a", expected_response_code=201
    )
    send_storage_request(
        follower, "POST", f"/{prefix}test_storage_list/b", expected_response_code=201
    )
    send_storage_request(
        follower, "POST", f"/{prefix}test_storage_list/c", expected_response_code=201
    )

    response = send_storage_request(
        follower, "GET", f"/{prefix}test_storage_list", params={"children": "true"}
    )
    assert sorted(response.json()["child_node_names"]) == ["a", "b", "c"]

    send_storage_request(
        follower,
        "DELETE",
        f"/{prefix}test_storage_list/b",
        params={"version": 0},
        expected_response_code=204,
    )

    response = send_storage_request(
        follower, "GET", f"/{prefix}test_storage_list", params={"children": "true"}
    )
    assert sorted(response.json()["child_node_names"]) == ["a", "c"]

    send_storage_request(
        follower, "DELETE", f"/{prefix}test_storage_list/a", expected_response_code=400
    )

    send_storage_request(
        follower,
        "GET",
        f"/{prefix}test_storage_list/not_found",
        params={"children": "true"},
        expected_response_code=404,
    )

#!/usr/bin/env python3

import http.client
import os
import socket
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


def send_chunked_storage_request(node, method, path, chunks):
    # Encoded up front and sent in one write: the whole request reaches the server before it
    # answers, even when it rejects the body without reading it to the end.
    body = (
        b"".join(b"%x\r\n%b\r\n" % (len(chunk), chunk) for chunk in chunks)
        + b"0\r\n\r\n"
    )
    connection = http.client.HTTPConnection(node.ip_address, 9182, timeout=30)
    try:
        connection.request(
            method,
            f"/api/v1/storage{path}",
            body=body,
            headers={"Transfer-Encoding": "chunked"},
        )
        return connection.getresponse().status
    finally:
        connection.close()


def send_unframed_storage_request(node, method, path, body):
    # Neither `Content-Length` nor chunked encoding: the body ends where the client shuts down
    # its side of the connection.
    with socket.create_connection((node.ip_address, 9182), timeout=30) as connection:
        connection.sendall(
            f"{method} /api/v1/storage{path} HTTP/1.1\r\nHost: {node.ip_address}\r\n\r\n".encode()
            + body
        )
        connection.shutdown(socket.SHUT_WR)
        with connection.makefile("rb") as reader:
            response = reader.read()
    return int(response.split(b" ", 2)[1])


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


def test_keeper_http_storage_max_request_size(started_cluster):
    # Only node3 sets `max_request_size`.
    max_request_size = 1024
    prefix = str(uuid.uuid4())
    at_limit = b"a" * max_request_size
    over_limit = b"b" * (max_request_size + 1)

    send_storage_request(
        node3, "POST", f"/{prefix}_over", over_limit, expected_response_code=413
    )
    send_storage_request(node3, "GET", f"/{prefix}_over", expected_response_code=404)

    send_storage_request(
        node3, "POST", f"/{prefix}_node", at_limit, expected_response_code=201
    )
    send_storage_request(
        node3,
        "PUT",
        f"/{prefix}_node",
        over_limit,
        params={"version": 0},
        expected_response_code=413,
    )
    response = send_storage_request(
        node3, "GET", f"/{prefix}_node", params={"children": "true"}
    )
    assert response.json()["stat"]["version"] == 0
    assert send_storage_request(node3, "GET", f"/{prefix}_node").content == at_limit

    send_storage_request(
        node3, "PUT", f"/{prefix}_node", at_limit.upper(), params={"version": 0}
    )
    assert (
        send_storage_request(node3, "GET", f"/{prefix}_node").content
        == at_limit.upper()
    )

    assert (
        send_chunked_storage_request(
            node3, "POST", f"/{prefix}_chunked_over", [b"d" * max_request_size, b"d"]
        )
        == 413
    )
    send_storage_request(
        node3, "GET", f"/{prefix}_chunked_over", expected_response_code=404
    )
    assert (
        send_chunked_storage_request(
            node3, "POST", f"/{prefix}_chunked", [b"c" * 1000, b"c" * 24]
        )
        == 201
    )
    assert (
        send_storage_request(node3, "GET", f"/{prefix}_chunked").content
        == b"c" * max_request_size
    )

    assert (
        send_unframed_storage_request(
            node3, "POST", f"/{prefix}_unframed_over", over_limit
        )
        == 413
    )
    send_storage_request(
        node3, "GET", f"/{prefix}_unframed_over", expected_response_code=404
    )
    assert (
        send_unframed_storage_request(node3, "POST", f"/{prefix}_unframed", at_limit)
        == 201
    )
    assert send_storage_request(node3, "GET", f"/{prefix}_unframed").content == at_limit

    send_storage_request(node3, "POST", f"/{prefix}_empty", expected_response_code=201)
    assert send_storage_request(node3, "GET", f"/{prefix}_empty").content == b""

    send_storage_request(
        node1, "POST", f"/{prefix}_unlimited", over_limit, expected_response_code=201
    )
    assert (
        send_storage_request(node1, "GET", f"/{prefix}_unlimited").content == over_limit
    )

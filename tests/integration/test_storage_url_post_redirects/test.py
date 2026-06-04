import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("node")

REDIRECT_PORT = 8081
TARGET_PORT = 8001
REDIRECT_307_PORT = 8082
REDIRECT_308_PORT = 8083
REDIRECT_301_PORT = 8084
REDIRECT_303_PORT = 8085
REDIRECT_NO_LOCATION_PORT = 8086


def _start_server(container_id, file_name, args):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    cluster.copy_file_to_container(
        container_id,
        os.path.join(script_dir, file_name),
        f"/{file_name}",
    )
    cmd_args = " ".join(str(x) for x in args)
    cluster.exec_in_container(
        container_id,
        ["bash", "-c", f"python3 /{file_name} {cmd_args} > {file_name}.log 2>&1"],
        detach=True,
        user="root",
    )
    host, port = args[0], args[1]

    def check():
        return cluster.exec_in_container(
            container_id,
            ["curl", "-s", f"http://{host}:{port}/"],
            nothrow=True,
        )

    wait_condition(
        check,
        lambda response: '{"status":"ok"}' in response,
        max_attempts=20,
        delay=0.5,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        container_id = cluster.get_container_id("node")
        # Target server (where the redirect would point if followed).
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", TARGET_PORT, "localhost", TARGET_PORT],
        )
        # Redirect servers which accept the body and respond with the
        # body-consuming statuses 301/302/303 (all accepted with the setting).
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", REDIRECT_301_PORT, "localhost", TARGET_PORT, 301],
        )
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", REDIRECT_PORT, "localhost", TARGET_PORT, 302],
        )
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", REDIRECT_303_PORT, "localhost", TARGET_PORT, 303],
        )
        # Redirect servers which respond with the method-preserving 307 and 308.
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", REDIRECT_307_PORT, "localhost", TARGET_PORT, 307],
        )
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", REDIRECT_308_PORT, "localhost", TARGET_PORT, 308],
        )
        # Redirect server which responds with a 302 but omits the Location header.
        _start_server(
            container_id,
            "post_redirect_server.py",
            ["localhost", REDIRECT_NO_LOCATION_PORT, "localhost", TARGET_PORT, 302, "no-location"],
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_post_redirect_rejected_by_default(started_cluster):
    query = (
        f"INSERT INTO TABLE FUNCTION "
        f"url('http://localhost:{REDIRECT_PORT}/insert', JSONEachRow, 'a UInt64') "
        f"SELECT 1"
    )
    with pytest.raises(QueryRuntimeException) as exc_info:
        server.query(query)
    assert "302" in str(exc_info.value) or "Moved Temporarily" in str(exc_info.value)


@pytest.mark.parametrize(
    "status, port",
    [
        pytest.param(301, REDIRECT_301_PORT, id="301"),
        pytest.param(302, REDIRECT_PORT, id="302"),
        pytest.param(303, REDIRECT_303_PORT, id="303"),
    ],
)
def test_post_redirect_accepted_with_setting(started_cluster, status, port):
    # 301/302/303 are body-consuming redirects: the server received the streamed
    # body and replies with a Location pointing at the canonical/result URL.
    # With the setting enabled, the write must be reported as successful.
    query = (
        f"INSERT INTO TABLE FUNCTION "
        f"url('http://localhost:{port}/insert', JSONEachRow, 'a UInt64') "
        f"SELECT 1 SETTINGS http_allow_redirects_on_post = 1"
    )
    server.query(query)


def test_post_redirect_without_location_rejected_with_setting(started_cluster):
    # A 3xx response without a Location header is not a real redirect acknowledgment
    # (it could be a proxy/auth/error page), so it must not be accepted as a
    # committed write even when http_allow_redirects_on_post = 1.
    query = (
        f"INSERT INTO TABLE FUNCTION "
        f"url('http://localhost:{REDIRECT_NO_LOCATION_PORT}/insert', JSONEachRow, 'a UInt64') "
        f"SELECT 1 SETTINGS http_allow_redirects_on_post = 1"
    )
    with pytest.raises(QueryRuntimeException) as exc_info:
        server.query(query)
    assert "without a Location header" in str(exc_info.value)


@pytest.mark.parametrize(
    "status, port",
    [
        pytest.param(307, REDIRECT_307_PORT, id="307"),
        pytest.param(308, REDIRECT_308_PORT, id="308"),
    ],
)
def test_method_preserving_redirect_rejected_with_setting(started_cluster, status, port):
    # 307 and 308 are method-preserving redirects: they ask the client to
    # replay the request body at the new URL. Since the body was streamed and
    # cannot be replayed, ClickHouse must surface this as an error rather than
    # silently report success, even when http_allow_redirects_on_post = 1.
    query = (
        f"INSERT INTO TABLE FUNCTION "
        f"url('http://localhost:{port}/insert', JSONEachRow, 'a UInt64') "
        f"SELECT 1 SETTINGS http_allow_redirects_on_post = 1"
    )
    with pytest.raises(QueryRuntimeException) as exc_info:
        server.query(query)
    assert str(status) in str(exc_info.value)
    assert "method-preserving redirect" in str(exc_info.value)

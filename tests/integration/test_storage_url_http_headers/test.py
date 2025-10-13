import os

import pytest

from helpers.cluster import ClickHouseCluster

from . import http_headers_echo_server, redirect_server

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("node")


def run_server(container_id, file_name, hostname, port, *args):
    script_dir = os.path.dirname(os.path.realpath(__file__))

    cluster.copy_file_to_container(
        container_id,
        os.path.join(script_dir, file_name),
        f"/{file_name}",
    )

    cmd_args = [hostname, port] + list(args)
    cmd_args_val = " ".join([str(x) for x in cmd_args])

    cluster.exec_in_container(
        container_id,
        [
            "bash",
            "-c",
            f"python3 /{file_name} {cmd_args_val} > {file_name}.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    for _ in range(0, 10):
        ping_response = cluster.exec_in_container(
            container_id,
            ["curl", "-s", f"http://{hostname}:{port}/"],
            nothrow=True,
        )

        if '{"status":"ok"}' in ping_response:
            return

        print(ping_response)

    raise Exception("Echo server is not responding")


def run_echo_server():
    container_id = cluster.get_container_id("node")
    run_server(container_id, "http_headers_echo_server.py", "localhost", 8000)


def run_redirect_server():
    container_id = cluster.get_container_id("node")
    run_server(container_id, "redirect_server.py", "localhost", 8080, "localhost", 8000)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_redirect_server()
        run_echo_server()

        yield cluster
    finally:
        cluster.shutdown()


def test_storage_url_http_headers(started_cluster):
    query = "INSERT INTO TABLE FUNCTION url('http://localhost:8000/', JSON, 'a UInt64', headers('X-My-Custom-Header'='test-header')) VALUES (1)"

    server.query(query)

    result = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )

    print(result)

    assert "X-My-Custom-Header: test-header" in result


def test_storage_url_redirected_headers(started_cluster):
    query = """
        SELECT
            title::String as title,
            theme::String as theme
        FROM
            url('http://127.0.0.1:8080/sample-data', 'JSONEachRow', 'title String, theme String')
        SETTINGS http_max_tries=2, max_http_get_redirects=2
        """

    result = server.query(query)
    assert 2 == len(result.strip().split("\n"))

    result_redirect = server.exec_in_container(
        ["cat", redirect_server.RESULT_PATH], user="root"
    )

    print(result_redirect)

    assert "Host: 127.0.0.1" in result_redirect
    assert "Host: localhost" not in result_redirect

    result = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )

    print(result)

    assert "Host: 127.0.0.1" not in result
    assert "Host: localhost" in result


def test_with_override_content_type_url_http_headers(started_cluster):
    query = "INSERT INTO TABLE FUNCTION url('http://localhost:8000/', JSONEachRow, 'x UInt8') SELECT 1"

    server.query(query)

    result = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )

    print(result)

    assert "Content-Type: application/x-ndjson; charset=UTF-8" in result

    query = "INSERT INTO TABLE FUNCTION url('http://localhost:8000/', JSONEachRow, 'x UInt8', headers('Content-Type' = 'upyachka')) SELECT 1"

    server.query(query)

    result = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )

    print(result)

    assert "Content-Type: application/x-ndjson; charset=UTF-8" not in result
    assert "Content-Type: upyachka" in result

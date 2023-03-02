import pytest
import os
import time

from . import http_headers_echo_server

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_storage_url_http_headers(started_cluster):
    script_dir = os.path.dirname(os.path.realpath(__file__))

    server.copy_file_to_container(
        os.path.join(script_dir, "http_headers_echo_server.py"),
        "/http_headers_echo_server.py",
    )
    server.exec_in_container(
        ["bash", "-c", "python3 /http_headers_echo_server.py > /http_headers_echo.server.log 2>&1"],
        detach=True,
        user="root",
    )

    query = "INSERT INTO TABLE FUNCTION url('http://localhost:8000/', JSON, 'a UInt64', headers('X-My-Custom-Header'='test-header')) VALUES (1)"
    
    server.query(query)

    result = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )

    print(result)

    assert 'X-My-Custom-Header: test-header' in result


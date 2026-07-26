import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

from . import http_dictionary_server

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/named_collections.xml"])


def run_server():
    container_id = cluster.get_container_id("node")
    script_dir = os.path.dirname(os.path.realpath(__file__))
    file_name = "http_dictionary_server.py"

    cluster.copy_file_to_container(
        container_id,
        os.path.join(script_dir, file_name),
        f"/{file_name}",
    )

    cluster.exec_in_container(
        container_id,
        [
            "bash",
            "-c",
            f"python3 /{file_name} localhost 8000 > {file_name}.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def check_server():
        return cluster.exec_in_container(
            container_id,
            ["curl", "-s", "http://localhost:8000/"],
            nothrow=True,
        )

    wait_condition(
        check_server,
        lambda response: '{"status":"ok"}' in response,
        max_attempts=20,
        delay=0.5,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_server()
        node.query("CREATE DATABASE IF NOT EXISTS test")

        yield cluster
    finally:
        cluster.shutdown()


def test_http_dictionary_with_headers_in_named_collection(started_cluster):
    node.query(
        """
        CREATE DICTIONARY test.http_dictionary_with_headers (
            id UInt64,
            value String
        )
        PRIMARY KEY id
        LAYOUT(FLAT())
        SOURCE(HTTP(NAME http_dictionary_collection))
        LIFETIME(MIN 0 MAX 0)
        """
    )

    assert (
        node.query(
            "SELECT dictGetString('test.http_dictionary_with_headers', 'value', toUInt64(1))"
        )
        == "first\n"
    )
    assert (
        node.query(
            "SELECT dictGetString('test.http_dictionary_with_headers', 'value', toUInt64(2))"
        )
        == "second\n"
    )

    headers = node.exec_in_container(
        ["cat", http_dictionary_server.RESULT_PATH], user="root"
    )
    assert "X-First-Header: first-value" in headers
    assert "X-Second-Header: second-value" in headers

    node.query("DROP DICTIONARY test.http_dictionary_with_headers")

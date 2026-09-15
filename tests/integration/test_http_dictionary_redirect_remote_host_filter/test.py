import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

# Regression test for the `remote_url_allow_hosts` bypass in HTTPDictionarySource: a
# DDL-created HTTP dictionary whose allow-listed source URL 302-redirects to a host that is
# NOT in <remote_url_allow_hosts>. Without threading RemoteHostFilter into the request
# builders, ClickHouse follows the redirect (and replays credentials) to the disallowed
# host; with the fix the redirect target is rejected with UNACCEPTABLE_URL.

SERVER_PORT = 8000

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/remote_hosts.xml"])


def run_server():
    container_id = cluster.get_container_id("node")
    script_dir = os.path.dirname(os.path.realpath(__file__))
    file_name = "redirect_server.py"

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
            f"python3 /{file_name} 0.0.0.0 {SERVER_PORT} > {file_name}.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def check_server():
        return cluster.exec_in_container(
            container_id,
            ["curl", "-s", f"http://127.0.0.1:{SERVER_PORT}/"],
            nothrow=True,
        )

    wait_condition(
        check_server,
        lambda response: '{"status":"ok"}' in response,
        max_attempts=20,
        delay=0.5,
    )


def followed():
    return cluster.exec_in_container(
        cluster.get_container_id("node"),
        ["curl", "-s", f"http://127.0.0.1:{SERVER_PORT}/followed"],
        nothrow=True,
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


def test_http_dictionary_redirect_target_is_host_filtered(started_cluster):
    # The dictionary source (`127.0.0.1:8000`) is allow-listed, but it 302-redirects to
    # `127.0.0.2:8000`, which is NOT allow-listed. `max_http_get_redirects = 5` enables
    # following redirects, so this exercises the redirect hop rather than the initial URL.
    node.query(f"""
        CREATE DICTIONARY test.redirect_http_dictionary (
            id UInt64,
            value String
        )
        PRIMARY KEY id
        LAYOUT(FLAT())
        SOURCE(HTTP(URL 'http://127.0.0.1:{SERVER_PORT}/redirect' FORMAT TabSeparated))
        LIFETIME(MIN 0 MAX 0)
        SETTINGS(max_http_get_redirects = 5)
        """)

    error = node.query_and_get_error(
        "SELECT dictGetString('test.redirect_http_dictionary', 'value', toUInt64(1))"
    )
    assert "is not allowed in configuration file" in error, (
        "expected RemoteHostFilter to reject the redirect target, got: " + error
    )

    # The server must not have served the disallowed target at all.
    assert "NO" in followed(), (
        "ClickHouse followed the 302 to a disallowed host "
        "(remote_url_allow_hosts bypass / SSRF)"
    )

    node.query("DROP DICTIONARY test.redirect_http_dictionary")

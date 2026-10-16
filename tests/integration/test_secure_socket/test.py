import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NODES = {"node" + str(i): None for i in (1, 2)}

config = """<clickhouse>
    <profiles>
        <default>
            <sleep_in_send_data_ms>{sleep_in_send_data_ms}</sleep_in_send_data_ms>
        </default>
    </profiles>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    cluster.__with_ssl_config = True
    main_configs = [
        "configs_secure/config.d/remote_servers.xml",
        "configs_secure/server.crt",
        "configs_secure/server.key",
        "configs_secure/dhparam.pem",
        "configs_secure/config.d/ssl_conf.xml",
    ]

    NODES["node1"] = cluster.add_instance("node1", main_configs=main_configs)
    NODES["node2"] = cluster.add_instance(
        "node2",
        main_configs=main_configs,
        user_configs=["configs_secure/users.d/users.xml"],
    )

    try:
        cluster.start()
        NODES["node2"].query(
            "CREATE TABLE base_table (x UInt64) ENGINE = MergeTree  ORDER BY x;"
        )
        NODES["node2"].query("INSERT INTO base_table VALUES (5);")
        NODES["node1"].query(
            "CREATE TABLE distributed_table (x UInt64) ENGINE = Distributed(test_cluster, default, base_table);"
        )

        yield cluster

    finally:
        cluster.shutdown()


def assert_socket_receive_timeout(error, expected_origin):
    # receive_timeout=5 must surface as SOCKET_TIMEOUT reporting the 5000 ms it asked for.
    # send_timeout stays distinct from it, or this value is ambiguous between the two settings,
    # and stays below it, since a blocking secure read waits max(receive_timeout, send_timeout).
    assert "Timeout exceeded while reading from socket" in error
    assert "5000 ms" in error
    assert "(SOCKET_TIMEOUT)" in error
    # Each combination formats the socket description differently, so an ignored
    # async_socket_for_remote or use_hedged_requests would show up as another arm's wording.
    assert expected_origin in error, f"expected {expected_origin} in: {error}"


def test(started_cluster):
    NODES["node2"].replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        config.format(sleep_in_send_data_ms=1000000),
    )

    if NODES["node1"].is_built_with_thread_sanitizer():
        pytest.skip("Hedged requests don't work under Thread Sanitizer")

    attempts = 0
    while attempts < 1000:
        setting = NODES["node2"].http_query(
            "SELECT value FROM system.settings WHERE name='sleep_in_send_data_ms'"
        )
        if int(setting) == 1000000:
            break
        time.sleep(0.1)
        attempts += 1

    assert attempts < 1000

    error = NODES["node1"].query_and_get_error(
        "SELECT * FROM distributed_table settings receive_timeout=5, send_timeout=4, use_hedged_requests=0, async_socket_for_remote=0;"
    )

    assert_socket_receive_timeout(error, "socket (peer: ")

    error = NODES["node1"].query_and_get_error(
        "SELECT * FROM distributed_table settings receive_timeout=5, send_timeout=4, use_hedged_requests=0, async_socket_for_remote=1;"
    )

    assert_socket_receive_timeout(error, "socket (socket (")

    error = NODES["node1"].query_and_get_error(
        "SELECT * FROM distributed_table settings receive_timeout=5, send_timeout=4, use_hedged_requests=1, async_socket_for_remote=1;"
    )

    assert_socket_receive_timeout(error, "socket (node2:9440, receive timeout")

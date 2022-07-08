import os.path
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

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


def test(started_cluster):
    NODES["node2"].replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        config.format(sleep_in_send_data_ms=1000000),
    )

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

    start = time.time()
    NODES["node1"].query_and_get_error(
        "SELECT * FROM distributed_table settings receive_timeout=5, send_timeout=5, use_hedged_requests=0, async_socket_for_remote=0;"
    )
    end = time.time()
    assert end - start < 10

    start = time.time()
    error = NODES["node1"].query_and_get_error(
        "SELECT * FROM distributed_table settings receive_timeout=5, send_timeout=5, use_hedged_requests=0, async_socket_for_remote=1;"
    )
    end = time.time()

    assert end - start < 10

    # Check that exception about timeout wasn't thrown from DB::ReadBufferFromPocoSocket::nextImpl().
    assert error.find("DB::ReadBufferFromPocoSocket::nextImpl()") == -1

    start = time.time()
    error = NODES["node1"].query_and_get_error(
        "SELECT * FROM distributed_table settings receive_timeout=5, send_timeout=5, use_hedged_requests=1, async_socket_for_remote=1;"
    )
    end = time.time()

    assert end - start < 10

    # Check that exception about timeout wasn't thrown from DB::ReadBufferFromPocoSocket::nextImpl().
    assert error.find("DB::ReadBufferFromPocoSocket::nextImpl()") == -1

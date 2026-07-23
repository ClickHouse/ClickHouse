#!/usr/bin/env python3

import os
import socket
import time

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

# Keeper and server from the current build.
keeper = cluster.add_instance(
    "keeper",
    main_configs=["configs/enable_keeper.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)

# Server of the version from the original report. Its `zookeeper` config
# contains a second host `keeper:9998` - a mock which responds to the `srvr`
# four letter command with `Zxid` above 2^31 - 1, as on a Keeper which has
# committed more than 2^31 - 1 transactions.
node = cluster.add_instance(
    "node",
    main_configs=["configs/use_keeper.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.2",
    with_installed_binary=True,
    with_remote_database_disk=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        keeper.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "mock_keeper_4lw.py"), "/mock_keeper_4lw.py"
        )
        keeper.exec_in_container(["python3", "/mock_keeper_4lw.py"], detach=True)

        mock_address = (cluster.get_instance_ip("keeper"), 9998)
        for _ in range(100):
            try:
                socket.create_connection(mock_address, timeout=1).close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise Exception("Mock four letter command endpoint did not start")

        yield cluster
    finally:
        cluster.shutdown()


def test_zookeeper_info_number_overflow(started_cluster):
    # The current version returns values above 2^31 - 1 correctly, and turns
    # the -1 reported for an unknown file descriptor count into NULL.
    assert (
        keeper.query(
            "SELECT zxid, node_count, packets_received, open_file_descriptor_count,"
            " max_file_descriptor_count FROM system.zookeeper_info WHERE port = 9998"
        )
        == "2147483648\t5\t3000000000\t100\t\\N\n"
    )

    # 26.2 parsed `Zxid` from the `srvr` four letter command response into
    # 32-bit `int`, so any zxid above 2^31 - 1 made the query fail with
    # `CANNOT_PARSE_NUMBER`.
    error = node.query_and_get_error("SELECT version FROM system.zookeeper_info")
    assert "CANNOT_PARSE_NUMBER" in error
    assert "Overflow while parsing a number" in error

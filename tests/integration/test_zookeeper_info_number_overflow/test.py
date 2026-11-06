#!/usr/bin/env python3

import os
import socket
import time

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

# Keeper and server from the current build. The `zookeeper` config contains a
# second host `keeper:9998` - a mock which responds to the four letter commands
# with values above 2^31 - 1, as on a Keeper which has committed more than
# 2^31 - 1 transactions.
keeper = cluster.add_instance(
    "keeper",
    main_configs=["configs/enable_keeper.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)

# Servers whose second ZooKeeper host is a mock which reports a `Zxid` without
# the 0x prefix: `5` on `keeper:9997` and `123` on `keeper:9996`.
node_short_zxid = cluster.add_instance(
    "node_short_zxid",
    main_configs=["configs/use_keeper_short_zxid.xml"],
)
node_long_zxid = cluster.add_instance(
    "node_long_zxid",
    main_configs=["configs/use_keeper_long_zxid.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        keeper.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "mock_keeper_4lw.py"), "/mock_keeper_4lw.py"
        )
        keeper.exec_in_container(["python3", "/mock_keeper_4lw.py"], detach=True)
        keeper.exec_in_container(
            ["python3", "/mock_keeper_4lw.py", "9997", "5"], detach=True
        )
        keeper.exec_in_container(
            ["python3", "/mock_keeper_4lw.py", "9996", "123"], detach=True
        )

        for port in (9998, 9997, 9996):
            mock_address = (cluster.get_instance_ip("keeper"), port)
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
    # The current version returns values above 2^31 - 1 correctly, turns
    # the -1 reported for an unknown file descriptor count into NULL, and
    # accepts 2^64 - 1 (RLIM_INFINITY, an unlimited file descriptor limit).
    assert (
        keeper.query(
            "SELECT zxid, node_count, packets_received, open_file_descriptor_count,"
            " max_file_descriptor_count, snapshot_dir_size, log_dir_size,"
            " last_log_idx, last_committed_idx"
            " FROM system.zookeeper_info WHERE port = 9998"
        )
        == "2147483648\t5\t3000000000\t\\N\t18446744073709551615\t3000000000\t4000000000"
        "\t5000000000\t5000000000\n"
    )


def test_zookeeper_info_file_descriptor_counts(started_cluster):
    # The same values from the real Keeper of this build: `mntr` prints the file
    # descriptor counts as unsigned numbers, and the textual -1 it reports for an
    # undetermined value must not wrap around to 2^64 - 1 (which is a valid value
    # on its own: an unlimited RLIMIT_NOFILE).
    open_fd, max_fd = (
        keeper.query(
            "SELECT open_file_descriptor_count, max_file_descriptor_count"
            " FROM system.zookeeper_info WHERE port = 9181"
        )
        .strip()
        .split("\t")
    )

    assert open_fd != "\\N" and 0 < int(open_fd) < 2**31
    assert max_fd != "\\N" and int(max_fd) >= int(open_fd)


def test_keeper_asynchronous_metrics_file_descriptor_counts(started_cluster):
    # The same contract on the sibling surface: `system.asynchronous_metrics` must
    # report an undetermined file descriptor count as -1, never as 2^64 - 1.
    keeper.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    open_fd, max_fd = (
        keeper.query(
            "SELECT"
            " maxIf(value, metric = 'KeeperOpenFileDescriptorCount'),"
            " maxIf(value, metric = 'KeeperMaxFileDescriptorCount')"
            " FROM system.asynchronous_metrics"
            " WHERE metric IN ('KeeperOpenFileDescriptorCount', 'KeeperMaxFileDescriptorCount')"
        )
        .strip()
        .split("\t")
    )

    assert 0 < float(open_fd) < 2**31
    assert float(max_fd) == -1 or float(max_fd) >= float(open_fd)


@pytest.mark.parametrize(
    "instance_name, zxid", [("node_short_zxid", "5"), ("node_long_zxid", "123")]
)
def test_zookeeper_info_zxid_without_prefix(started_cluster, instance_name, zxid):
    # A `Zxid` without the 0x prefix fails the query with a proper error, whether
    # it is shorter than the prefix or not.
    node = cluster.instances[instance_name]
    node.query_with_retry("SELECT count() FROM system.zookeeper WHERE path = '/'")
    error = node.query_and_get_error("SELECT zxid FROM system.zookeeper_info")
    assert "CANNOT_PARSE_NUMBER" in error and f"Cannot parse Zxid '{zxid}'" in error

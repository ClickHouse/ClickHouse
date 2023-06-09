#!/usr/bin/env python3

import pytest
import os
from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as keeper_utils
from kazoo.client import KazooClient, KazooState

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def restart_clickhouse(api_version=None, expect_fail=True):
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/enable_keeper.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )

    if api_version:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper.xml",
            "<!-- API VERSION -->",
            f"<api_version>{api_version}<\\/api_version>",
        )

    node.start_clickhouse(retry_start=not expect_fail)
    keeper_utils.wait_until_connected(cluster, node)


def test_keeper_api_version(started_cluster):
    restart_clickhouse()

    def assert_version(string_version, version_number):
        node.wait_for_log_line(
            f"Detected server's API version: {string_version}", look_behind_lines=1000
        )

        try:
            node_zk = get_connection_zk(node.name)
            assert node_zk.get("/keeper/api_version")[0] == str(version_number).encode()
        finally:
            if node_zk:
                node_zk.stop()
                node_zk.close()

    assert_version("WITH_CHECK_NOT_EXISTS", 3)

    for i, version in enumerate(
        [
            "ZOOKEEPER_COMPATIBLE",
            "WITH_FILTERED_LIST",
            "WITH_MULTI_READ",
            "WITH_CHECK_NOT_EXISTS",
        ]
    ):
        restart_clickhouse(version)
        assert_version(version, i)

    with pytest.raises(Exception):
        restart_clickhouse("INVALID_VERSION", expect_fail=True)

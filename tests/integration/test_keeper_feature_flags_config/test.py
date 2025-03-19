#!/usr/bin/env python3

import os

import pytest
from kazoo.client import KazooClient, KazooState

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

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


def restart_clickhouse(feature_flags=[], expect_fail=True):
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/enable_keeper.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )

    if len(feature_flags) > 0:
        feature_flags_config = "<feature_flags>"

        for feature, is_enabled in feature_flags:
            feature_flags_config += f"<{feature}>{is_enabled}<\\/{feature}>"

        feature_flags_config += "<\\/feature_flags>"

        node.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper.xml",
            "<!-- FEATURE FLAGS -->",
            feature_flags_config,
        )

    node.start_clickhouse(retry_start=not expect_fail)
    keeper_utils.wait_until_connected(cluster, node)


def test_keeper_feature_flags(started_cluster):
    restart_clickhouse()

    def assert_feature_flags(feature_flags):
        res = keeper_utils.send_4lw_cmd(started_cluster, node, "ftfl")

        for feature, is_enabled in feature_flags:
            node.wait_for_log_line(
                f"ZooKeeperClient: Keeper feature flag {feature.upper()}: {'enabled' if is_enabled else 'disabled'}",
                look_behind_lines=1000,
            )

            node.wait_for_log_line(
                f"KeeperContext: Keeper feature flag {feature.upper()}: {'enabled' if is_enabled else 'disabled'}",
                look_behind_lines=1000,
            )

            assert f"{feature}\t{1 if is_enabled else 0}" in res

    assert_feature_flags(
        [("filtered_list", 1), ("multi_read", 1), ("check_not_exists", 0)]
    )

    feature_flags = [
        ("multi_read", 0),
        ("check_not_exists", 1),
        ("create_if_not_exists", 1),
    ]
    restart_clickhouse(feature_flags)
    assert_feature_flags(feature_flags + [("filtered_list", 1)])

    feature_flags = [
        ("multi_read", 0),
        ("check_not_exists", 0),
        ("filtered_list", 0),
        ("create_if_not_exists", 0),
    ]
    restart_clickhouse(feature_flags)
    assert_feature_flags(feature_flags)

    with pytest.raises(Exception):
        restart_clickhouse([("invalid_feature", 1)], expect_fail=True)

#!/usr/bin/env python3
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_manager import ConfigManager

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config_reloader.xml", "configs/cluster.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shard_names(start_cluster):
    assert (
        node.query(
            "SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names_literal' ORDER BY shard_name"
        )
        == "first\nsecond\n"
    )
    assert (
        node.query(
            "SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names_nums' ORDER BY shard_name"
        )
        == "10\n3\n"
    )
    assert (
        node.query(
            "SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names_with_nodes' ORDER BY shard_name"
        )
        == "3\n6\n"
    )


def test_incorrect_shard_names(start_cluster):
    with ConfigManager() as config_manager:

        config_manager.add_main_config(
            node, "configs/incorrect_cluster_empty.xml", reload_config=False
        )
        response = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "DB::Exception" in response and "INVALID_SHARD_ID" in response
        config_manager.reset()

        config_manager.add_main_config(
            node, "configs/incorrect_cluster_non_unique.xml", reload_config=False
        )
        response = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "DB::Exception" in response and "INVALID_SHARD_ID" in response
        config_manager.reset()

        config_manager.add_main_config(
            node, "configs/incorrect_cluster_no_shard_name.xml", reload_config=False
        )
        response = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        print(response)
        assert "DB::Exception" in response and "INVALID_SHARD_ID" in response
        config_manager.reset()

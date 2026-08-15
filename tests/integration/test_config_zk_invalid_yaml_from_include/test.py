"""Malformed YAML in a structural `from_zk` include must reject the configuration."""

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    user_configs=["configs/config_zk_users.xml"],
    with_zookeeper=True,
)


def test_config_zk_invalid_yaml_from_include():
    def create_zk_root(zk):
        zk.create(
            path="/profile_settings_yaml",
            value=b"max_query_size: [1",
            makepath=True,
        )

    cluster.add_zookeeper_startup_command(create_zk_root)
    try:
        with pytest.raises(Exception, match="Unable to parse YAML configuration from a string"):
            cluster.start()
    finally:
        cluster.shutdown()

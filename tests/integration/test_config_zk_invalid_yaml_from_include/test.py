"""Malformed YAML in a structural `from_zk` include must reject the configuration."""

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    user_configs=["configs/config_zk_users.xml"],
    with_zookeeper=True,
)


@pytest.mark.parametrize(
    "config_name",
    [
        "config_zk_yaml_on_leaf.xml",
        "config_zk_yaml_invalid_value.xml",
    ],
)
def test_config_zk_yaml_attribute_is_rejected_when_unsupported(config_name):
    invalid_attribute_cluster = ClickHouseCluster(__file__)
    invalid_attribute_cluster.add_instance(
        "node",
        user_configs=[f"configs/{config_name}"],
        with_zookeeper=True,
    )

    try:
        with pytest.raises(Exception, match="failed to start"):
            invalid_attribute_cluster.start()
    finally:
        invalid_attribute_cluster.shutdown()


def test_config_zk_invalid_yaml_from_include():
    def create_zk_root(zk):
        zk.create(
            path="/profile_settings_yaml",
            value=b"max_query_size: [1",
            makepath=True,
        )

    cluster.add_zookeeper_startup_command(create_zk_root)
    try:
        with pytest.raises(Exception, match="failed to start"):
            cluster.start()
    finally:
        cluster.shutdown()

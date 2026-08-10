import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

nodes = {
    "node0": cluster.add_instance(
        "node0",
        main_configs=["config/config_discovery_disabled.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
    "node1": cluster.add_instance(
        "node1",
        main_configs=["config/config_discovery_disabled.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
}

CONFIG_PATH = "/etc/clickhouse-server/config.d/config_discovery_disabled.xml"

CONFIG_ENABLED = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_enable_after_startup>
            <discovery>
                <path>/clickhouse/discovery/test_enable_after_startup</path>
            </discovery>
        </test_enable_after_startup>
    </remote_servers>
</clickhouse>
"""


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_enable_discovery_after_startup_starts_worker(start_cluster):
    """Creating ClusterDiscovery on reload must start the worker without a process restart."""
    for node in nodes.values():
        count = int(
            node.query(
                "SELECT count() FROM system.clusters WHERE cluster = 'test_enable_after_startup'",
                password="passwordAbc",
            )
        )
        assert count == 0

    for node in nodes.values():
        node.replace_config(CONFIG_PATH, CONFIG_ENABLED)
        node.query("SYSTEM RELOAD CONFIG", password="passwordAbc")

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_enable_after_startup",
        what="count()",
        msg="Discovery cluster missing after enabling feature post-startup",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

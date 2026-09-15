import time

import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

nodes = {
    "node0": cluster.add_instance(
        "node0",
        main_configs=["config/config_discovery_disabled_with_path.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
    "node1": cluster.add_instance(
        "node1",
        main_configs=["config/config_discovery_disabled_with_path.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
}

CONFIG_PATH = "/etc/clickhouse-server/config.d/config_discovery_disabled_with_path.xml"

CONFIG_ALLOW_DISABLED = """
<clickhouse>
    <allow_experimental_cluster_discovery>0</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_enable_allow_only>
            <discovery>
                <path>/clickhouse/discovery/test_enable_allow_only</path>
            </discovery>
        </test_enable_allow_only>
    </remote_servers>
</clickhouse>
"""

CONFIG_ALLOW_ENABLED = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_enable_allow_only>
            <discovery>
                <path>/clickhouse/discovery/test_enable_allow_only</path>
            </discovery>
        </test_enable_allow_only>
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


def _cluster_host_count(node):
    return int(
        node.query(
            "SELECT count() FROM system.clusters WHERE cluster = 'test_enable_allow_only'",
            password="passwordAbc",
        )
    )


def test_enable_allow_flag_only_starts_worker(start_cluster):
    """Flipping only allow_experimental_cluster_discovery must start discovery (remote_servers unchanged)."""
    for node in nodes.values():
        node.replace_config(CONFIG_PATH, CONFIG_ALLOW_DISABLED)
        node.query("SYSTEM RELOAD CONFIG", password="passwordAbc")

    for _ in range(15):
        if all(_cluster_host_count(node) == 0 for node in nodes.values()):
            break
        time.sleep(1)
    else:
        raise AssertionError("Discovery cluster still published after allow=0 baseline")

    for node in nodes.values():
        node.replace_config(CONFIG_PATH, CONFIG_ALLOW_ENABLED)
        node.query("SYSTEM RELOAD CONFIG", password="passwordAbc")

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_enable_allow_only",
        what="count()",
        msg="Discovery cluster missing after allow-flag-only reload",
        query_params={"password": "passwordAbc"},
        retries=6,
    )


def test_disable_allow_flag_only_stops_discovery(start_cluster):
    """allow 1 → 0 must unregister and unpublish; 0 → 1 must restore without changing remote_servers."""
    for node in nodes.values():
        node.replace_config(CONFIG_PATH, CONFIG_ALLOW_ENABLED)
        node.query("SYSTEM RELOAD CONFIG", password="passwordAbc")

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_enable_allow_only",
        what="count()",
        msg="Discovery cluster not ready before allow-disable test",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    node0 = nodes["node0"]
    node1 = nodes["node1"]

    node0.replace_config(CONFIG_PATH, CONFIG_ALLOW_DISABLED)
    node0.query("SYSTEM RELOAD CONFIG", password="passwordAbc")

    for _ in range(15):
        if _cluster_host_count(node0) == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("node0 still publishes discovery cluster after allow=0 reload")

    for _ in range(15):
        if _cluster_host_count(node1) == 1:
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"node1 still sees node0 in Keeper after allow=0 on node0; hosts={_cluster_host_count(node1)}"
        )

    node0.replace_config(CONFIG_PATH, CONFIG_ALLOW_ENABLED)
    node0.query("SYSTEM RELOAD CONFIG", password="passwordAbc")

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_enable_allow_only",
        what="count()",
        msg="Discovery cluster missing after allow 0 → 1 reload",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

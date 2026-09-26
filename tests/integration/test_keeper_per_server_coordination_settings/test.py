import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=[
            f"configs/enable_keeper{i}.xml",
            "configs/per_server_coordination_settings.xml",
            "configs/use_keeper.xml",
        ],
        stay_alive=True,
    )
    for i in (1, 2, 3)
]
node1, node2, node3 = nodes

PER_SERVER_CONFIG_PATH = (
    "/etc/clickhouse-server/config.d/per_server_coordination_settings.xml"
)

# Hot reload: max_request_size changes for servers 1 and 2, stays default for server 3.
# use_lsmt_storage is not hot-reloadable and must stay as it was on startup.
UPDATED_PER_SERVER_CONFIG = """
<clickhouse>
    <keeper_server>
        <per_server_coordination_settings>
            <server-1>
                <use_lsmt_storage>true</use_lsmt_storage>
                <max_request_size>2000</max_request_size>
            </server-1>
            <server-2>
                <max_request_size>3000</max_request_size>
            </server-2>
            <server-3>
                <use_lsmt_storage>false</use_lsmt_storage>
            </server-3>
        </per_server_coordination_settings>
    </keeper_server>
</clickhouse>
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)
        yield cluster
    finally:
        cluster.shutdown()


def get_coordination_settings(node):
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="conf")
    result = {}
    for line in data.split("\n"):
        if "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def wait_for_setting(node, name, expected):
    settings = {}
    for _ in range(30):
        settings = get_coordination_settings(node)
        if settings.get(name) == expected:
            return settings
        time.sleep(1)
    assert False, (
        f"{node.name}: {name} did not change to {expected} after config reload; "
        f"current value: {settings.get(name)}"
    )


def test_per_server_overrides(started_cluster):
    # All servers have identical configs: use_lsmt_storage=false and max_request_size=0
    # in coordination_settings, overridden for servers 1 and 3 in the per-server section.
    for node, lsmt, max_request_size in (
        (node1, "true", "1000"),
        (node2, "false", "0"),
        (node3, "true", "0"),
    ):
        settings = get_coordination_settings(node)
        assert settings["use_lsmt_storage"] == lsmt, node.name
        assert settings["max_request_size"] == max_request_size, node.name

    # The LSM tree storage writes an `info` file to data_storage_path on startup;
    # the memory storage doesn't touch that directory.
    for node, expected in ((node1, "1"), (node2, "0"), (node3, "1")):
        exists = node.exec_in_container(
            [
                "bash",
                "-c",
                "test -f /var/lib/clickhouse/coordination/data/info && echo 1 || echo 0",
            ]
        ).strip()
        assert exists == expected, node.name

    # The mixed cluster works: a write through one server is visible through all.
    zk_clients = []
    try:
        for node in nodes:
            zk_clients.append(keeper_utils.get_fake_zk(cluster, node.name))

        zk_clients[1].create("/test_per_server_settings", b"hello")
        for i, node in enumerate(nodes):
            zk_clients[i].sync("/test_per_server_settings")
            value = zk_clients[i].get("/test_per_server_settings")[0]
            assert value == b"hello", node.name
    finally:
        for zk in zk_clients:
            zk.stop()
            zk.close()


def test_per_server_overrides_hot_reload(started_cluster):
    for node in nodes:
        node.replace_config(PER_SERVER_CONFIG_PATH, UPDATED_PER_SERVER_CONFIG)
        node.query("SYSTEM RELOAD CONFIG")

    settings1 = wait_for_setting(node1, "max_request_size", "2000")
    settings2 = wait_for_setting(node2, "max_request_size", "3000")
    settings3 = get_coordination_settings(node3)
    assert settings3["max_request_size"] == "0"

    # Not hot-reloadable: unchanged despite the new per-server values.
    assert settings1["use_lsmt_storage"] == "true"
    assert settings2["use_lsmt_storage"] == "false"
    assert settings3["use_lsmt_storage"] == "true"

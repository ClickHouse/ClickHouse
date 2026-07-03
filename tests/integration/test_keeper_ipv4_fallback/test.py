import os
import pytest
import subprocess
from helpers.cluster import ClickHouseCluster

keeper_config_ipv6_enabled = """
<clickhouse>
    <keeper_server>
        <enable_ipv6>true</enable_ipv6>
    </keeper_server>
</clickhouse>
"""

@pytest.fixture(scope="module")
def started_cluster(request):
    if os.geteuid() != 0:
        pytest.skip("requires root privileges to run sysctl")

    cluster = ClickHouseCluster(request.module.__file__)

    test_configs_dir = os.path.join(os.path.dirname(request.module.__file__), 'configs')
    os.makedirs(test_configs_dir, exist_ok=True)
    config_path = os.path.join(test_configs_dir, "keeper_ipv6_enabled.xml")

    with open(config_path, "w") as f:
        f.write(keeper_config_ipv6_enabled)

    cluster.add_instance(
        "node1",
        with_zookeeper=True,
        user_configs=["configs/keeper_ipv6_enabled.xml"]
    )

    try:
        subprocess.check_call(["sysctl", "-w", "net.ipv6.conf.all.disable_ipv6=1"])
        cluster.start()
        yield cluster
    except subprocess.CalledProcessError:
        pytest.xfail("IPv6 cannot be modified in this environment")
    finally:
        subprocess.check_call(["sysctl", "-w", "net.ipv6.conf.all.disable_ipv6=0"])
        cluster.shutdown()

def test_keeper_starts_on_ipv4_only_node(started_cluster):
    """Checks whether Keeper is able to start successfully on a system with IPv6 disabled."""
    zk = None
    try:
        zk = started_cluster.get_kazoo_client("zoo1")
        result = zk.command(b"ruok")
        assert result == "imok", f"expected 'imok', got '{result}'"
    finally:
        if zk:
            zk.stop()
            zk.close()


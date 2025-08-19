import os
import pytest
import subprocess
from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        subprocess.check_call(["sysctl", "-w", "net.ipv6.conf.all.disable_ipv6=1"])
    except subprocess.CalledProcessError:
        pytest.xfail("IPv6 cannot be modified in this environment")

    cluster = ClickHouseCluster(request.module.__file__)
    cluster.add_instance("node1", with_zookeeper=True)

    try:
        cluster.start()
        yield cluster
    finally:
        try:
            subprocess.check_call(["sysctl", "-w", "net.ipv6.conf.all.disable_ipv6=0"])
        except subprocess.CalledProcessError:
            pytest.xfail("Failed to restore IPv6 setting")
        cluster.shutdown()

@pytest.mark.skipif(os.geteuid() != 0, reason="requires root privileges")
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


import pytest
from helpers.cluster import ClickHouseCluster
import time

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/keeper_config.xml'], stay_alive=True)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            zk = get_fake_zk(node.name, timeout=30.0)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            destroy_zk_client(zk)
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def wait_nodes():
    for n in [node1]:
        wait_node(n)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance


def test_session_timeout(started_cluster):
    zk = None
    try:
        wait_nodes()

        zk1 = get_fake_zk(node1.name, timeout=1.0)
        assert zk1._session_timeout == 5000

        zk1 = get_fake_zk(node1.name, timeout=8.0)
        assert zk1._session_timeout == 8000

        zk1 = get_fake_zk(node1.name, timeout=20.0)
        assert zk1._session_timeout == 10000
    finally:
        destroy_zk_client(zk)

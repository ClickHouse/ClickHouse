#!/usr/bin/env python3

#!/usr/bin/env python3
from helpers.cluster import ClickHouseCluster
from kazoo.client import KazooClient

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml'], stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml'], stay_alive=True)

def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance

def test_smoke():
    try:
        cluster.start()

        node1_zk = get_fake_zk("node1")
        node1_zk.create("/test_alive", b"aaaa")

    finally:
        cluster.shutdown()

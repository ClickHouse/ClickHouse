import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/config_no_substs.xml']) # hardcoded value 33333
node2 = cluster.add_instance('node2', main_configs=['configs/config_env.xml'], env_variables={"MAX_QUERY_SIZE": "55555"})
node3 = cluster.add_instance('node3', main_configs=['configs/config_zk.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', main_configs=['configs/config_incl.xml', 'configs/max_query_size.xml']) # include value 77777

@pytest.fixture(scope="module")
def start_cluster():
    try:
        def create_zk_roots(zk):
            zk.create(path="/setting/max_query_size", value="77777", makepath=True)
        cluster.add_zookeeper_startup_command(create_zk_roots)

        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_config(start_cluster):
   assert node1.query("select value from system.settings where name = 'max_query_size'") == "33333\n"
   assert node2.query("select value from system.settings where name = 'max_query_size'") == "55555\n"
   assert node3.query("select value from system.settings where name = 'max_query_size'") == "77777\n"
   assert node4.query("select value from system.settings where name = 'max_query_size'") == "99999\n"

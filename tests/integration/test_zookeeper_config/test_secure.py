

import threading
import os 
from tempfile import NamedTemporaryFile

import pytest
from helpers.cluster import ClickHouseCluster

TEST_DIR = os.path.dirname(__file__)

cluster = ClickHouseCluster(__file__, name="secure",
                            zookeeper_certfile=os.path.join(TEST_DIR, "configs_secure", "client.crt"),
                            zookeeper_keyfile=os.path.join(TEST_DIR, "configs_secure", "client.key"))

node1 = cluster.add_instance('node1', main_configs=["configs_secure/client.crt", "configs_secure/client.key",
                                                    "configs_secure/conf.d/remote_servers.xml",
                                                    "configs_secure/conf.d/ssl_conf.xml",
                                                    "configs/zookeeper_config_with_ssl.xml"], 
                             with_zookeeper_secure=True)
node2 = cluster.add_instance('node2', main_configs=["configs_secure/client.crt", "configs_secure/client.key",
                                                    "configs_secure/conf.d/remote_servers.xml",
                                                    "configs_secure/conf.d/ssl_conf.xml",
                                                    "configs/zookeeper_config_with_ssl.xml"],
                             with_zookeeper_secure=True)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

# NOTE this test have to be ported to Keeper
def test_secure_connection(started_cluster):
        assert node1.query("SELECT count() FROM system.zookeeper WHERE path = '/'") == '2\n'
        assert node2.query("SELECT count() FROM system.zookeeper WHERE path = '/'") == '2\n'

        kThreadsNumber = 16
        kIterations = 100
        threads = []
        for _ in range(kThreadsNumber):
            threads.append(threading.Thread(target=(lambda: 
                [node1.query("SELECT count() FROM system.zookeeper WHERE path = '/'") for _ in range(kIterations)])))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

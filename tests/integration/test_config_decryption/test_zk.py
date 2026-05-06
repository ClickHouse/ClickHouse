import logging
import pytest
import os
from helpers.cluster import ClickHouseCluster

def create_params_in_zk(zk):
    zk.ensure_path("/clickhouse")

    key128 = "/clickhouse/key128"
    zk.create(key128, b"00112233445566778899aabbccddeeff")
    logging.debug(f"Create ZK key: {key128}, value: {zk.get(key128)}")


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True, main_configs=["configs/config_zk.xml"], user_configs=["configs/users.xml"])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_zookeeper_startup_command(create_params_in_zk)
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_successful_decryption_xml_keys_from_zk(started_cluster):
    assert (node.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        ) == "test_user\n")

import logging
import pytest
import os
from helpers.cluster import ClickHouseCluster

TEST_DIR = os.path.dirname(__file__)

cluster = ClickHouseCluster(
    __file__,
    zookeeper_certfile=os.path.join(TEST_DIR, "configs", "client.crt"),
    zookeeper_keyfile=os.path.join(TEST_DIR, "configs", "client.key"),
)


def create_params_in_zk(zk):
    zk.ensure_path("/clickhouse")

    key128 = "/clickhouse/key128"
    zk.create(key128, b"00112233445566778899aabbccddeeff")
    logging.debug(f"Create ZK key: {key128}, value: {zk.get(key128)}")


node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config_zk.xml",
        "configs/zookeeper_config_with_ssl.xml",
        "configs/ssl_conf.xml",
        "configs/client.crt",
        "configs/client.key",
    ],
    user_configs=["configs/users.xml"],
    with_zookeeper_secure=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_zookeeper_startup_command(create_params_in_zk)
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_successful_decryption_xml_keys_from_zk_secure(started_cluster):
    assert (
        node.query("SELECT currentUser()", user="test_user", password="test_password")
        == "test_user\n"
    )

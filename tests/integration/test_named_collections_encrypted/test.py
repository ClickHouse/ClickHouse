import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NAMED_COLLECTIONS_CONFIG = os.path.join(
    SCRIPT_DIR, "./configs/config.d/named_collections.xml"
)

ZK_PATH = "/named_collections_path"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node_encrypted",
            main_configs=[
                "configs/config.d/named_collections_encrypted.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_with_keeper_encrypted",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper_encrypted.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_with_keeper_2_encrypted",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper_encrypted.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def check_encrypted_content(node, zk=None):
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections").strip()
    )

    assert (
        "['key1','key2']"
        == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection2'"
        ).strip()
    )

    assert (
        "1234\tvalue2"
        == node.query(
            "select collection['key1'], collection['key2'] from system.named_collections where name = 'collection2'"
        ).strip()
    )

    # Check that the underlying storage is encrypted
    content = (
        zk.get(ZK_PATH + "/collection2.sql")[0]
        if zk is not None
        else open(
            f"{node.path}/database/named_collections/collection2.sql", "rb"
        ).read()
    )

    assert (
        content[0:3] == b"ENC"
    )  # file signature (aka magic number) of the encrypted file
    assert b"key1" not in content
    assert b"1234" not in content
    assert b"key2" not in content
    assert b"value2" not in content


def test_local_storage_encrypted(cluster):
    node = cluster.instances["node_encrypted"]
    node.query("CREATE NAMED COLLECTION collection2 AS key1=1234, key2='value2'")

    check_encrypted_content(node)
    node.restart_clickhouse()
    check_encrypted_content(node)

    node.query("DROP NAMED COLLECTION collection2")


def test_zookeper_storage_encrypted(cluster):
    node1 = cluster.instances["node_with_keeper_encrypted"]
    node2 = cluster.instances["node_with_keeper_2_encrypted"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("CREATE NAMED COLLECTION collection2 AS key1=1234, key2='value2'")

    check_encrypted_content(node1, zk)
    check_encrypted_content(node2, zk)
    node1.restart_clickhouse()
    node2.restart_clickhouse()
    check_encrypted_content(node1, zk)
    check_encrypted_content(node2, zk)

    node1.query("DROP NAMED COLLECTION collection2")

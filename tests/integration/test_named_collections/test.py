import logging
import pytest
import os
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NAMED_COLLECTIONS_CONFIG = os.path.join(
    SCRIPT_DIR, "./configs/config.d/named_collections.xml"
)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def replace_config(path, old, new):
    config = open(path, "r")
    config_lines = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(path, "w")
    config.writelines(config_lines)
    config.close()


def test_config_reload(cluster):
    node = cluster.instances["node"]
    assert (
        "collection1" == node.query("select name from system.named_collections").strip()
    )
    assert (
        "['key1']"
        == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection1'"
        ).strip()
    )
    assert (
        "value1"
        == node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
    )

    replace_config(
        NAMED_COLLECTIONS_CONFIG,
        "<key1>value1",
        "<key1>value2",
    )

    node.query("SYSTEM RELOAD CONFIG")

    assert (
        "['key1']"
        == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection1'"
        ).strip()
    )
    assert (
        "value2"
        == node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
    )


def test_create_and_drop_collection(cluster):
    node = cluster.instances["node"]
    assert "1" == node.query("select count() from system.named_collections").strip()

    node.query("CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'")

    def check_created():
        assert "2" == node.query("select count() from system.named_collections").strip()
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
            "1"
            == node.query(
                "select collection['key1'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "value2"
            == node.query(
                "select collection['key2'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

    check_created()
    node.restart_clickhouse()
    check_created()

    node.query("DROP NAMED COLLECTION collection2")

    def check_dropped():
        assert "1" == node.query("select count() from system.named_collections").strip()
        assert (
            "collection1"
            == node.query("select name from system.named_collections").strip()
        )

    check_dropped()
    node.restart_clickhouse()
    check_dropped()

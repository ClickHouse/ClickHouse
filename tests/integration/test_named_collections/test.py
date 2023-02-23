import logging
import pytest
import os
import time
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
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_no_default_access",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            user_configs=[
                "configs/users.d/users_no_default_access.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_no_default_access_but_with_access_management",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            user_configs=[
                "configs/users.d/users_no_default_access_with_access_management.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def replace_in_server_config(node, old, new):
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/named_collections.xml",
        old,
        new,
    )


def replace_in_users_config(node, old, new):
    node.replace_in_config(
        "/etc/clickhouse-server/users.d/users.xml",
        old,
        new,
    )


def test_access(cluster):
    node = cluster.instances["node_no_default_access"]
    assert (
        "DB::Exception: default: Not enough privileges. To execute this query it's necessary to have grant SHOW NAMED COLLECTIONS ON *.*"
        in node.query_and_get_error("select count() from system.named_collections")
    )
    node = cluster.instances["node_no_default_access_but_with_access_management"]
    assert (
        "DB::Exception: default: Not enough privileges. To execute this query it's necessary to have grant SHOW NAMED COLLECTIONS ON *.*"
        in node.query_and_get_error("select count() from system.named_collections")
    )

    node = cluster.instances["node"]
    assert int(node.query("select count() from system.named_collections")) > 0
    replace_in_users_config(
        node, "show_named_collections>1", "show_named_collections>0"
    )
    assert "show_named_collections>0" in node.exec_in_container(
        ["bash", "-c", f"cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert (
        "DB::Exception: default: Not enough privileges. To execute this query it's necessary to have grant SHOW NAMED COLLECTIONS ON *.*"
        in node.query_and_get_error("select count() from system.named_collections")
    )
    replace_in_users_config(
        node, "show_named_collections>0", "show_named_collections>1"
    )
    assert "show_named_collections>1" in node.exec_in_container(
        ["bash", "-c", f"cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert (
        node.query("select collection['key1'] from system.named_collections").strip()
        == "value1"
    )
    replace_in_users_config(
        node, "show_named_collections_secrets>1", "show_named_collections_secrets>0"
    )
    assert "show_named_collections_secrets>0" in node.exec_in_container(
        ["bash", "-c", f"cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert (
        node.query("select collection['key1'] from system.named_collections").strip()
        == "[HIDDEN]"
    )
    replace_in_users_config(
        node, "show_named_collections_secrets>0", "show_named_collections_secrets>1"
    )
    assert "show_named_collections_secrets>1" in node.exec_in_container(
        ["bash", "-c", f"cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert (
        node.query("select collection['key1'] from system.named_collections").strip()
        == "value1"
    )


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

    replace_in_server_config(node, "value1", "value2")
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


def test_sql_commands(cluster):
    node = cluster.instances["node"]
    assert "1" == node.query("select count() from system.named_collections").strip()

    node.query("CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'")

    def check_created():
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

    node.query("ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'")

    def check_altered():
        assert (
            "['key1','key2','key3']"
            == node.query(
                "select mapKeys(collection) from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "4"
            == node.query(
                "select collection['key1'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "value3"
            == node.query(
                "select collection['key3'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

    check_altered()
    node.restart_clickhouse()
    check_altered()

    node.query("ALTER NAMED COLLECTION collection2 DELETE key2")

    def check_deleted():
        assert (
            "['key1','key3']"
            == node.query(
                "select mapKeys(collection) from system.named_collections where name = 'collection2'"
            ).strip()
        )

    check_deleted()
    node.restart_clickhouse()
    check_deleted()

    node.query(
        "ALTER NAMED COLLECTION collection2 SET key3=3, key4='value4' DELETE key1"
    )

    def check_altered_and_deleted():
        assert (
            "['key3','key4']"
            == node.query(
                "select mapKeys(collection) from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "3"
            == node.query(
                "select collection['key3'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "value4"
            == node.query(
                "select collection['key4'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

    check_altered_and_deleted()
    node.restart_clickhouse()
    check_altered_and_deleted()

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

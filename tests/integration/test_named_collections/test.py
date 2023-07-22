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
            "node_only_named_collection_control",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            user_configs=[
                "configs/users.d/users_only_named_collection_control.xml",
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


def test_default_access(cluster):
    node = cluster.instances["node_no_default_access"]
    assert 0 == int(node.query("select count() from system.named_collections"))
    node = cluster.instances["node_only_named_collection_control"]
    assert 1 == int(node.query("select count() from system.named_collections"))
    assert (
        node.query("select collection['key1'] from system.named_collections").strip()
        == "[HIDDEN]"
    )

    node = cluster.instances["node"]
    assert int(node.query("select count() from system.named_collections")) > 0

    replace_in_users_config(
        node, "named_collection_control>1", "named_collection_control>0"
    )
    assert "named_collection_control>0" in node.exec_in_container(
        ["bash", "-c", f"cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert 0 == int(node.query("select count() from system.named_collections"))

    replace_in_users_config(
        node, "named_collection_control>0", "named_collection_control>1"
    )
    assert "named_collection_control>1" in node.exec_in_container(
        ["bash", "-c", f"cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert (
        node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
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
        node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
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
        node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
        == "value1"
    )


def test_granular_access_show_query(cluster):
    node = cluster.instances["node"]
    assert (
        "GRANT ALL ON *.* TO default WITH GRANT OPTION"
        == node.query("SHOW GRANTS FOR default").strip()
    )  # includes named collections control
    assert 1 == int(node.query("SELECT count() FROM system.named_collections"))
    assert (
        "collection1" == node.query("SELECT name FROM system.named_collections").strip()
    )

    node.query("DROP USER IF EXISTS kek")
    node.query("CREATE USER kek")
    node.query("GRANT select ON *.* TO kek")
    assert 0 == int(
        node.query("SELECT count() FROM system.named_collections", user="kek")
    )

    node.query("GRANT show named collections ON collection1 TO kek")
    assert 1 == int(
        node.query("SELECT count() FROM system.named_collections", user="kek")
    )
    assert (
        "collection1"
        == node.query("SELECT name FROM system.named_collections", user="kek").strip()
    )

    node.query("CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'")
    assert 2 == int(node.query("SELECT count() FROM system.named_collections"))
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections").strip()
    )

    assert 1 == int(
        node.query("SELECT count() FROM system.named_collections", user="kek")
    )
    assert (
        "collection1"
        == node.query("select name from system.named_collections", user="kek").strip()
    )

    node.query("GRANT show named collections ON collection2 TO kek")
    assert 2 == int(
        node.query("SELECT count() FROM system.named_collections", user="kek")
    )
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections", user="kek").strip()
    )
    node.restart_clickhouse()
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections", user="kek").strip()
    )

    # check:
    # GRANT show named collections ON *
    # REVOKE show named collections ON collection

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT select ON *.* TO koko")
    assert 0 == int(
        node.query("SELECT count() FROM system.named_collections", user="koko")
    )
    assert "GRANT SELECT ON *.* TO koko" == node.query("SHOW GRANTS FOR koko;").strip()
    node.query("GRANT show named collections ON * TO koko")
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT SHOW NAMED COLLECTIONS ON * TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections", user="koko").strip()
    )
    node.restart_clickhouse()
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT SHOW NAMED COLLECTIONS ON * TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections", user="koko").strip()
    )

    node.query("REVOKE show named collections ON collection1 FROM koko;")
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT SHOW NAMED COLLECTIONS ON * TO koko\nREVOKE SHOW NAMED COLLECTIONS ON collection1 FROM koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )
    assert (
        "collection2"
        == node.query("select name from system.named_collections", user="koko").strip()
    )
    node.restart_clickhouse()
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT SHOW NAMED COLLECTIONS ON * TO koko\nREVOKE SHOW NAMED COLLECTIONS ON collection1 FROM koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )
    assert (
        "collection2"
        == node.query("select name from system.named_collections", user="koko").strip()
    )
    node.query("REVOKE show named collections ON collection2 FROM koko;")
    assert (
        "" == node.query("select * from system.named_collections", user="koko").strip()
    )
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT SHOW NAMED COLLECTIONS ON * TO koko\nREVOKE SHOW NAMED COLLECTIONS ON collection1 FROM koko\nREVOKE SHOW NAMED COLLECTIONS ON collection2 FROM koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    # check:
    # GRANT show named collections ON collection
    # REVOKE show named collections ON *

    node.query("GRANT show named collections ON collection2 TO koko")
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT SHOW NAMED COLLECTIONS ON * TO koko\nREVOKE SHOW NAMED COLLECTIONS ON collection1 FROM koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )
    assert (
        "collection2"
        == node.query("select name from system.named_collections", user="koko").strip()
    )
    node.query("REVOKE show named collections ON * FROM koko;")
    assert "GRANT SELECT ON *.* TO koko" == node.query("SHOW GRANTS FOR koko;").strip()
    assert (
        "" == node.query("select * from system.named_collections", user="koko").strip()
    )

    node.query("DROP NAMED COLLECTION collection2")


def test_show_grants(cluster):
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT CREATE NAMED COLLECTION ON name1 TO koko")
    node.query("GRANT select ON name1.* TO koko")
    assert (
        "GRANT SELECT ON name1.* TO koko\nGRANT CREATE NAMED COLLECTION ON name1 TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT CREATE NAMED COLLECTION ON name1 TO koko")
    node.query("GRANT select ON name1 TO koko")
    assert (
        "GRANT SELECT ON default.name1 TO koko\nGRANT CREATE NAMED COLLECTION ON name1 TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT select ON name1 TO koko")
    node.query("GRANT CREATE NAMED COLLECTION ON name1 TO koko")
    assert (
        "GRANT SELECT ON default.name1 TO koko\nGRANT CREATE NAMED COLLECTION ON name1 TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT select ON *.* TO koko")
    node.query("GRANT CREATE NAMED COLLECTION ON * TO koko")
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT CREATE NAMED COLLECTION ON * TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT CREATE NAMED COLLECTION ON * TO koko")
    node.query("GRANT select ON *.* TO koko")
    assert (
        "GRANT SELECT ON *.* TO koko\nGRANT CREATE NAMED COLLECTION ON * TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT CREATE NAMED COLLECTION ON * TO koko")
    node.query("GRANT select ON * TO koko")
    assert (
        "GRANT CREATE NAMED COLLECTION ON * TO koko\nGRANT SELECT ON default.* TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )

    node.query("DROP USER IF EXISTS koko")
    node.query("CREATE USER koko")
    node.query("GRANT select ON * TO koko")
    node.query("GRANT CREATE NAMED COLLECTION ON * TO koko")
    assert (
        "GRANT CREATE NAMED COLLECTION ON * TO koko\nGRANT SELECT ON default.* TO koko"
        == node.query("SHOW GRANTS FOR koko;").strip()
    )


def test_granular_access_create_alter_drop_query(cluster):
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS kek")
    node.query("CREATE USER kek")
    node.query("GRANT select ON *.* TO kek")
    assert 0 == int(
        node.query("SELECT count() FROM system.named_collections", user="kek")
    )

    assert (
        "DB::Exception: kek: Not enough privileges. To execute this query it's necessary to have grant CREATE NAMED COLLECTION"
        in node.query_and_get_error(
            "CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'", user="kek"
        )
    )
    node.query("GRANT create named collection ON collection2 TO kek")
    node.query(
        "CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'", user="kek"
    )
    assert 0 == int(
        node.query("select count() from system.named_collections", user="kek")
    )

    node.query("GRANT show named collections ON collection2 TO kek")
    assert (
        "collection2"
        == node.query("select name from system.named_collections", user="kek").strip()
    )
    assert (
        "1"
        == node.query(
            "select collection['key1'] from system.named_collections where name = 'collection2'"
        ).strip()
    )

    assert (
        "DB::Exception: kek: Not enough privileges. To execute this query it's necessary to have grant ALTER NAMED COLLECTION"
        in node.query_and_get_error(
            "ALTER NAMED COLLECTION collection2 SET key1=2", user="kek"
        )
    )
    node.query("GRANT alter named collection ON collection2 TO kek")
    node.query("ALTER NAMED COLLECTION collection2 SET key1=2", user="kek")
    assert (
        "2"
        == node.query(
            "select collection['key1'] from system.named_collections where name = 'collection2'"
        ).strip()
    )
    node.query("REVOKE alter named collection ON collection2 FROM kek")
    assert (
        "DB::Exception: kek: Not enough privileges. To execute this query it's necessary to have grant ALTER NAMED COLLECTION"
        in node.query_and_get_error(
            "ALTER NAMED COLLECTION collection2 SET key1=3", user="kek"
        )
    )

    assert (
        "DB::Exception: kek: Not enough privileges. To execute this query it's necessary to have grant DROP NAMED COLLECTION"
        in node.query_and_get_error("DROP NAMED COLLECTION collection2", user="kek")
    )
    node.query("GRANT drop named collection ON collection2 TO kek")
    node.query("DROP NAMED COLLECTION collection2", user="kek")
    assert 0 == int(
        node.query("select count() from system.named_collections", user="kek")
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

    replace_in_server_config(node, "value2", "value1")
    node.query("SYSTEM RELOAD CONFIG")

    assert (
        "value1"
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

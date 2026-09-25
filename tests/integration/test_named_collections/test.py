import logging
import os
import time
import threading
import random
from contextlib import nullcontext as does_not_raise

import pytest
import uuid

from helpers.client import QueryRuntimeException
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
            "node_with_keeper",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_with_keeper_2",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
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
        ["bash", "-c", "cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert 0 == int(node.query("select count() from system.named_collections"))

    replace_in_users_config(
        node, "named_collection_control>0", "named_collection_control>1"
    )
    assert "named_collection_control>1" in node.exec_in_container(
        ["bash", "-c", "cat /etc/clickhouse-server/users.d/users.xml"]
    )
    node.restart_clickhouse()
    assert (
        node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
        == "value1"
    )

    assert (
        node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'",
            settings={"format_display_secrets_in_show_and_select": 0}
        ).strip()
        == "[HIDDEN]"
    )

    replace_in_server_config(
        node, "display_secrets_in_show_and_select>1", "display_secrets_in_show_and_select>0"
    )
    assert "display_secrets_in_show_and_select>0" in node.exec_in_container(
        ["bash", "-c", "cat /etc/clickhouse-server/config.d/named_collections.xml"]
    )
    node.restart_clickhouse()
    assert (
        node.query(
            "select collection['key1'] from system.named_collections where name = 'collection1'"
        ).strip()
        == "[HIDDEN]"
    )

    replace_in_server_config(
        node, "display_secrets_in_show_and_select>0", "display_secrets_in_show_and_select>1"
    )
    assert "display_secrets_in_show_and_select>1" in node.exec_in_container(
        ["bash", "-c", "cat /etc/clickhouse-server/config.d/named_collections.xml"]
    )

    replace_in_users_config(
        node, "show_named_collections_secrets>1", "show_named_collections_secrets>0"
    )
    assert "show_named_collections_secrets>0" in node.exec_in_container(
        ["bash", "-c", "cat /etc/clickhouse-server/users.d/users.xml"]
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
        ["bash", "-c", "cat /etc/clickhouse-server/users.d/users.xml"]
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
        "DB::Exception: kek: Not enough privileges. To execute this query, it's necessary to have the grant CREATE NAMED COLLECTION"
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
        "DB::Exception: kek: Not enough privileges. To execute this query, it's necessary to have the grant ALTER NAMED COLLECTION"
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
        "DB::Exception: kek: Not enough privileges. To execute this query, it's necessary to have the grant ALTER NAMED COLLECTION"
        in node.query_and_get_error(
            "ALTER NAMED COLLECTION collection2 SET key1=3", user="kek"
        )
    )

    assert (
        "DB::Exception: kek: Not enough privileges. To execute this query, it's necessary to have the grant DROP NAMED COLLECTION"
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


def test_storage_type_does_not_change_on_config_reload(cluster):
    node = cluster.instances["node"]

    def get_storage_type_state(instance):
        return instance.query(
            """
            SELECT name, value, default, changed, type, changeable_without_restart,
                getServerSetting('named_collections_storage_type')
            FROM system.server_settings
            WHERE name = 'named_collections_storage.type'
            """
        ).strip()

    assert (
        "named_collections_storage.type\tlocal\tlocal\t0\tString\tNo\tlocal"
        == get_storage_type_state(node)
    )
    assert (
        "named_collections_storage.type\tzookeeper\tlocal\t1\tString\tNo\tzookeeper"
        == get_storage_type_state(cluster.instances["node_with_keeper"])
    )

    config = """
<clickhouse>
  <named_collections_storage>
    <type>keeper</type>
    <path>/named_collections_reload_test</path>
  </named_collections_storage>
</clickhouse>
"""

    with node.with_replace_config(
        "/etc/clickhouse-server/config.d/named_collections.xml",
        config,
        reload_before=True,
        reload_after=True,
    ):
        assert (
            "named_collections_storage.type\tlocal\tlocal\t1\tString\tNo\tlocal"
            == get_storage_type_state(node)
        )

        node.query("CREATE NAMED COLLECTION storage_type_reload_test AS value = 1")
        assert "1" == node.query(
            """
            SELECT collection['value']
            FROM system.named_collections
            WHERE name = 'storage_type_reload_test'
            """
        ).strip()
        node.query("DROP NAMED COLLECTION storage_type_reload_test")


@pytest.mark.parametrize("with_keeper", [False, True])
def test_sql_commands(cluster, with_keeper):
    zk = None
    node = None
    if with_keeper:
        node = cluster.instances["node_with_keeper"]
        zk = cluster.get_kazoo_client("zoo1")
    else:
        node = cluster.instances["node"]

    assert "1" == node.query("select count() from system.named_collections").strip()

    query_id = f"query_{uuid.uuid4()}"
    node.query(
        "CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%value2%' and query_id = '{query_id}'"
        )
    )
    assert 1 == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%CREATE NAMED COLLECTION collection2 AS key1 = \\'[HIDDEN]\\', key2 = \\'[HIDDEN]\\' (stage: Complete)%' and query_id = '{query_id}'"
        )
    )
    assert "key1 = \\'[HIDDEN]\\', key2 = \\'[HIDDEN]\\'" in node.query(f"SELECT query FROM system.query_log WHERE query_id = '{query_id}'")

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
        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 1, key2 = 'value2'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_created()
    node.restart_clickhouse()
    check_created()

    query_id = f"query_{uuid.uuid4()}"
    node.query(
        "ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%value3%' and query_id = '{query_id}'"
        )
    )
    assert 1 == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%ALTER NAMED COLLECTION collection2 SET key1 = \\'[HIDDEN]\\', key3 = \\'[HIDDEN]\\' (stage: Complete)%' and query_id = '{query_id}'"
        )
    )

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

        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 4, key2 = 'value2', key3 = 'value3'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
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

        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 4, key3 = 'value3'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_deleted()
    node.restart_clickhouse()
    check_deleted()

    query_id = f"query_{uuid.uuid4()}"
    node.query(
        "ALTER NAMED COLLECTION collection2 SET key3=3, key4='value4' DELETE key1",
        query_id=query_id,
    )
    time.sleep(2)

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%value3%' and query_id = '{query_id}'"
        )
    )
    assert 1 == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%ALTER NAMED COLLECTION collection2 SET key3 = \\'[HIDDEN]\\', key4 = \\'[HIDDEN]\\' DELETE key1 (stage: Complete)%' and query_id = '{query_id}'"
        )
    )
    assert "key3 = \\'[HIDDEN]\\', key4 = \\'[HIDDEN]\\'" in node.query(f"SELECT query FROM system.query_log WHERE query_id = '{query_id}'")

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

        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key3 = 3, key4 = 'value4'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
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
        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 0 == len(children)

    check_dropped()
    node.restart_clickhouse()
    check_dropped()


def test_keeper_storage(cluster):
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]
    zk = cluster.get_kazoo_client("zoo1")

    assert "1" == node1.query("select count() from system.named_collections").strip()
    assert "1" == node2.query("select count() from system.named_collections").strip()

    node1.query("CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'")

    def check_created(node):
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

        zk.sync(ZK_PATH)
        children = zk.get_children(ZK_PATH)
        assert 1 == len(children)
        assert "collection2.sql" in children
        assert (
            b"CREATE NAMED COLLECTION collection2 AS key1 = 1, key2 = 'value2'"
            in zk.get(ZK_PATH + "/collection2.sql")[0]
        )

    check_created(node1)
    check_created(node2)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    check_created(node1)
    check_created(node2)

    node2.query("ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'")

    time.sleep(5)

    def check_altered(node):
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

        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 4, key2 = 'value2', key3 = 'value3'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_altered(node2)
    check_altered(node1)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    check_altered(node1)
    check_altered(node2)

    node1.query("DROP NAMED COLLECTION collection2")

    time.sleep(5)

    def check_dropped(node):
        assert "1" == node.query("select count() from system.named_collections").strip()
        assert (
            "collection1"
            == node.query("select name from system.named_collections").strip()
        )
        if zk is not None:
            zk.sync(ZK_PATH)
            children = zk.get_children(ZK_PATH)
            assert 0 == len(children)

    check_dropped(node1)
    check_dropped(node2)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    check_dropped(node1)
    check_dropped(node2)


@pytest.mark.parametrize(
    "ignore, expected_raise",
    [(True, does_not_raise()), (False, pytest.raises(QueryRuntimeException))],
)
def test_keeper_storage_remove_on_cluster(cluster, ignore, expected_raise):
    node = cluster.instances["node_with_keeper"]

    replace_in_users_config(
        node,
        "ignore_on_cluster_for_replicated_named_collections_queries>.",
        f"ignore_on_cluster_for_replicated_named_collections_queries>{int(ignore)}",
    )
    node.query("SYSTEM RELOAD CONFIG")

    with expected_raise:
        node.query(
            "DROP NAMED COLLECTION IF EXISTS test_nc ON CLUSTER `replicated_nc_nodes_cluster`"
        )
        node.query(
            "CREATE NAMED COLLECTION test_nc ON CLUSTER `replicated_nc_nodes_cluster` AS key1=1, key2=2 OVERRIDABLE"
        )
        node.query(
            "ALTER NAMED COLLECTION  test_nc ON CLUSTER `replicated_nc_nodes_cluster` SET key2=3"
        )
        node.query(
            "DROP NAMED COLLECTION test_nc ON CLUSTER `replicated_nc_nodes_cluster`"
        )
    node.query("DROP NAMED COLLECTION IF EXISTS test_nc")


@pytest.mark.parametrize(
    "instance_name",
    [("node"), ("node_with_keeper")],
)
def test_name_escaping(cluster, instance_name):
    node = cluster.instances[instance_name]

    node.query("DROP NAMED COLLECTION IF EXISTS `test_!strange/symbols!`;")
    node.query("CREATE NAMED COLLECTION `test_!strange/symbols!` AS key1=1, key2=2")
    node.restart_clickhouse()

    node.query("DROP NAMED COLLECTION `test_!strange/symbols!`")


@pytest.mark.parametrize(
    "instance_name, show_secrets",
    [("node", True), ("node_only_named_collection_control", False)],
)
def test_system_named_collection(cluster, instance_name, show_secrets):
    node = cluster.instances[instance_name]

    def validate_named_collection(collection_name, source, key_value):
        assert (
            node.query(
                f"SELECT name FROM system.named_collections WHERE name='{collection_name}'"
            )
            == f"{collection_name}\n"
        )
        assert (
            node.query(
                f"SELECT collection['key1'] FROM system.named_collections WHERE name='{collection_name}'"
            )
            == f"{f'{key_value}' if show_secrets else '[HIDDEN]'}\n"
        )
        assert (
            node.query(
                f"SELECT source FROM system.named_collections WHERE name='{collection_name}'"
            )
            == f"{source}\n"
        )
        if source == "CONFIG":
            assert (
                node.query(
                    f"SELECT create_query FROM system.named_collections WHERE name='{collection_name}'"
                )
                == "\n"
            )
        else:
            hidden_str = "\\'[HIDDEN]\\'"
            assert (
                node.query(
                    f"SELECT create_query FROM system.named_collections WHERE name='{collection_name}'"
                )
                == f"CREATE NAMED COLLECTION collection2 AS key1 = {f'{key_value}' if show_secrets else hidden_str} OVERRIDABLE\n"
            )

    validate_named_collection("collection1", "CONFIG", "value1")

    node.query("CREATE NAMED COLLECTION collection2 AS key1=1 OVERRIDABLE")
    validate_named_collection("collection2", "SQL", "1")
    node.query("ALTER NAMED COLLECTION collection2 SET key1 = 30")
    validate_named_collection("collection2", "SQL", "30")

    node.query("DROP NAMED COLLECTION collection2")


def test_concurrent_create_drop_race_condition(cluster):
    """
    Test for race condition when collections are deleted between list and read operations.

    The background update thread in NamedCollectionFactory calls `getAll` which first
    lists all collections, then reads each one. If a collection is deleted between
    these operations (by another node or concurrent query), it should not cause an
    exception.

    This test rapidly creates and drops collections concurrently to trigger this race.
    The test passes if no "Logical error" occurs (which would indicate chassert failure).
    """
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    num_iterations = 15
    stop_flag = threading.Event()

    def create_collections(node, prefix, count):
        for i in range(count):
            if stop_flag.is_set():
                break
            try:
                coll_name = f"{prefix}_{i}_{random.randint(0, 10000)}"
                node.query(f"CREATE NAMED COLLECTION IF NOT EXISTS {coll_name} AS key='value'")
            except Exception:
                pass  # Ignore errors during concurrent operations

    def drop_collections(node, prefix, count):
        for i in range(count):
            if stop_flag.is_set():
                break
            try:
                # Try to drop collections that might or might not exist
                collections = node.query(
                    f"SELECT name FROM system.named_collections WHERE name LIKE '{prefix}%'"
                ).strip().split('\n')
                for coll in collections:
                    if coll:
                        node.query(f"DROP NAMED COLLECTION IF EXISTS {coll}")
            except Exception:
                pass  # Ignore errors during concurrent operations

    try:
        # Run multiple iterations to increase chance of hitting the race
        for iteration in range(3):
            prefix = f"race_test_{iteration}"
            threads = []

            # Create threads that create collections on both nodes
            for node in [node1, node2]:
                t = threading.Thread(target=create_collections, args=(node, prefix, num_iterations))
                threads.append(t)

            # Create threads that drop collections on both nodes
            for node in [node1, node2]:
                t = threading.Thread(target=drop_collections, args=(node, prefix, num_iterations))
                threads.append(t)

            # Start all threads
            for t in threads:
                t.start()

            # Wait for all threads to complete
            for t in threads:
                t.join(timeout=60)

            # Small delay between iterations
            time.sleep(0.1)

        # Verify both nodes are still healthy by running a simple query
        for node in [node1, node2]:
            result = node.query("SELECT 1").strip()
            assert result == "1", "Node health check failed"

        # Check for logical errors in server logs - this is the key assertion
        # A logical error would indicate the race condition caused an exception (chassert failure)
        for node, name in [(node1, "node_with_keeper"), (node2, "node_with_keeper_2")]:
            logs = node.grep_in_log("Logical error")
            assert not logs, f"{name}: Found logical error in logs: {logs[:500]}"

    finally:
        stop_flag.set()
        # Cleanup any remaining collections from this test
        for node in [node1, node2]:
            try:
                collections = node.query(
                    "SELECT name FROM system.named_collections WHERE name LIKE 'race_test_%'"
                ).strip().split('\n')
                for coll in collections:
                    if coll:
                        node.query(f"DROP NAMED COLLECTION IF EXISTS {coll}")
            except Exception:
                pass


def test_missing_collection_in_config_does_not_block_startup(cluster):
    """A collection removed from the configuration leaves no DROP to intercept, so a table that
    names it survives into the next start and must not break the loading of its database."""
    node = cluster.instances["node"]

    config = """<clickhouse>
  <named_collections>
    <collection1>
      <key1>value1</key1>
    </collection1>
    <nc_startup>
      <url>http://127.0.0.1:1/none</url>
      <format>TSV</format>
    </nc_startup>
  </named_collections>
  <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
"""

    with node.with_replace_config(
        "/etc/clickhouse-server/config.d/named_collections.xml",
        config,
        reload_before=True,
        reload_after=True,
    ):
        node.query("CREATE TABLE t_startup (n UInt32) ENGINE = URL(nc_startup)")

    assert "nc_startup" not in node.query("SELECT name FROM system.named_collections")

    node.restart_clickhouse()

    assert "t_startup" in node.query(
        "SELECT name FROM system.tables WHERE database = currentDatabase()"
    )
    assert "NAMED_COLLECTION_DOESNT_EXIST" in node.query_and_get_error(
        "SELECT * FROM t_startup"
    )

    # A rename commits its metadata move before it creates the table's symlink, and the symlink
    # needs the table's data path, so a stand-in must not fail it.
    node.query("RENAME TABLE t_startup TO t_startup_renamed")
    assert "t_startup_renamed" in node.query(
        "SELECT name FROM system.tables WHERE database = currentDatabase()"
    )

    assert (
        node.query(
            "SELECT engine FROM system.tables WHERE database = currentDatabase()"
            " AND name = 't_startup_renamed'"
        ).strip()
        == "TableProxy"
    )

    # Putting the collection back makes the table work again without another restart. The read
    # still fails, because the endpoint is unreachable, but no longer on the collection.
    with node.with_replace_config(
        "/etc/clickhouse-server/config.d/named_collections.xml",
        config,
        reload_before=True,
        reload_after=True,
    ):
        assert "NAMED_COLLECTION_DOESNT_EXIST" not in node.query_and_get_error(
            "SELECT * FROM t_startup_renamed"
        )
        # `getName` forwards to the storage once it is built, so the engine column reporting `URL`
        # is what proves the stand-in materialized without a restart.
        assert (
            node.query(
                "SELECT engine FROM system.tables WHERE database = currentDatabase()"
                " AND name = 't_startup_renamed'"
            ).strip()
            == "URL"
        )
        node.query("DROP TABLE t_startup_renamed")


def test_missing_collection_still_fails_a_push_source(cluster):
    """A push source ingests from a background job that only `startup` starts, and it answers a
    rename veto and streaming controls that a stand-in cannot forward, so a missing collection
    must keep failing its load. A `URL` table in the same database still gets its stand-in.

    The instance loads databases asynchronously, so a failed load reports itself on access
    (`ASYNC_LOAD_WAIT_FAILED`) rather than refusing the start.
    """
    node = cluster.instances["node"]

    config = """<clickhouse>
  <named_collections>
    <collection1>
      <key1>value1</key1>
    </collection1>
    <nc_push_url>
      <url>http://127.0.0.1:1/none</url>
      <format>TSV</format>
    </nc_push_url>
    <nc_push_queue>
      <kafka_broker_list>127.0.0.1:1</kafka_broker_list>
      <kafka_topic_list>topic</kafka_topic_list>
      <kafka_group_name>group</kafka_group_name>
      <kafka_format>TSV</kafka_format>
    </nc_push_queue>
  </named_collections>
  <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
"""

    # Own database: while a load has failed, the whole database answers that failure, so a
    # leftover would break every later test in this module.
    with node.with_replace_config(
        "/etc/clickhouse-server/config.d/named_collections.xml",
        config,
        reload_before=True,
        reload_after=True,
    ):
        node.query("CREATE DATABASE db_push_source")
        node.query(
            "CREATE TABLE db_push_source.t_url (n UInt32) ENGINE = URL(nc_push_url)"
        )
        node.query(
            "CREATE TABLE db_push_source.t_queue (n UInt32) ENGINE = Kafka(nc_push_queue)"
        )

    assert "nc_push_queue" not in node.query(
        "SELECT name FROM system.named_collections"
    )

    node.restart_clickhouse()

    try:
        # The push source: its own load failed, so nothing stood in for it. Reading it first also
        # settles the load job, which is what makes the EXISTS reading below deterministic.
        error = node.query_and_get_error("SELECT * FROM db_push_source.t_queue")
        assert "NAMED_COLLECTION_DOESNT_EXIST" in error
        assert "ASYNC_LOAD_WAIT_FAILED" in error
        assert node.query("EXISTS TABLE db_push_source.t_queue").strip() == "0"

        # The URL table in the same database: attached, and the stand-in itself is what reports
        # the missing collection, so its load job completed.
        error = node.query_and_get_error("SELECT * FROM db_push_source.t_url")
        assert "NAMED_COLLECTION_DOESNT_EXIST" in error
        assert "ASYNC_LOAD_WAIT_FAILED" not in error
        assert node.query("EXISTS TABLE db_push_source.t_url").strip() == "1"
    finally:
        # A failed load cannot be dropped, so the collections have to come back first.
        with node.with_replace_config(
            "/etc/clickhouse-server/config.d/named_collections.xml",
            config,
            reload_before=True,
            reload_after=True,
        ):
            node.restart_clickhouse()
            node.query("DROP DATABASE db_push_source SYNC")

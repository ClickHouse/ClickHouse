import logging
import time
import uuid

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

ZK_PATH = "/named_collections_path"


def wait_for_value(node, collection, key, expected, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = node.query(
                f"SELECT collection['{key}'] FROM system.named_collections WHERE name = '{collection}'").strip()
            if result == expected:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def wait_for_keys(node, collection, expected_keys, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = node.query(
                f"SELECT mapKeys(collection) FROM system.named_collections WHERE name = '{collection}'").strip()
            if result == expected_keys:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def wait_exists(node, collection, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if node.query(f"SELECT count() FROM system.named_collections WHERE name = '{collection}'").strip() == "1":
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def wait_not_exists(node, collection, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if node.query(f"SELECT count() FROM system.named_collections WHERE name = '{collection}'").strip() == "0":
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def check_encrypted(zk, collection):
    content = zk.get(f"{ZK_PATH}/{collection}.sql")[0]
    assert content[:3] == b"ENC"
    return content


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        for name in ["node1", "node2"]:
            cluster.add_instance(
                name,
                main_configs=["configs/config.d/named_collections_with_zookeeper_encrypted.xml"],
                user_configs=["configs/users.d/users.xml"],
                stay_alive=True,
                with_zookeeper=True,
            )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def cluster_with_node3():
    try:
        cluster = ClickHouseCluster(__file__, name="with_node3")
        for name in ["node1", "node2", "node3"]:
            cluster.add_instance(
                name,
                main_configs=["configs/config.d/named_collections_with_zookeeper_encrypted.xml"],
                user_configs=["configs/users.d/users.xml"],
                stay_alive=True,
                with_zookeeper=True,
            )
        logging.info("Starting cluster...")
        cluster.start()
        cluster.instances["node3"].stop_clickhouse()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function")
def stopped_node3(cluster_with_node3):
    node3 = cluster_with_node3.instances["node3"]
    try:
        node3.stop_clickhouse()
    except Exception:
        pass
    yield cluster_with_node3
    try:
        node3.stop_clickhouse()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def cleanup_named_collections(request):
    """Drop all named collections before each test."""
    cluster = None
    if 'cluster' in request.fixturenames:
        cluster = request.getfixturevalue('cluster')
    elif 'stopped_node3' in request.fixturenames:
        cluster = request.getfixturevalue('stopped_node3')

    if cluster is not None:
        for instance in cluster.instances.values():
            try:
                collections = instance.query("SELECT name FROM system.named_collections").strip()
                if collections:
                    for coll in collections.split('\n'):
                        if coll:
                            instance.query(f"DROP NAMED COLLECTION IF EXISTS {coll}")
            except Exception:
                pass  # Instance might not be running
    yield


def test_zookeeper_encrypted_storage(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("CREATE NAMED COLLECTION enc_test AS key1=1234, secret='my_secret'")

    assert wait_for_value(node2, "enc_test", "key1", "1234")

    for node in [node1, node2]:
        assert node.query(
            "SELECT collection['secret'] FROM system.named_collections WHERE name='enc_test'").strip() == "my_secret"

    content = check_encrypted(zk, "enc_test")
    assert b"my_secret" not in content
    assert b"1234" not in content


def test_encryption_persists_after_restart(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("CREATE NAMED COLLECTION persist_test AS password='super_secret', host='myhost'")
    assert wait_for_value(node2, "persist_test", "password", "super_secret")

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    for node in [node1, node2]:
        assert node.query(
            "SELECT collection['password'] FROM system.named_collections WHERE name='persist_test'").strip() == "super_secret"

    content = check_encrypted(zk, "persist_test")
    assert b"super_secret" not in content


def test_create_visible_across_nodes(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION create_test AS key1='val1', key2=42")
    assert wait_for_value(node2, "create_test", "key1", "val1")
    assert node2.query(
        "SELECT collection['key2'] FROM system.named_collections WHERE name='create_test'").strip() == "42"

    node2.query("CREATE NAMED COLLECTION create_test2 AS host='127.0.0.1'")
    assert wait_for_value(node1, "create_test2", "host", "127.0.0.1")


def test_create_if_not_exists_and_duplicate(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION dup_test AS key='original'")
    assert wait_exists(node2, "dup_test")

    node2.query("CREATE NAMED COLLECTION IF NOT EXISTS dup_test AS key='modified'")
    assert node1.query(
        "SELECT collection['key'] FROM system.named_collections WHERE name='dup_test'").strip() == "original"

    with pytest.raises(QueryRuntimeException) as exc:
        node2.query("CREATE NAMED COLLECTION dup_test AS key='fail'")
    assert "already exists" in str(exc.value).lower() or "NAMED_COLLECTION_ALREADY_EXISTS" in str(exc.value)


def test_alter_operations(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION alter_test AS key1='v1', key2='v2', key3='v3'")
    assert wait_for_value(node2, "alter_test", "key1", "v1")

    node1.query("ALTER NAMED COLLECTION alter_test SET key1='modified'")
    assert wait_for_value(node2, "alter_test", "key1", "modified")

    node2.query("ALTER NAMED COLLECTION alter_test SET key4='added'")
    assert wait_for_value(node1, "alter_test", "key4", "added")

    node1.query("ALTER NAMED COLLECTION alter_test DELETE key2")
    assert wait_for_keys(node2, "alter_test", "['key1','key3','key4']")

    node2.query("ALTER NAMED COLLECTION alter_test SET key1='final', key5='new' DELETE key3")
    assert wait_for_keys(node1, "alter_test", "['key1','key4','key5']")
    assert wait_for_value(node1, "alter_test", "key1", "final")


def test_alter_if_exists_and_nonexistent(cluster):
    node1 = cluster.instances["node1"]
    node1.query("ALTER NAMED COLLECTION IF EXISTS nonexistent SET key='val'")

    with pytest.raises(QueryRuntimeException):
        node1.query("ALTER NAMED COLLECTION definitely_nonexistent SET key='val'")


def test_drop_operations(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION drop_test AS key='val'")
    assert wait_exists(node2, "drop_test")

    node2.query("DROP NAMED COLLECTION drop_test")
    assert wait_not_exists(node1, "drop_test")
    assert wait_not_exists(node2, "drop_test")

    node1.query("DROP NAMED COLLECTION IF EXISTS nonexistent")

    with pytest.raises(QueryRuntimeException):
        node1.query("DROP NAMED COLLECTION totally_nonexistent_xyz")


def test_cannot_delete_last_key(cluster):
    node1 = cluster.instances["node1"]

    node1.query("CREATE NAMED COLLECTION last_key_test AS key1='v1', key2='v2'")

    node1.query("ALTER NAMED COLLECTION last_key_test DELETE key1")

    with pytest.raises(QueryRuntimeException) as exc:
        node1.query("ALTER NAMED COLLECTION last_key_test DELETE key2")
    assert "cannot be empty" in str(exc.value)

    node1.query("ALTER NAMED COLLECTION last_key_test SET key3='v3'")
    node1.query("ALTER NAMED COLLECTION last_key_test DELETE key2")


def test_overridable_keys(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("""
        CREATE NAMED COLLECTION override_test AS
        public='pub' OVERRIDABLE, secret='sec' NOT OVERRIDABLE, normal='norm'
    """)
    assert wait_for_value(node2, "override_test", "public", "pub")
    assert node2.query(
        "SELECT collection['secret'] FROM system.named_collections WHERE name='override_test'").strip() == "sec"


def test_rbac_permissions(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("DROP USER IF EXISTS test_rbac_user")
    node1.query("CREATE USER test_rbac_user")
    node1.query("GRANT SELECT ON *.* TO test_rbac_user")
    node1.query("CREATE NAMED COLLECTION rbac_coll AS key='secret'")
    assert wait_exists(node2, "rbac_coll")

    assert node1.query("SELECT count() FROM system.named_collections", user="test_rbac_user").strip() == "0"

    node1.query("GRANT SHOW NAMED COLLECTIONS ON rbac_coll TO test_rbac_user")
    assert node1.query("SELECT name FROM system.named_collections", user="test_rbac_user").strip() == "rbac_coll"

    with pytest.raises(QueryRuntimeException):
        node1.query("ALTER NAMED COLLECTION rbac_coll SET key='new'", user="test_rbac_user")

    node1.query("GRANT ALTER NAMED COLLECTION ON rbac_coll TO test_rbac_user")
    node1.query("ALTER NAMED COLLECTION rbac_coll SET key='modified'", user="test_rbac_user")
    assert wait_for_value(node2, "rbac_coll", "key", "modified")

    node1.query("DROP USER test_rbac_user")


def test_named_collection_grants_visibility_in_system_grants(cluster):
    node1 = cluster.instances["node1"]

    node1.query("DROP USER IF EXISTS grants_vis_user")

    node1.query("CREATE USER grants_vis_user")
    node1.query("GRANT SELECT ON system.named_collections TO grants_vis_user")
    node1.query("CREATE NAMED COLLECTION foobar AS a='1', b='2'")
    node1.query("CREATE NAMED COLLECTION barfoo AS c='3', d='4'")

    node1.query("GRANT SHOW NAMED COLLECTIONS ON foobar TO grants_vis_user")
    node1.query("GRANT SHOW NAMED COLLECTIONS ON barfoo TO grants_vis_user")
    node1.query("GRANT ALTER NAMED COLLECTION ON foobar TO grants_vis_user")

    grants = node1.query(
        """
        SELECT access_type, database, table, column
        FROM system.grants
        WHERE user_name = 'grants_vis_user'
        ORDER BY access_type
        """).strip()

    grant_count = node1.query(
        """
        SELECT count()
        FROM system.grants
        WHERE user_name = 'grants_vis_user'
        """).strip()
    assert int(grant_count) >= 3

    visible = node1.query("SELECT name FROM system.named_collections ORDER BY name", user="grants_vis_user").strip()
    assert "foobar" in visible
    assert "barfoo" in visible

    node1.query("ALTER NAMED COLLECTION foobar SET a='modified'", user="grants_vis_user")
    assert node1.query("SELECT collection['a'] FROM system.named_collections WHERE name='foobar'").strip() == "modified"

    with pytest.raises(QueryRuntimeException):
        node1.query("ALTER NAMED COLLECTION barfoo SET c='should_fail'", user="grants_vis_user")

    node1.query("REVOKE SHOW NAMED COLLECTIONS ON foobar FROM grants_vis_user")
    visible_after = node1.query("SELECT name FROM system.named_collections", user="grants_vis_user").strip()
    assert "barfoo" in visible_after
    assert "foobar" not in visible_after

    node1.query("DROP USER grants_vis_user")


def test_grants_persist_after_restart(cluster):
    node1 = cluster.instances["node1"]

    node1.query("DROP USER IF EXISTS persist_grant_user")

    node1.query("CREATE USER persist_grant_user")
    node1.query("GRANT SELECT ON *.* TO persist_grant_user")
    node1.query("CREATE NAMED COLLECTION persist_grant_coll AS secret='mysecret'")
    node1.query("GRANT SHOW NAMED COLLECTIONS ON persist_grant_coll TO persist_grant_user")
    node1.query("GRANT ALTER NAMED COLLECTION ON persist_grant_coll TO persist_grant_user")

    assert node1.query("SELECT name FROM system.named_collections",
                       user="persist_grant_user").strip() == "persist_grant_coll"
    node1.query("ALTER NAMED COLLECTION persist_grant_coll SET secret='modified'", user="persist_grant_user")
    assert node1.query(
        "SELECT collection['secret'] FROM system.named_collections WHERE name='persist_grant_coll'").strip() == "modified"

    node1.restart_clickhouse()

    assert node1.query("SELECT name FROM system.named_collections",
                       user="persist_grant_user").strip() == "persist_grant_coll"
    node1.query("ALTER NAMED COLLECTION persist_grant_coll SET secret='after_restart'", user="persist_grant_user")
    assert node1.query(
        "SELECT collection['secret'] FROM system.named_collections WHERE name='persist_grant_coll'").strip() == "after_restart"

    node1.query("DROP USER persist_grant_user")


def test_user_recreation_loses_grants(cluster):
    node1 = cluster.instances["node1"]

    node1.query("DROP USER IF EXISTS recreate_user")

    node1.query("CREATE USER recreate_user")
    node1.query("GRANT SELECT ON *.* TO recreate_user")
    node1.query("CREATE NAMED COLLECTION recreate_coll AS key='value'")
    node1.query("GRANT SHOW NAMED COLLECTIONS ON recreate_coll TO recreate_user")

    assert node1.query("SELECT name FROM system.named_collections", user="recreate_user").strip() == "recreate_coll"

    node1.query("DROP USER recreate_user")
    node1.query("CREATE USER recreate_user")
    node1.query("GRANT SELECT ON *.* TO recreate_user")
    assert node1.query("SELECT count() FROM system.named_collections", user="recreate_user").strip() == "0"

    node1.query("GRANT SHOW NAMED COLLECTIONS ON recreate_coll TO recreate_user")
    assert node1.query("SELECT count() FROM system.named_collections", user="recreate_user").strip() == "1"

    node1.query("DROP USER recreate_user")


def test_role_grants_survive_user_recreation(cluster):
    node1 = cluster.instances["node1"]

    node1.query("DROP USER IF EXISTS role_user")
    node1.query("DROP ROLE IF EXISTS nc_role")

    node1.query("CREATE NAMED COLLECTION role_coll AS data='important'")
    node1.query("CREATE ROLE nc_role")
    node1.query("GRANT SHOW NAMED COLLECTIONS ON role_coll TO nc_role")
    node1.query("GRANT ALTER NAMED COLLECTION ON role_coll TO nc_role")

    node1.query("CREATE USER role_user")
    node1.query("GRANT SELECT ON *.* TO role_user")
    node1.query("GRANT nc_role TO role_user")

    assert node1.query("SELECT name FROM system.named_collections", user="role_user").strip() == "role_coll"
    node1.query("ALTER NAMED COLLECTION role_coll SET data='changed'", user="role_user")

    node1.query("DROP USER role_user")
    node1.query("CREATE USER role_user")
    node1.query("GRANT SELECT ON *.* TO role_user")
    node1.query("GRANT nc_role TO role_user")

    assert node1.query("SELECT name FROM system.named_collections", user="role_user").strip() == "role_coll"
    node1.query("ALTER NAMED COLLECTION role_coll SET data='after_recreate'", user="role_user")
    assert node1.query(
        "SELECT collection['data'] FROM system.named_collections WHERE name='role_coll'").strip() == "after_recreate"

    node1.restart_clickhouse()

    assert node1.query("SELECT name FROM system.named_collections", user="role_user").strip() == "role_coll"

    node1.query("DROP USER role_user")
    node1.query("DROP ROLE nc_role")


def test_special_characters_and_unicode(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]
    zk = cluster.get_kazoo_client("zoo1")

    special = "p@ss!#$%^&*()_+-=[]{}|;:,./<>?"
    unicode_val = "こんにちは世界"

    node1.query(f"CREATE NAMED COLLECTION special_test AS special='{special}', unicode='{unicode_val}'")
    assert wait_for_value(node2, "special_test", "special", special)
    assert node2.query(
        "SELECT collection['unicode'] FROM system.named_collections WHERE name='special_test'").strip() == unicode_val

    content = check_encrypted(zk, "special_test")
    assert special.encode() not in content


def test_empty_and_long_values(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    long_val = "x" * 10000

    node1.query(f"CREATE NAMED COLLECTION length_test AS empty='', long='{long_val}'")
    assert wait_exists(node2, "length_test")

    assert node1.query(
        "SELECT collection['empty'] FROM system.named_collections WHERE name='length_test'").strip() == ""
    assert node1.query(
        "SELECT length(collection['long']) FROM system.named_collections WHERE name='length_test'").strip() == "10000"


def test_numeric_values(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query(
        "CREATE NAMED COLLECTION num_test AS int_val=12345, neg=-9999, float_val=3.14, zero=0, large=9999999999999")

    assert wait_for_value(node2, "num_test", "int_val", "12345")
    assert node1.query(
        "SELECT collection['neg'] FROM system.named_collections WHERE name='num_test'").strip() == "-9999"
    assert node1.query(
        "SELECT collection['float_val'] FROM system.named_collections WHERE name='num_test'").strip() == "3.14"


def test_many_keys(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]
    zk = cluster.get_kazoo_client("zoo1")

    keys_values = ", ".join([f"key{i}='val{i}'" for i in range(100)])
    node1.query(f"CREATE NAMED COLLECTION many_keys_test AS {keys_values}")

    assert node1.query(
        "SELECT length(mapKeys(collection)) FROM system.named_collections WHERE name='many_keys_test'").strip() == "100"
    assert wait_for_value(node2, "many_keys_test", "key99", "val99")

    content = check_encrypted(zk, "many_keys_test")
    assert b"val50" not in content


def test_ddl_collection_basic(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION ddl_coll1 AS key1 = 'value1', key2 = 'value2'")

    for node in [node1, node2]:
        assert wait_exists(node, "ddl_coll1")
        assert node.query(
            "SELECT collection['key1'] FROM system.named_collections WHERE name='ddl_coll1'").strip() == "value1"
        assert node.query("SELECT source FROM system.named_collections WHERE name='ddl_coll1'").strip() == "SQL"

    node1.query("ALTER NAMED COLLECTION ddl_coll1 SET key2 = 'modified'")
    assert wait_for_value(node2, "ddl_coll1", "key2", "modified")


def test_secrets_hidden_in_logs(cluster):
    node1 = cluster.instances["node1"]

    qid = f"q_{uuid.uuid4()}"
    node1.query("CREATE NAMED COLLECTION log_test AS password='super_secret'", query_id=qid)
    node1.query("SYSTEM FLUSH LOGS")

    query_text = node1.query(
        f"SELECT query FROM system.query_log WHERE query_id='{qid}' AND type='QueryFinish' LIMIT 1").strip()
    assert "super_secret" not in query_text
    assert "[HIDDEN]" in query_text


def test_zk_node_lifecycle(cluster):
    node1 = cluster.instances["node1"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("CREATE NAMED COLLECTION zk_test AS key='val'")

    assert "zk_test.sql" in zk.get_children(ZK_PATH)

    node1.query("DROP NAMED COLLECTION zk_test")
    assert wait_not_exists(node1, "zk_test")
    assert "zk_test.sql" not in zk.get_children(ZK_PATH)


def test_remote_select_insert(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node2.query("DROP TABLE IF EXISTS remote_test_table")
    node2.query("CREATE TABLE remote_test_table (id UInt64, val String) ENGINE=MergeTree() ORDER BY id")
    node2.query("INSERT INTO remote_test_table VALUES (1,'a'), (2,'b'), (3,'c')")

    node1.query(
        "CREATE NAMED COLLECTION remote_coll AS host='node2', port=9000, user='default', password='', database='default'")
    assert wait_exists(node2, "remote_coll")

    result = node1.query("SELECT count() FROM remote(remote_coll, table='remote_test_table')").strip()
    assert result == "3"

    node1.query("INSERT INTO FUNCTION remote(remote_coll, table='remote_test_table') VALUES (4,'d')")
    assert node2.query("SELECT count() FROM remote_test_table").strip() == "4"

    node2.query("DROP TABLE remote_test_table")


def test_dictionary_with_named_collection(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node2.query("DROP TABLE IF EXISTS dict_source")
    node2.query("CREATE TABLE dict_source (id UInt64, name String) ENGINE=MergeTree() ORDER BY id")
    node2.query("INSERT INTO dict_source VALUES (1,'one'), (2,'two'), (3,'three')")

    node1.query(
        "CREATE NAMED COLLECTION dict_coll AS host='node2', port=9000, user='default', password='', db='default', table='dict_source'")

    node1.query("DROP DICTIONARY IF EXISTS test_dict")
    node1.query("""
        CREATE DICTIONARY test_dict (id UInt64, name String) PRIMARY KEY id
        SOURCE(CLICKHOUSE(NAME dict_coll)) LAYOUT(FLAT()) LIFETIME(0)
    """)

    assert node1.query("SELECT dictGet('test_dict', 'name', toUInt64(2))").strip() == "two"

    node1.query("DROP DICTIONARY test_dict")
    node2.query("DROP TABLE dict_source")


def test_concurrent_creates(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION conc1 AS from='node1'")
    node2.query("CREATE NAMED COLLECTION conc2 AS from='node2'")

    assert wait_exists(node2, "conc1")
    assert wait_exists(node1, "conc2")


def test_rapid_alters(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]

    node1.query("CREATE NAMED COLLECTION rapid_alter AS counter='0'")

    for i in range(1, 11):
        node1.query(f"ALTER NAMED COLLECTION rapid_alter SET counter='{i}'")

    assert node1.query(
        "SELECT collection['counter'] FROM system.named_collections WHERE name='rapid_alter'").strip() == "10"
    assert wait_for_value(node2, "rapid_alter", "counter", "10")


def test_survives_restart(cluster):
    node1, node2 = cluster.instances["node1"], cluster.instances["node2"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("CREATE NAMED COLLECTION restart_test AS key='persistent'")
    assert wait_for_value(node2, "restart_test", "key", "persistent")

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    for node in [node1, node2]:
        assert node.query(
            "SELECT collection['key'] FROM system.named_collections WHERE name='restart_test'").strip() == "persistent"

    content = check_encrypted(zk, "restart_test")
    assert b"persistent" not in content


def test_new_replica_sees_existing_collections(stopped_node3):
    cluster = stopped_node3
    node1, node2, node3 = cluster.instances["node1"], cluster.instances["node2"], cluster.instances["node3"]

    node1.query("CREATE NAMED COLLECTION existing_coll_1 AS host='server1', secret='abc'")
    node2.query("CREATE NAMED COLLECTION existing_coll_2 AS endpoint='http://api.com', token='xyz'")

    assert wait_exists(node2, "existing_coll_1")

    node3.start_clickhouse()

    assert wait_exists(node3, "existing_coll_1", timeout=30)
    assert wait_exists(node3, "existing_coll_2", timeout=30)
    assert wait_for_value(node3, "existing_coll_1", "secret", "abc", timeout=30)
    assert wait_for_value(node3, "existing_coll_2", "token", "xyz", timeout=30)


def test_new_replica_executes_remote_queries(stopped_node3):
    cluster = stopped_node3
    node1, node2, node3 = cluster.instances["node1"], cluster.instances["node2"], cluster.instances["node3"]

    node2.query("DROP TABLE IF EXISTS query_test_data")

    node2.query("CREATE TABLE query_test_data (id UInt64, val String, score Int32) ENGINE=MergeTree() ORDER BY id")
    node2.query("INSERT INTO query_test_data VALUES (1,'alpha',100), (2,'beta',200), (3,'gamma',300)")
    node1.query(
        "CREATE NAMED COLLECTION query_coll AS host='node2', port=9000, user='default', password='', database='default', table='query_test_data'")

    assert node1.query("SELECT count() FROM remote(query_coll)").strip() == "3"

    node3.start_clickhouse()
    assert wait_exists(node3, "query_coll", timeout=30)

    assert node3.query("SELECT count() FROM remote(query_coll)").strip() == "3"
    assert node3.query("SELECT sum(score) FROM remote(query_coll)").strip() == "600"

    node2.query("DROP TABLE query_test_data")


def test_new_replica_sees_final_state_after_alterations(stopped_node3):
    cluster = stopped_node3
    node1, node3 = cluster.instances["node1"], cluster.instances["node3"]

    node1.query("CREATE NAMED COLLECTION altered_coll AS a='1', b='2', c='3'")
    node1.query("ALTER NAMED COLLECTION altered_coll SET a='modified_a'")
    node1.query("ALTER NAMED COLLECTION altered_coll DELETE b")
    node1.query("ALTER NAMED COLLECTION altered_coll SET d='added_d'")

    node3.start_clickhouse()

    assert wait_for_value(node3, "altered_coll", "a", "modified_a", timeout=30)
    assert wait_for_value(node3, "altered_coll", "d", "added_d", timeout=30)

    keys = node3.query("SELECT mapKeys(collection) FROM system.named_collections WHERE name='altered_coll'").strip()
    assert "b" not in keys


def test_new_replica_can_create_new_collections(stopped_node3):
    cluster = stopped_node3
    node1, node2, node3 = cluster.instances["node1"], cluster.instances["node2"], cluster.instances["node3"]

    node1.query("CREATE NAMED COLLECTION pre_existing_coll AS key1 = 'value1'")
    assert wait_exists(node2, "pre_existing_coll")

    node3.start_clickhouse()
    assert wait_exists(node3, "pre_existing_coll", timeout=30)

    node3.query("CREATE NAMED COLLECTION new_from_replica AS created_by='node3', timestamp='12345'")

    assert wait_exists(node1, "new_from_replica")
    assert wait_exists(node2, "new_from_replica")
    assert wait_for_value(node1, "new_from_replica", "created_by", "node3")


def test_new_replica_can_drop_collections(stopped_node3):
    cluster = stopped_node3
    node1, node2, node3 = cluster.instances["node1"], cluster.instances["node2"], cluster.instances["node3"]

    node1.query("CREATE NAMED COLLECTION to_be_dropped AS key='value'")
    assert wait_exists(node2, "to_be_dropped")

    node3.start_clickhouse()
    assert wait_exists(node3, "to_be_dropped", timeout=30)

    node3.query("DROP NAMED COLLECTION to_be_dropped")

    assert wait_not_exists(node1, "to_be_dropped")
    assert wait_not_exists(node2, "to_be_dropped")


def test_new_replica_encrypted_data_integrity(stopped_node3):
    cluster = stopped_node3
    node1, node3 = cluster.instances["node1"], cluster.instances["node3"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("""
        CREATE NAMED COLLECTION encrypted_coll AS
        api_key='super_secret_api_key_12345',
        password='P@ssw0rd!Complex#123'
    """)

    content = zk.get(f"{ZK_PATH}/encrypted_coll.sql")[0]
    assert content[:3] == b"ENC"
    assert b"super_secret_api_key_12345" not in content

    node3.start_clickhouse()

    assert wait_for_value(node3, "encrypted_coll", "api_key", "super_secret_api_key_12345", timeout=30)
    assert wait_for_value(node3, "encrypted_coll", "password", "P@ssw0rd!Complex#123", timeout=30)


def test_new_replica_with_many_collections(stopped_node3):
    cluster = stopped_node3
    node1, node2, node3 = cluster.instances["node1"], cluster.instances["node2"], cluster.instances["node3"]

    for i in range(20):
        node = node1 if i % 2 == 0 else node2
        node.query(f"CREATE NAMED COLLECTION batch_coll_{i} AS idx='{i}'")

    assert wait_exists(node2, "batch_coll_0")

    node3.start_clickhouse()

    assert wait_exists(node3, "batch_coll_19", timeout=30)
    count = int(node3.query("SELECT count() FROM system.named_collections WHERE name LIKE 'batch_coll_%'").strip())
    assert count == 20


def test_new_replica_join_leave_rejoin(stopped_node3):
    cluster = stopped_node3
    node1, node3 = cluster.instances["node1"], cluster.instances["node3"]

    node1.query("CREATE NAMED COLLECTION rejoin_coll AS version='1'")

    node3.start_clickhouse()
    assert wait_for_value(node3, "rejoin_coll", "version", "1", timeout=30)

    node3.stop_clickhouse()
    node1.query("ALTER NAMED COLLECTION rejoin_coll SET version='2', extra='added'")

    node3.start_clickhouse()
    assert wait_for_value(node3, "rejoin_coll", "version", "2", timeout=30)
    assert wait_for_value(node3, "rejoin_coll", "extra", "added", timeout=30)

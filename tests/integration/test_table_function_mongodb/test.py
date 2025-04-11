import pymongo
import pytest
import urllib

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mongo_pass


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            with_mongo=True,
            main_configs=["configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_mongo_connection(started_cluster, secure=False, with_credentials=True):
    if secure:
        return pymongo.MongoClient(
            f"mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@localhost:{started_cluster.mongo_secure_port}/?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true"
        )
    if with_credentials:
        return pymongo.MongoClient(
            f"mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@localhost:{started_cluster.mongo_port}"
        )

    return pymongo.MongoClient(
        "mongodb://localhost:{}".format(started_cluster.mongo_no_cred_port)
    )


def test_simple_select(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    assert (
        node.query(
            f"SELECT COUNT() FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', '{mongo_pass}', structure='key UInt64, data String')"
        )
        == "100\n"
    )
    assert (
        node.query(
            f"SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', '{mongo_pass}', structure='key UInt64, data String')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            f"SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', '{mongo_pass}', 'key UInt64, data String')"
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            f"SELECT data FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', '{mongo_pass}', structure='key UInt64, data String') WHERE key = 42"
        )
        == hex(42 * 42) + "\n"
    )
    simple_mongo_table.drop()


def test_simple_select_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    assert (
        node.query(
            f"SELECT COUNT() FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo1:27017/test', 'simple_table', structure='key UInt64, data String')"
        )
        == "100\n"
    )
    assert (
        node.query(
            f"SELECT sum(key) FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo1:27017/test', 'simple_table', structure='key UInt64, data String')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            f"SELECT sum(key) FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo1:27017/test', 'simple_table', 'key UInt64, data String')"
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            f"SELECT data FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo1:27017/test', 'simple_table', structure='key UInt64, data String') WHERE key = 42"
        )
        == hex(42 * 42) + "\n"
    )
    simple_mongo_table.drop()


def test_complex_data_type(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    incomplete_mongo_table = db["complex_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i), "dict": {"a": i, "b": str(i)}})
    incomplete_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]

    assert (
        node.query(
            f"""
            SELECT COUNT()
            FROM mongodb('mongo1:27017',
                         'test',
                         'complex_table',
                         'root',
                         '{mongo_pass}',
                         structure='key UInt64, data String, dict Map(UInt64, String)')"""
        )
        == "100\n"
    )
    assert (
        node.query(
            f"""
            SELECT sum(key)
            FROM mongodb('mongo1:27017',
                         'test',
                         'complex_table',
                         'root',
                         '{mongo_pass}',
                         structure='key UInt64, data String, dict Map(UInt64, String)')"""
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            f"""
            SELECT data
            FROM mongodb('mongo1:27017',
                         'test',
                         'complex_table',
                         'root',
                         '{mongo_pass}',
                         structure='key UInt64, data String, dict Map(UInt64, String)')
            WHERE key = 42
            """
        )
        == hex(42 * 42) + "\n"
    )
    incomplete_mongo_table.drop()


def test_incorrect_data_type(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    strange_mongo_table = db["strange_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i), "aaaa": "Hello"})
    strange_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]

    with pytest.raises(QueryRuntimeException):
        node.query(
            f"SELECT aaaa FROM mongodb('mongo1:27017', 'test', 'strange_table', 'root', '{mongo_pass}', structure='key UInt64, data String')"
        )

    strange_mongo_table.drop()


def test_secure_connection(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]

    assert (
        node.query(
            f"""SELECT COUNT()
               FROM mongodb('mongo_secure:27017',
                            'test',
                            'simple_table',
                            'root',
                            '{mongo_pass}',
                            structure='key UInt64, data String',
                            options='tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true')"""
        )
        == "100\n"
    )
    assert (
        node.query(
            f"""SELECT sum(key)
               FROM mongodb('mongo_secure:27017',
                            'test',
                            'simple_table',
                            'root',
                            '{mongo_pass}',
                            structure='key UInt64, data String',
                            options='tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true')"""
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            f"""SELECT sum(key)
               FROM mongodb('mongo_secure:27017',
                            'test',
                            'simple_table',
                            'root',
                            '{mongo_pass}',
                            'key UInt64, data String',
                            'tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true')"""
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            f"""SELECT data
               FROM mongodb('mongo_secure:27017',
                            'test',
                            'simple_table',
                            'root',
                            '{mongo_pass}',
                            'key UInt64, data String',
                            'tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true')
               WHERE key = 42"""
        )
        == hex(42 * 42) + "\n"
    )
    simple_mongo_table.drop()


def test_secure_connection_with_validation(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""SELECT COUNT() FROM mongodb('mongo_secure:27017',
                   'test',
                   'simple_table',
                   'root',
                   '{mongo_pass}',
                   structure='key UInt64, data String',
                   options='tls=true')"""
        )

    simple_mongo_table.drop()


def test_secure_connection_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]

    assert (
        node.query(
            f"""SELECT COUNT()
               FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo_secure:27017/test?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true',
                            'simple_table',
                            'key UInt64, data String')"""
        )
        == "100\n"
    )
    assert (
        node.query(
            f"""SELECT sum(key)
               FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo_secure:27017/test?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true',
                            'simple_table',
                            'key UInt64, data String')"""
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            f"""SELECT sum(key)
               FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo_secure:27017/test?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true',
                            'simple_table',
                            'key UInt64, data String')"""
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            f"""SELECT data
               FROM mongodb('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo_secure:27017/test?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true',
                            'simple_table',
                            'key UInt64, data String')
               WHERE key = 42"""
        )
        == hex(42 * 42) + "\n"
    )
    simple_mongo_table.drop()


def test_no_credentials(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    db = mongo_connection["test"]
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    assert (
        node.query(
            "SELECT count() FROM mongodb('mongo_no_cred:27017', 'test', 'simple_table', '', '', structure='key UInt64, data String')"
        )
        == "100\n"
    )
    simple_mongo_table.drop()


def test_auth_source(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    admin_db = mongo_connection["admin"]
    admin_db.add_user(
        "root",
        mongo_pass,
        roles=[{"role": "userAdminAnyDatabase", "db": "admin"}, "readWriteAnyDatabase"],
    )
    simple_mongo_table = admin_db["simple_table"]
    data = []
    for i in range(0, 50):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    db = mongo_connection["test"]
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"SELECT count() FROM mongodb('mongo_no_cred:27017', 'test', 'simple_table', 'root', '{mongo_pass}', structure='key UInt64, data String')"
        )

    assert (
        node.query(
            f"SELECT count() FROM mongodb('mongo_no_cred:27017', 'test', 'simple_table', 'root', '{mongo_pass}', structure='key UInt64, data String', options='authSource=admin')"
        )
        == "100\n"
    )

    simple_mongo_table.drop()


def test_missing_columns(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", mongo_pass)
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 10):
        data.append({"key": i, "data": hex(i * i)})
    for i in range(0, 10):
        data.append({"key": i})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    result = node.query(
        f"SELECT count() FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', '{mongo_pass}', structure='key UInt64, data Nullable(String)') WHERE isNull(data)"
    )
    assert result == "10\n"
    simple_mongo_table.drop()


def test_oid(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    oid_mongo_table = db["oid_table"]
    inserted_result = oid_mongo_table.insert_many(
        [
            {"key": "oid1"},
            {"key": "oid2"},
            {"key": "oid3"},
        ]
    )
    ids = inserted_result.inserted_ids

    node = started_cluster.instances["node"]
    table_definitions = [
        "mongodb('mongo1:27017', 'test', 'oid_table', 'root', 'clickhouse', '_id String, key String')",
        "mongodb('mongodb://root:clickhouse@mongo1:27017/test', 'oid_table', '_id String, key String')",
    ]

    for table_definition in table_definitions:
        assert node.query(f"SELECT COUNT() FROM {table_definition}") == "3\n"

        assert node.query(f"SELECT _id FROM {table_definition} WHERE _id = '{str(ids[0])}'") == f"{str(ids[0])}\n"
        assert node.query(f"SELECT key FROM {table_definition} WHERE _id = '{str(ids[0])}'") == "oid1\n"
        assert (node.query(f"SELECT key FROM {table_definition} WHERE _id != '{str(ids[0])}' ORDER BY key") ==
                "oid2\noid3\n")
        assert (node.query(f"SELECT key FROM {table_definition} WHERE _id in ['{ids[0]}', '{ids[1]}'] ORDER BY key")
                == "oid1\noid2\n")
        assert (node.query(f"SELECT key FROM {table_definition} WHERE _id not in ['{ids[0]}', '{ids[1]}'] ORDER BY key")
                == "oid3\n")

        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT * FROM {table_definition} WHERE _id = 'not-oid'")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT * FROM {table_definition} WHERE _id != 'not-oid'")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT * FROM {table_definition} WHERE _id = 1234567")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT * FROM {table_definition} WHERE _id != 1234567")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT key FROM {table_definition} WHERE _id in ['{ids[0]}', 'not-oid']")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT key FROM {table_definition} WHERE _id not in ['{ids[0]}', 'not-oid']")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT key FROM {table_definition} WHERE _id in ['nope', 'not-oid']")
        with pytest.raises(QueryRuntimeException):
            node.query(f"SELECT key FROM {table_definition} WHERE _id not in ['nope', 'not-oid']")

    table_definition = ("mongodb('mongo1:27017', 'test', 'oid_table', 'root', 'clickhouse', '_id String, key String', "
                        "'', 'key)")
    with pytest.raises(QueryRuntimeException):
        node.query(f"SELECT * FROM {table_definition} WHERE key = 'not-oid'")

    table_definition = ("mongodb('mongodb://root:clickhouse@mongo1:27017/test', 'oid_table', '_id String, key String', "
                        "'key')")
    with pytest.raises(QueryRuntimeException):
        node.query(f"SELECT * FROM {table_definition} WHERE key = 'not-oid'")


    oid_mongo_table.drop()

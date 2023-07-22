import pymongo

import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            "node",
            main_configs=[
                "configs_secure/config.d/ssl_conf.xml",
                "configs/named_collections.xml",
            ],
            with_mongo=True,
            with_mongo_secure=request.param,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_mongo_connection(started_cluster, secure=False, with_credentials=True):
    connection_str = ""
    if with_credentials:
        connection_str = "mongodb://root:clickhouse@localhost:{}".format(
            started_cluster.mongo_port
        )
    else:
        connection_str = "mongodb://localhost:{}".format(
            started_cluster.mongo_no_cred_port
        )
    if secure:
        connection_str += "/?tls=true&tlsAllowInvalidCertificates=true"
    return pymongo.MongoClient(connection_str)


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_simple_select(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse')"
    )

    assert node.query("SELECT COUNT() FROM simple_mongo_table") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM simple_mongo_table")
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query("SELECT data from simple_mongo_table where key = 42")
        == hex(42 * 42) + "\n"
    )
    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_complex_data_type(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    incomplete_mongo_table = db["complex_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i), "dict": {"a": i, "b": str(i)}})
    incomplete_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE incomplete_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'complex_table', 'root', 'clickhouse')"
    )

    assert node.query("SELECT COUNT() FROM incomplete_mongo_table") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM incomplete_mongo_table")
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query("SELECT data from incomplete_mongo_table where key = 42")
        == hex(42 * 42) + "\n"
    )
    node.query("DROP TABLE incomplete_mongo_table")
    incomplete_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_incorrect_data_type(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    strange_mongo_table = db["strange_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i), "aaaa": "Hello"})
    strange_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE strange_mongo_table(key String, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'strange_table', 'root', 'clickhouse')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM strange_mongo_table")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT uniq(key) FROM strange_mongo_table")

    node.query(
        "CREATE TABLE strange_mongo_table2(key UInt64, data String, bbbb String) ENGINE = MongoDB('mongo1:27017', 'test', 'strange_table', 'root', 'clickhouse')"
    )

    node.query("DROP TABLE strange_mongo_table")
    node.query("DROP TABLE strange_mongo_table2")
    strange_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [True], indirect=["started_cluster"])
def test_secure_connection(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', 'ssl=true')"
    )

    assert node.query("SELECT COUNT() FROM simple_mongo_table") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM simple_mongo_table")
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query("SELECT data from simple_mongo_table where key = 42")
        == hex(42 * 42) + "\n"
    )
    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_predefined_connection_configuration(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query("drop table if exists simple_mongo_table")
    node.query(
        "create table simple_mongo_table(key UInt64, data String) engine = MongoDB(mongo1)"
    )
    assert node.query("SELECT count() FROM simple_mongo_table") == "100\n"
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_no_credentials(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    db = mongo_connection["test"]
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "create table simple_mongo_table_2(key UInt64, data String) engine = MongoDB('mongo2:27017', 'test', 'simple_table', '', '')"
    )
    assert node.query("SELECT count() FROM simple_mongo_table_2") == "100\n"
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_auth_source(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    admin_db = mongo_connection["admin"]
    admin_db.add_user(
        "root",
        "clickhouse",
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
    node.query(
        "create table simple_mongo_table_fail(key UInt64, data String) engine = MongoDB('mongo2:27017', 'test', 'simple_table', 'root', 'clickhouse')"
    )
    node.query_and_get_error("SELECT count() FROM simple_mongo_table_fail")
    node.query(
        "create table simple_mongo_table_ok(key UInt64, data String) engine = MongoDB('mongo2:27017', 'test', 'simple_table', 'root', 'clickhouse', 'authSource=admin')"
    )
    assert node.query("SELECT count() FROM simple_mongo_table_ok") == "100\n"
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_missing_columns(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 10):
        data.append({"key": i, "data": hex(i * i)})
    for i in range(0, 10):
        data.append({"key": i})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query("drop table if exists simple_mongo_table")
    node.query(
        "create table simple_mongo_table(key UInt64, data Nullable(String)) engine = MongoDB(mongo1)"
    )
    result = node.query("SELECT count() FROM simple_mongo_table WHERE isNull(data)")
    assert result == "10\n"
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_simple_insert_select(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    simple_mongo_table = db["simple_table"]

    node = started_cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS simple_mongo_table")
    node.query(
        "CREATE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse')"
    )
    node.query("INSERT INTO simple_mongo_table SELECT 1, 'kek'")

    assert (
        node.query("SELECT data from simple_mongo_table where key = 1").strip() == "kek"
    )
    node.query("INSERT INTO simple_mongo_table(key) SELECT 12")
    assert int(node.query("SELECT count() from simple_mongo_table")) == 2
    assert (
        node.query("SELECT data from simple_mongo_table where key = 12").strip() == ""
    )

    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()

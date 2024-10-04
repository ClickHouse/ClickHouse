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
            with_mongo=True,
            main_configs=[
                "mongo_secure_config/config.d/ssl_conf.xml",
                "configs/feature_flag.xml",
            ],
            user_configs=["configs/users.xml"],
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_mongo_connection(started_cluster, secure=False, with_credentials=True):
    connection_str = ""
    if with_credentials:
        connection_str = "mongodb://root:clickhouse@localhost:{}".format(
            started_cluster.mongo_secure_port if secure else started_cluster.mongo_port
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

    node = started_cluster.instances["node"]
    for i in range(0, 100):
        node.query(
            "INSERT INTO FUNCTION mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String') (key, data) VALUES ({}, '{}')".format(
                i, hex(i * i)
            )
        )
    assert (
        node.query(
            "SELECT COUNT() FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String')"
        )
        == "100\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', 'key UInt64, data String')"
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            "SELECT data from mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String') where key = 42"
        )
        == hex(42 * 42) + "\n"
    )
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

    assert (
        node.query(
            "SELECT COUNT() FROM mongodb('mongo1:27017', 'test', 'complex_table', 'root', 'clickhouse', structure='key UInt64, data String, dict Map(UInt64, String)')"
        )
        == "100\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'complex_table', 'root', 'clickhouse', structure='key UInt64, data String, dict Map(UInt64, String)')"
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            "SELECT data from mongodb('mongo1:27017', 'test', 'complex_table', 'root', 'clickhouse', structure='key UInt64, data String, dict Map(UInt64, String)') where key = 42"
        )
        == hex(42 * 42) + "\n"
    )
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

    with pytest.raises(QueryRuntimeException):
        node.query(
            "SELECT aaaa FROM mongodb('mongo1:27017', 'test', 'strange_table', 'root', 'clickhouse', structure='key UInt64, data String')"
        )

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

    assert (
        node.query(
            "SELECT COUNT() FROM mongodb('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='ssl=true')"
        )
        == "100\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='ssl=true')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', 'key UInt64, data String', 'ssl=true')"
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            "SELECT data from mongodb('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='ssl=true') where key = 42"
        )
        == hex(42 * 42) + "\n"
    )
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
    assert (
        node.query(
            "SELECT count() FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String')"
        )
        == "100\n"
    )
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
    assert (
        node.query(
            "SELECT count() FROM mongodb('mongo_no_cred:27017', 'test', 'simple_table', '', '', structure='key UInt64, data String')"
        )
        == "100\n"
    )
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

    node.query_and_get_error(
        "SELECT count() FROM mongodb('mongo_no_cred:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String')"
    )

    assert (
        node.query(
            "SELECT count() FROM mongodb('mongo_no_cred:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='authSource=admin')"
        )
        == "100\n"
    )
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
    result = node.query(
        "SELECT count() FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data Nullable(String)') WHERE isNull(data)"
    )
    assert result == "10\n"
    simple_mongo_table.drop()

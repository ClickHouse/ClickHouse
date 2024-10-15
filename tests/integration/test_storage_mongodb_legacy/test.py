import datetime
from uuid import UUID

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
                "mongo_secure_config/config.d/ssl_conf.xml",
                "configs/named_collections.xml",
                "configs/feature_flag.xml",
            ],
            user_configs=["configs/users.xml"],
            with_mongo=True,
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
def test_uuid(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    mongo_table = db["uuid_table"]
    mongo_table.insert({"key": 0, "data": UUID("f0e77736-91d1-48ce-8f01-15123ca1c7ed")})

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE uuid_mongo_table(key UInt64, data UUID) ENGINE = MongoDB('mongo1:27017', 'test', 'uuid_table', 'root', 'clickhouse')"
    )

    assert node.query("SELECT COUNT() FROM uuid_mongo_table") == "1\n"
    assert (
        node.query("SELECT data from uuid_mongo_table where key = 0")
        == "f0e77736-91d1-48ce-8f01-15123ca1c7ed\n"
    )
    node.query("DROP TABLE uuid_mongo_table")
    mongo_table.drop()


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
def test_simple_select_from_view(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    simple_mongo_table_view = db.create_collection(
        "simple_table_view", viewOn="simple_table"
    )

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table_view', 'root', 'clickhouse')"
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
    simple_mongo_table_view.drop()
    simple_mongo_table.drop()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_arrays(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    arrays_mongo_table = db["arrays_table"]
    data = []
    for i in range(0, 100):
        data.append(
            {
                "key": i,
                "arr_int64": [-(i + 1), -(i + 2), -(i + 3)],
                "arr_int32": [-(i + 1), -(i + 2), -(i + 3)],
                "arr_int16": [-(i + 1), -(i + 2), -(i + 3)],
                "arr_int8": [-(i + 1), -(i + 2), -(i + 3)],
                "arr_uint64": [i + 1, i + 2, i + 3],
                "arr_uint32": [i + 1, i + 2, i + 3],
                "arr_uint16": [i + 1, i + 2, i + 3],
                "arr_uint8": [i + 1, i + 2, i + 3],
                "arr_float32": [i + 1.125, i + 2.5, i + 3.750],
                "arr_float64": [i + 1.125, i + 2.5, i + 3.750],
                "arr_date": [
                    datetime.datetime(2002, 10, 27),
                    datetime.datetime(2024, 1, 8),
                ],
                "arr_datetime": [
                    datetime.datetime(2023, 3, 31, 6, 3, 12),
                    datetime.datetime(1999, 2, 28, 12, 46, 34),
                ],
                "arr_string": [str(i + 1), str(i + 2), str(i + 3)],
                "arr_uuid": [
                    "f0e77736-91d1-48ce-8f01-15123ca1c7ed",
                    "93376a07-c044-4281-a76e-ad27cf6973c5",
                ],
                "arr_mongo_uuid": [
                    UUID("f0e77736-91d1-48ce-8f01-15123ca1c7ed"),
                    UUID("93376a07-c044-4281-a76e-ad27cf6973c5"),
                ],
                "arr_arr_bool": [
                    [True, False, True],
                    [True],
                    [],
                    None,
                    [False],
                    [None],
                ],
                "arr_empty": [],
                "arr_null": None,
                "arr_nullable": None,
            }
        )

    arrays_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE arrays_mongo_table("
        "key UInt64,"
        "arr_int64 Array(Int64),"
        "arr_int32 Array(Int32),"
        "arr_int16 Array(Int16),"
        "arr_int8 Array(Int8),"
        "arr_uint64 Array(UInt64),"
        "arr_uint32 Array(UInt32),"
        "arr_uint16 Array(UInt16),"
        "arr_uint8 Array(UInt8),"
        "arr_float32 Array(Float32),"
        "arr_float64 Array(Float64),"
        "arr_date Array(Date),"
        "arr_datetime Array(DateTime),"
        "arr_string Array(String),"
        "arr_uuid Array(UUID),"
        "arr_mongo_uuid Array(UUID),"
        "arr_arr_bool Array(Array(Bool)),"
        "arr_empty Array(UInt64),"
        "arr_null Array(UInt64),"
        "arr_arr_null Array(Array(UInt64)),"
        "arr_nullable Array(Nullable(UInt64))"
        ") ENGINE = MongoDB('mongo1:27017', 'test', 'arrays_table', 'root', 'clickhouse')"
    )

    assert node.query("SELECT COUNT() FROM arrays_mongo_table") == "100\n"

    for column_name in ["arr_int64", "arr_int32", "arr_int16", "arr_int8"]:
        assert (
            node.query(f"SELECT {column_name} FROM arrays_mongo_table WHERE key = 42")
            == "[-43,-44,-45]\n"
        )

    for column_name in ["arr_uint64", "arr_uint32", "arr_uint16", "arr_uint8"]:
        assert (
            node.query(f"SELECT {column_name} FROM arrays_mongo_table WHERE key = 42")
            == "[43,44,45]\n"
        )

    for column_name in ["arr_float32", "arr_float64"]:
        assert (
            node.query(f"SELECT {column_name} FROM arrays_mongo_table WHERE key = 42")
            == "[43.125,44.5,45.75]\n"
        )

    assert (
        node.query(f"SELECT arr_date FROM arrays_mongo_table WHERE key = 42")
        == "['2002-10-27','2024-01-08']\n"
    )

    assert (
        node.query(f"SELECT arr_datetime FROM arrays_mongo_table WHERE key = 42")
        == "['2023-03-31 06:03:12','1999-02-28 12:46:34']\n"
    )

    assert (
        node.query(f"SELECT arr_string FROM arrays_mongo_table WHERE key = 42")
        == "['43','44','45']\n"
    )

    assert (
        node.query(f"SELECT arr_uuid FROM arrays_mongo_table WHERE key = 42")
        == "['f0e77736-91d1-48ce-8f01-15123ca1c7ed','93376a07-c044-4281-a76e-ad27cf6973c5']\n"
    )

    assert (
        node.query(f"SELECT arr_mongo_uuid FROM arrays_mongo_table WHERE key = 42")
        == "['f0e77736-91d1-48ce-8f01-15123ca1c7ed','93376a07-c044-4281-a76e-ad27cf6973c5']\n"
    )

    assert (
        node.query(f"SELECT arr_arr_bool FROM arrays_mongo_table WHERE key = 42")
        == "[[true,false,true],[true],[],[],[false],[false]]\n"
    )

    assert (
        node.query(f"SELECT arr_empty FROM arrays_mongo_table WHERE key = 42") == "[]\n"
    )

    assert (
        node.query(f"SELECT arr_null FROM arrays_mongo_table WHERE key = 42") == "[]\n"
    )

    assert (
        node.query(f"SELECT arr_arr_null FROM arrays_mongo_table WHERE key = 42")
        == "[]\n"
    )

    assert (
        node.query(f"SELECT arr_nullable FROM arrays_mongo_table WHERE key = 42")
        == "[]\n"
    )

    # Test INSERT SELECT
    node.query("INSERT INTO arrays_mongo_table SELECT * FROM arrays_mongo_table")

    assert node.query("SELECT COUNT() FROM arrays_mongo_table") == "200\n"
    assert node.query("SELECT COUNT(DISTINCT *) FROM arrays_mongo_table") == "100\n"

    node.query("DROP TABLE arrays_mongo_table")
    arrays_mongo_table.drop()


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
        "CREATE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', 'ssl=true')"
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
        f"create table simple_mongo_table_2(key UInt64, data String) engine = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', '', '')"
    )
    assert node.query("SELECT count() FROM simple_mongo_table_2") == "100\n"
    simple_mongo_table.drop()
    node.query("DROP TABLE IF EXISTS simple_mongo_table_2")


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_auth_source(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    admin_db = mongo_connection["admin"]
    admin_db.add_user(
        "root",
        "clickhouse",
        roles=[{"role": "userAdminAnyDatabase", "db": "admin"}, "readWriteAnyDatabase"],
    )
    simple_mongo_table_admin = admin_db["simple_table"]
    data = []
    for i in range(0, 50):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table_admin.insert_many(data)

    db = mongo_connection["test"]
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "create table simple_mongo_table_fail(key UInt64, data String) engine = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', 'root', 'clickhouse')"
    )
    node.query_and_get_error("SELECT count() FROM simple_mongo_table_fail")
    node.query(
        "create table simple_mongo_table_ok(key UInt64, data String) engine = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', 'root', 'clickhouse', 'authSource=admin')"
    )
    assert node.query("SELECT count() FROM simple_mongo_table_ok") == "100\n"
    simple_mongo_table.drop()
    simple_mongo_table_admin.drop()
    node.query("DROP TABLE IF EXISTS simple_mongo_table_ok")
    node.query("DROP TABLE IF EXISTS simple_mongo_table_fail")


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
    node.query("DROP TABLE IF EXISTS simple_mongo_table")


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
    node.query(
        "INSERT INTO simple_mongo_table SELECT number, 'kek' || toString(number) FROM numbers(10)"
    )

    assert (
        node.query("SELECT data from simple_mongo_table where key = 7").strip()
        == "kek7"
    )
    node.query("INSERT INTO simple_mongo_table(key) SELECT 12")
    assert int(node.query("SELECT count() from simple_mongo_table")) == 11
    assert (
        node.query("SELECT data from simple_mongo_table where key = 12").strip() == ""
    )

    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()

import datetime
import json

import bson
import pymongo
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
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
    if secure:
        return pymongo.MongoClient(
            "mongodb://root:clickhouse@localhost:{}/?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true".format(
                started_cluster.mongo_secure_port
            )
        )
    if with_credentials:
        return pymongo.MongoClient(
            "mongodb://root:clickhouse@localhost:{}".format(started_cluster.mongo_port)
        )

    return pymongo.MongoClient(
        "mongodb://localhost:{}".format(started_cluster.mongo_no_cred_port)
    )


def test_simple_select(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse')"
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


def test_simple_select_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_table_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://root:clickhouse@mongo1:27017/test', 'simple_table_uri')"
    )

    assert node.query("SELECT COUNT() FROM simple_table_uri") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM simple_table_uri")
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query("SELECT data from simple_table_uri where key = 42")
        == hex(42 * 42) + "\n"
    )

    node.query("DROP TABLE simple_table_uri")
    simple_mongo_table.drop()


def test_simple_select_from_view(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
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
        "CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table_view', 'root', 'clickhouse')"
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


def test_arrays(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
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
                    datetime.datetime(2024, 1, 8, 23, 59, 59),
                ],
                "arr_date32": [
                    datetime.datetime(2002, 10, 27),
                    datetime.datetime(2024, 1, 8, 23, 59, 59),
                ],
                "arr_datetime": [
                    datetime.datetime(2023, 3, 31, 6, 3, 12),
                    datetime.datetime(1999, 2, 28, 12, 46, 34),
                ],
                "arr_datetime64": [
                    datetime.datetime(2023, 3, 31, 6, 3, 12),
                    datetime.datetime(1999, 2, 28, 12, 46, 34),
                ],
                "arr_string": [str(i + 1), str(i + 2), str(i + 3)],
                "arr_uuid": [
                    "f0e77736-91d1-48ce-8f01-15123ca1c7ed",
                    "93376a07-c044-4281-a76e-ad27cf6973c5",
                ],
                "arr_arr_bool": [
                    [True, False, True],
                    [True],
                    [None],
                    [False],
                ],
                "arr_arr_bool_nullable": [
                    [True, False, True],
                    [True, None],
                    [None],
                    [False],
                ],
                "arr_empty": [],
                "arr_null": None,
                "arr_nullable": None,
            }
        )

    arrays_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE arrays_mongo_table("
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
        "arr_date32 Array(Date32),"
        "arr_datetime Array(DateTime),"
        "arr_datetime64 Array(DateTime64),"
        "arr_string Array(String),"
        "arr_uuid Array(UUID),"
        "arr_arr_bool Array(Array(Bool)),"
        "arr_arr_bool_nullable Array(Array(Nullable(Bool))),"
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
        node.query(f"SELECT arr_date32 FROM arrays_mongo_table WHERE key = 42")
        == "['2002-10-27','2024-01-08']\n"
    )
    assert (
        node.query(f"SELECT arr_datetime FROM arrays_mongo_table WHERE key = 42")
        == "['2023-03-31 06:03:12','1999-02-28 12:46:34']\n"
    )
    assert (
        node.query(f"SELECT arr_datetime64 FROM arrays_mongo_table WHERE key = 42")
        == "['2023-03-31 06:03:12.000','1999-02-28 12:46:34.000']\n"
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
        node.query(f"SELECT arr_arr_bool FROM arrays_mongo_table WHERE key = 42")
        == "[[true,false,true],[true],[false],[false]]\n"
    )
    assert (
        node.query(
            f"SELECT arr_arr_bool_nullable FROM arrays_mongo_table WHERE key = 42"
        )
        == "[[true,false,true],[true,NULL],[NULL],[false]]\n"
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

    node.query("DROP TABLE arrays_mongo_table")
    arrays_mongo_table.drop()


def test_complex_data_type(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    incomplete_mongo_table = db["complex_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i), "dict": {"a": i, "b": str(i)}})
    incomplete_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE incomplete_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'complex_table', 'root', 'clickhouse')"
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


def test_secure_connection(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', 'tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true')"
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


def test_secure_connection_with_validation(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo_secure:27017', 'test', 'simple_table', 'root', 'clickhouse', 'tls=true')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM simple_mongo_table")

    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()


def test_secure_connection_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    simple_mongo_table = db["test_secure_connection_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE test_secure_connection_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://root:clickhouse@mongo_secure:27017/test?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true', 'test_secure_connection_uri')"
    )

    assert node.query("SELECT COUNT() FROM test_secure_connection_uri") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM test_secure_connection_uri")
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query("SELECT data from test_secure_connection_uri where key = 42")
        == hex(42 * 42) + "\n"
    )

    node.query("DROP TABLE test_secure_connection_uri")
    simple_mongo_table.drop()


def test_secure_connection_uri_with_validation(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    simple_mongo_table = db["test_secure_connection_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE test_secure_connection_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://root:clickhouse@mongo_secure:27017/test?tls=true', 'test_secure_connection_uri')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM test_secure_connection_uri")

    node.query("DROP TABLE test_secure_connection_uri")

    simple_mongo_table.drop()


def test_predefined_connection_configuration(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB(mongo1)"
    )

    assert node.query("SELECT count() FROM simple_mongo_table") == "100\n"

    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()


def test_predefined_connection_configuration_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_table_uri(key UInt64, data String) ENGINE = MongoDB(mongo1_uri)"
    )

    assert node.query("SELECT count() FROM simple_table_uri") == "100\n"

    node.query("DROP TABLE simple_table_uri")
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
    node.query(
        "CREATE OR REPLACE TABLE simple_table(key UInt64, data String) ENGINE = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', '', '')"
    )

    assert node.query("SELECT count() FROM simple_table") == "100\n"

    node.query("DROP TABLE simple_table")
    simple_mongo_table.drop()


def test_no_credentials_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    db = mongo_connection["test"]
    simple_mongo_table = db["simple_table_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        "CREATE OR REPLACE TABLE simple_table_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://mongo_no_cred:27017/test', 'simple_table_uri')"
    )

    assert node.query("SELECT count() FROM simple_table_uri") == "100\n"

    node.query("DROP TABLE simple_table_uri")
    simple_mongo_table.drop()


def test_auth_source(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, with_credentials=False)
    admin_db = mongo_connection["admin"]
    admin_db.command("dropAllUsersFromDatabase")
    admin_db.command(
        "createUser",
        "root",
        pwd="clickhouse",
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
        "CREATE OR REPLACE TABLE simple_mongo_table_fail(key UInt64, data String) ENGINE = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', 'root', 'clickhouse')"
    )
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT count() FROM simple_mongo_table_fail")

    node.query(
        "CREATE OR REPLACE TABLE simple_mongo_table_ok(key UInt64, data String) ENGINE = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', 'root', 'clickhouse', 'authSource=admin')"
    )
    assert node.query("SELECT count() FROM simple_mongo_table_ok") == "100\n"

    node.query("DROP TABLE simple_mongo_table_fail")
    node.query("DROP TABLE simple_mongo_table_ok")
    simple_mongo_table.drop()


def test_missing_columns(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 10):
        data.append({"key": i, "data": hex(i * i)})
    for i in range(0, 10):
        data.append({"key": i})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        """CREATE OR REPLACE TABLE simple_mongo_table(
               key                 UInt64,
               data                Nullable(String),
               not_exists          Int64,
               not_exists_nullable Nullable(Int64)
           ) ENGINE = MongoDB(mongo1)"""
    )

    assert (
        node.query("SELECT count() FROM simple_mongo_table WHERE isNull(data)")
        == "10\n"
    )
    assert (
        node.query("SELECT count() FROM simple_mongo_table WHERE isNull(not_exists)")
        == "0\n"
    )

    node.query("DROP TABLE IF EXISTS simple_mongo_table")
    simple_mongo_table.drop()


def test_string_casting(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    string_mongo_table = db["strings_table"]
    data = {
        "k_boolT": True,
        "k_boolF": False,
        "k_int32P": 100,
        "k_int32N": -100,
        "k_int64P": bson.int64.Int64(100),
        "k_int64N": bson.int64.Int64(-100),
        "k_doubleP": 6.66,
        "k_doubleN": -6.66,
        "k_date": datetime.datetime(1999, 2, 28, 0, 0, 0),
        "k_timestamp": datetime.datetime(1999, 2, 28, 12, 46, 34),
        "k_string": "ClickHouse",
        "k_document": {
            "Hello": "world!",
            "meow123": True,
            "number": 321,
            "doc": {"Hello": "world!"},
            "arr": [{"Hello": "world!"}, 1, "c"],
        },
        "k_array": [
            "Hello",
            "world!",
            {"cat": "meow!"},
            [1, 2, 3],
        ],
    }
    string_mongo_table.insert_one(data)

    node = started_cluster.instances["node"]
    node.query(
        """CREATE OR REPLACE TABLE strings_table (
             _id     String,
             k_boolT String,
             k_boolF String,
             k_int32P String,
             k_int32N String,
             k_int64P String,
             k_int64N String,
             k_doubleP String,
             k_doubleN String,
             k_date String,
             k_timestamp String,
             k_string String,
             k_document String,
             k_array String
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'strings_table', 'root', 'clickhouse')"""
    )

    assert node.query("SELECT COUNT() FROM strings_table") == "1\n"
    assert node.query("SELECT _id FROM strings_table") != ""
    assert node.query("SELECT k_boolT FROM strings_table") == "true\n"
    assert node.query("SELECT k_boolF FROM strings_table") == "false\n"
    assert node.query("SELECT k_int32P FROM strings_table") == "100\n"
    assert node.query("SELECT k_int32N FROM strings_table") == "-100\n"
    assert node.query("SELECT k_int64P FROM strings_table") == "100\n"
    assert node.query("SELECT k_int64N FROM strings_table") == "-100\n"
    assert node.query("SELECT k_doubleP FROM strings_table") == "6.660000\n"
    assert node.query("SELECT k_doubleN FROM strings_table") == "-6.660000\n"
    assert node.query("SELECT k_date FROM strings_table") == "1999-02-28 00:00:00\n"
    assert (
        node.query("SELECT k_timestamp FROM strings_table") == "1999-02-28 12:46:34\n"
    )
    assert node.query("SELECT k_string FROM strings_table") == "ClickHouse\n"
    assert json.dumps(
        json.loads(node.query("SELECT k_document FROM strings_table"))
    ) == json.dumps(data["k_document"])
    assert json.dumps(
        json.loads(node.query("SELECT k_array FROM strings_table"))
    ) == json.dumps(data["k_array"])

    node.query("DROP TABLE strings_table")
    string_mongo_table.drop()


def test_dates_casting(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    dates_mongo_table = db["dates_table"]
    data = {
        "k_dateTime": datetime.datetime(1999, 2, 28, 11, 23, 16),
        "k_dateTime64": datetime.datetime(1999, 2, 28, 11, 23, 16),
        "k_date": datetime.datetime(1999, 2, 28, 11, 23, 16),
        "k_date32": datetime.datetime(1999, 2, 28, 11, 23, 16),
    }
    dates_mongo_table.insert_one(data)

    node = started_cluster.instances["node"]
    node.query(
        """CREATE OR REPLACE TABLE dates_table (
             k_dateTime   DateTime,
             k_dateTime64 DateTime64,
             k_date       Date,
             k_date32     Date32
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'dates_table', 'root', 'clickhouse')"""
    )

    assert node.query("SELECT COUNT() FROM dates_table") == "1\n"
    assert (
        node.query("SELECT * FROM dates_table")
        == "1999-02-28 11:23:16\t1999-02-28 11:23:16.000\t1999-02-28\t1999-02-28\n"
    )

    node.query("DROP TABLE dates_table")
    dates_mongo_table.drop()


def test_order_by(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    sort_mongo_table = db["sort_table"]
    data = []
    for i in range(1, 31):
        for d in range(1, 31):
            data.append(
                {
                    "keyInt": i,
                    "keyFloat": i + (d * 0.001),
                    "keyDateTime": datetime.datetime(1999, 12, i, 11, 23, 16),
                    "keyDate": datetime.datetime(1999, 12, i, 11, 23, 16),
                }
            )
    sort_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        """CREATE OR REPLACE TABLE sort_table (
             keyInt      Int,
             keyFloat    Float64,
             keyDateTime DateTime,
             keyDate     Date
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'sort_table', 'root', 'clickhouse')"""
    )

    assert node.query("SELECT COUNT() FROM sort_table") == "900\n"
    assert node.query("SELECT keyInt FROM sort_table ORDER BY keyInt LIMIT 1") == "1\n"
    assert (
        node.query("SELECT keyInt FROM sort_table ORDER BY keyInt DESC LIMIT 1")
        == "30\n"
    )
    assert (
        node.query(
            "SELECT keyInt, keyFloat FROM sort_table ORDER BY keyInt, keyFloat DESC LIMIT 1"
        )
        == "1\t1.03\n"
    )
    assert (
        node.query(
            "SELECT keyDateTime FROM sort_table ORDER BY keyDateTime DESC LIMIT 1"
        )
        == "1999-12-30 11:23:16\n"
    )
    assert (
        node.query("SELECT keyDate FROM sort_table ORDER BY keyDate DESC LIMIT 1")
        == "1999-12-30\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM sort_table ORDER BY keyInt WITH FILL")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM sort_table ORDER BY keyInt WITH FILL TO sort_table")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM sort_table ORDER BY keyInt WITH FILL FROM sort_table")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM sort_table ORDER BY keyInt WITH FILL STEP 1")

    node.query("DROP TABLE sort_table")
    sort_mongo_table.drop()


def test_where(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    where_mongo_table = db["where_table"]
    data = []
    for i in range(1, 3):
        for d in range(1, 3):
            data.append(
                {
                    "id": str(i) + str(d),
                    "keyInt": i,
                    "keyFloat": i + (d * 0.001),
                    "keyString": str(d) + "string",
                    "keyDateTime": datetime.datetime(1999, d, i, 11, 23, 16),
                    "keyDate": datetime.datetime(1999, d, i, 11, 23, 16),
                    "keyNull": None,
                }
            )
    where_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        """CREATE OR REPLACE TABLE where_table (
             id           String,
             keyInt       Int,
             keyFloat     Float64,
             keyString    String,
             keyDateTime  DateTime,
             keyDate      Date,
             keyNull      Nullable(UInt8),
             keyNotExists Nullable(Int)
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'where_table', 'root', 'clickhouse')"""
    )

    assert node.query("SELECT COUNT() FROM where_table") == "4\n"

    assert (
        node.query("SELECT keyString FROM where_table WHERE id = '11'") == "1string\n"
    )
    assert (
        node.query(
            "SELECT keyString FROM where_table WHERE id != '11' ORDER BY keyFloat"
        )
        == "2string\n1string\n2string\n"
    )
    assert (
        node.query(
            "SELECT keyString FROM where_table WHERE id = '11' AND keyString = '1string'"
        )
        == "1string\n"
    )
    assert (
        node.query("SELECT id FROM where_table WHERE keyInt = 1 AND keyFloat = 1.001")
        == "11\n"
    )
    assert (
        node.query("SELECT id FROM where_table WHERE keyInt = 0 OR keyFloat = 1.001")
        == "11\n"
    )

    assert (
        node.query("SELECT id FROM where_table WHERE keyInt BETWEEN 1 AND 2")
        == "11\n12\n21\n22\n"
    )
    assert node.query("SELECT id FROM where_table WHERE keyInt > 10") == ""
    assert (
        node.query("SELECT id FROM where_table WHERE keyInt < 10.1 ORDER BY keyFloat")
        == "11\n12\n21\n22\n"
    )

    assert node.query("SELECT id FROM where_table WHERE id IN ('11')") == "11\n"
    assert node.query("SELECT id FROM where_table WHERE id IN ['11']") == "11\n"
    assert node.query("SELECT id FROM where_table WHERE id IN ('11', 100)") == "11\n"
    assert (
        node.query(
            "SELECT id FROM where_table WHERE id IN ('11', '22') ORDER BY keyFloat"
        )
        == "11\n22\n"
    )
    assert (
        node.query(
            "SELECT id FROM where_table WHERE id IN ['11', '22'] ORDER BY keyFloat"
        )
        == "11\n22\n"
    )

    assert (
        node.query(
            "SELECT id FROM where_table WHERE id NOT IN ('11') ORDER BY keyFloat"
        )
        == "12\n21\n22\n"
    )
    assert (
        node.query(
            "SELECT id FROM where_table WHERE id NOT IN ['11'] ORDER BY keyFloat"
        )
        == "12\n21\n22\n"
    )
    assert (
        node.query(
            "SELECT id FROM where_table WHERE id NOT IN ('11', 100) ORDER BY keyFloat"
        )
        == "12\n21\n22\n"
    )
    assert (
        node.query("SELECT id FROM where_table WHERE id NOT IN ('11') AND id IN ('12')")
        == "12\n"
    )
    assert (
        node.query("SELECT id FROM where_table WHERE id NOT IN ['11'] AND id IN ('12')")
        == "12\n"
    )

    with pytest.raises(QueryRuntimeException):
        assert (
            node.query(
                "SELECT id FROM where_table WHERE id NOT IN ['11', 100] ORDER BY keyFloat"
            )
            == "12\n21\n22\n"
        )

    assert node.query("SELECT id FROM where_table WHERE keyDateTime > now()") == ""
    assert (
        node.query(
            "SELECT keyInt FROM where_table WHERE keyDateTime < now() AND keyString = '1string' AND keyInt IS NOT NULL ORDER BY keyInt"
        )
        == "1\n2\n"
    )

    assert node.query("SELECT count() FROM where_table WHERE isNotNull(id)") == "4\n"
    assert (
        node.query("SELECT count() FROM where_table WHERE isNotNull(keyNull)") == "0\n"
    )
    assert node.query("SELECT count() FROM where_table WHERE isNull(keyNull)") == "4\n"
    assert (
        node.query("SELECT count() FROM where_table WHERE isNotNull(keyNotExists)")
        == "0\n"
    )
    assert (
        node.query("SELECT count() FROM where_table WHERE isNull(keyNotExists)")
        == "4\n"
    )
    assert node.query("SELECT count() FROM where_table WHERE keyNotExists = 0") == "0\n"
    assert (
        node.query("SELECT count() FROM where_table WHERE keyNotExists != 0") == "0\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM where_table WHERE keyInt = keyFloat")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM where_table WHERE equals(keyInt, keyFloat)")

    node.query("DROP TABLE where_table")
    where_mongo_table.drop()


def test_defaults(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    defaults_mongo_table = db["defaults_table"]
    defaults_mongo_table.insert_one({"key": "key"})

    node = started_cluster.instances["node"]
    node.query(
        """
        CREATE OR REPLACE TABLE defaults_table(
        _id          String,
        k_int64      Int64,
        k_int32      Int32,
        k_int16      Int16,
        k_int8       Int8,
        k_uint64     UInt64,
        k_uint32     UInt32,
        k_uint16     UInt16,
        k_uint8      UInt8,
        k_float32    Float32,
        k_float64    Float64,
        k_date       Date,
        k_date32     Date32,
        k_datetime   DateTime,
        k_datetime64 DateTime64,
        k_string     String,
        k_uuid       UUID,
        k_arr        Array(Bool)
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'defaults_table', 'root', 'clickhouse')
        """
    )

    assert node.query("SELECT COUNT() FROM defaults_table") == "1\n"

    assert (
        node.query(
            "SELECT k_int64, k_int32, k_int16, k_int8, k_uint64, k_uint32, k_uint16, k_uint8, k_float32, k_float64 FROM defaults_table"
        )
        == "0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n"
    )
    assert (
        node.query(
            "SELECT k_date, k_date32, k_datetime, k_datetime64, k_string, k_uuid, k_arr FROM defaults_table"
        )
        == "1970-01-01\t1900-01-01\t1970-01-01 00:00:00\t1970-01-01 00:00:00.000\t\t00000000-0000-0000-0000-000000000000\t[]\n"
    )

    node.query("DROP TABLE defaults_table")
    defaults_mongo_table.drop()


def test_nulls(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    nulls_mongo_table = db["nulls_table"]
    nulls_mongo_table.insert_one({"key": "key"})

    node = started_cluster.instances["node"]
    node.query(
        """
        CREATE OR REPLACE TABLE nulls_table(
        _id          String,
        k_int64      Nullable(Int64),
        k_int32      Nullable(Int32),
        k_int16      Nullable(Int16),
        k_int8       Nullable(Int8),
        k_uint64     Nullable(UInt64),
        k_uint32     Nullable(UInt32),
        k_uint16     Nullable(UInt16),
        k_uint8      Nullable(UInt8),
        k_float32    Nullable(Float32),
        k_float64    Nullable(Float64),
        k_date       Nullable(Date),
        k_date32     Nullable(Date32),
        k_datetime   Nullable(DateTime),
        k_datetime64 Nullable(DateTime64),
        k_string     Nullable(String),
        k_uuid       Nullable(UUID)
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'nulls_table', 'root', 'clickhouse')
        """
    )

    assert node.query("SELECT COUNT() FROM nulls_table") == "1\n"

    assert (
        node.query(
            "SELECT k_int64, k_int32, k_int16, k_int8, k_uint64, k_uint32, k_uint16, k_uint8, k_float32, k_float64 FROM nulls_table"
        )
        == "\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n"
    )
    assert (
        node.query(
            "SELECT k_date, k_date32, k_datetime, k_datetime64, k_string, k_uuid FROM nulls_table"
        )
        == "\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n"
    )

    node.query("DROP TABLE nulls_table")
    nulls_mongo_table.drop()


def test_oid(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    oid_mongo_table = db["oid_table"]
    inserted_result = oid_mongo_table.insert_many(
        [
            {"key": "a"},
            {"key": "b"},
            {"key": "c"},
            {"key": "d"},
            {"key": "e"},
        ]
    )
    oid = inserted_result.inserted_ids

    node = started_cluster.instances["node"]
    node.query(
        """
        CREATE OR REPLACE TABLE oid_table(
        _id  String,
        key  String
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'oid_table', 'root', 'clickhouse')
        """
    )

    assert node.query("SELECT COUNT() FROM oid_table") == "5\n"

    assert node.query(f"SELECT key FROM oid_table WHERE _id = '{oid[0]}'") == "a\n"
    assert (
        node.query(f"SELECT * FROM oid_table WHERE _id = '{oid[2]}'")
        == f"{oid[2]}\tc\n"
    )
    assert node.query(f"SELECT COUNT() FROM oid_table WHERE _id != '{oid[0]}'") == "4\n"

    assert (
        node.query(
            f"SELECT key FROM oid_table WHERE _id in ('{oid[0]}', '{oid[1]}') ORDER BY key"
        )
        == "a\nb\n"
    )
    assert (
        node.query(
            f"SELECT key FROM oid_table WHERE _id in ['{oid[0]}', '{oid[1]}'] ORDER BY key"
        )
        == "a\nb\n"
    )
    assert (
        node.query(f"SELECT key FROM oid_table WHERE _id in ('{oid[0]}') ORDER BY key")
        == "a\n"
    )
    assert (
        node.query(f"SELECT key FROM oid_table WHERE _id in ['{oid[1]}'] ORDER BY key")
        == "b\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id = 'invalidOID'")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id = 123123")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id in (123123, 123)")

    node.query("DROP TABLE oid_table")
    oid_mongo_table.drop()


def test_uuid(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    uuid_mongo_table = db["uuid_table"]
    uuid_mongo_table.insert_many(
        [
            {"isValid": 0, "kUUID": "bad_uuid"},
            {"isValid": 1, "kUUID": "f0e77736-91d1-48ce-8f01-15123ca1c7ed"},
        ]
    )

    node = started_cluster.instances["node"]
    node.query(
        """
        CREATE OR REPLACE TABLE uuid_table(
        isValid UInt8,
        kUUID   UUID
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'uuid_table', 'root', 'clickhouse')
        """
    )

    assert (
        node.query(f"SELECT kUUID FROM uuid_table WHERE isValid = 1")
        == "f0e77736-91d1-48ce-8f01-15123ca1c7ed\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM uuid_table WHERE isValid = 0")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM uuid_table")

    node.query("DROP TABLE uuid_table")
    uuid_mongo_table.drop()


def test_no_fail_on_unsupported_clauses(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd="clickhouse", roles=["readWrite"])
    unsupported_clauses_table = db["unsupported_clauses"]

    node = started_cluster.instances["node"]
    node.query(
        """
        CREATE OR REPLACE TABLE unsupported_clauses(
        a UInt64,
        b UInt64
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'unsupported_clauses', 'root', 'clickhouse')
        """
    )

    node.query(
        f"SELECT * FROM unsupported_clauses WHERE a > rand() SETTINGS mongodb_throw_on_unsupported_query = 0"
    )
    node.query(
        f"SELECT * FROM unsupported_clauses WHERE a / 1000 > 0 SETTINGS mongodb_throw_on_unsupported_query = 0"
    )
    node.query(
        f"SELECT * FROM unsupported_clauses WHERE toFloat64(a) < 6.66 > rand() SETTINGS mongodb_throw_on_unsupported_query = 0"
    )
    node.query(
        f"SELECT * FROM unsupported_clauses ORDER BY a, b LIMIT 2 BY a SETTINGS mongodb_throw_on_unsupported_query = 0"
    )

    node.query("DROP TABLE unsupported_clauses")
    unsupported_clauses_table.drop()


def test_password_masking(started_cluster):
    node = started_cluster.instances["node"]

    node.query(
        """
        CREATE OR REPLACE TABLE mongodb_uri_password_masking (_id String)
        ENGINE = MongoDB('mongodb://testuser:mypassword@127.0.0.1:27017/example', 'test_clickhouse');
        """
    )
    assert (
        node.query(
            """
                SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables
                WHERE table = 'mongodb_uri_password_masking' AND database = currentDatabase();
                """
        )
        == "CREATE TABLE default.mongodb_uri_password_masking (`_id` String) ENGINE = MongoDB(\\'mongodb://testuser:[HIDDEN]@127.0.0.1:27017/example\\', \\'test_clickhouse\\')\n"
    )
    assert (
        node.query(
            """
        SELECT replaceAll(engine_full, currentDatabase(), 'default') FROM system.tables
        WHERE table = 'mongodb_uri_password_masking' AND database = currentDatabase();
        """
        )
        == "MongoDB(\\'mongodb://testuser:[HIDDEN]@127.0.0.1:27017/example\\', \\'test_clickhouse\\')\n"
    )
    node.query("DROP TABLE IF EXISTS mongodb_uri_password_masking;")

    node.query(
        """
        CREATE OR REPLACE DICTIONARY mongodb_dictionary_uri_password_masking (_id String)
        PRIMARY KEY _id
        SOURCE(MONGODB(uri 'mongodb://testuser:mypassword@127.0.0.1:27017/example' collection 'test_clickhouse'))
        LAYOUT(FLAT())
        LIFETIME(0);
        """
    )
    assert (
        node.query(
            """
        SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables
        WHERE table = 'mongodb_dictionary_uri_password_masking' AND database = currentDatabase();"""
        )
        == "CREATE DICTIONARY default.mongodb_dictionary_uri_password_masking (`_id` String) PRIMARY KEY _id SOURCE(MONGODB(URI \\'mongodb://testuser:[HIDDEN]@127.0.0.1:27017/example\\' COLLECTION \\'test_clickhouse\\')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())\n"
    )
    node.query("DROP DICTIONARY IF EXISTS mongodb_dictionary_uri_password_masking;")

    node.query(
        """
        CREATE TABLE mongodb_password_masking (_id String)
        ENGINE = MongoDB('127.0.0.1:27017', 'example', 'test_clickhouse', 'testuser', 'mypassword');
        """
    )
    assert (
        node.query(
            """
        SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables
        WHERE table = 'mongodb_password_masking' AND database = currentDatabase();
        """
        )
        == "CREATE TABLE default.mongodb_password_masking (`_id` String) ENGINE = MongoDB(\\'127.0.0.1:27017\\', \\'example\\', \\'test_clickhouse\\', \\'testuser\\', \\'[HIDDEN]\\')\n"
    )
    assert (
        node.query(
            """
        SELECT replaceAll(engine_full, currentDatabase(), 'default') FROM system.tables
        WHERE table = 'mongodb_password_masking' AND database = currentDatabase();
        """
        )
        == "MongoDB(\\'127.0.0.1:27017\\', \\'example\\', \\'test_clickhouse\\', \\'testuser\\', \\'[HIDDEN]\\')\n"
    )
    node.query("DROP TABLE IF EXISTS mongodb_password_masking;")

    node.query(
        """
        CREATE OR REPLACE DICTIONARY mongodb_dictionary_password_masking (_id String)
        PRIMARY KEY _id
        SOURCE(MONGODB(
            host '127.0.0.1'
            port 27017
            user 'testuser'
            password 'mypassword'
            db 'example'
            collection 'test_clickhouse'
            options 'ssl=true'
           ))
        LAYOUT(FLAT())
        LIFETIME(0);
        """
    )
    assert (
        node.query(
            """
        SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables
        WHERE table = 'mongodb_dictionary_password_masking' AND database = currentDatabase();
        """
        )
        == "CREATE DICTIONARY default.mongodb_dictionary_password_masking (`_id` String) PRIMARY KEY _id SOURCE(MONGODB(HOST \\'127.0.0.1\\' PORT 27017 USER \\'testuser\\' PASSWORD \\'[HIDDEN]\\' DB \\'example\\' COLLECTION \\'test_clickhouse\\' OPTIONS \\'ssl=true\\')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())\n"
    )
    node.query("DROP DICTIONARY IF EXISTS mongodb_dictionary_password_masking;")

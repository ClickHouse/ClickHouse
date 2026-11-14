import base64
import datetime
import json
import uuid
import urllib
import bson
from bson.raw_bson import RawBSONDocument
import pymongo
import pytest

from helpers.database_disk import replace_text_in_metadata
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mongo_pass
from helpers.test_tools import wait_condition


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            with_mongo=True,
            stay_alive=True,
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


def drop_mongo_collection_if_exists(db, collection_name):
    if collection_name in db.list_collection_names():
        db[collection_name].drop()


def test_simple_select(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1', 'test', 'simple_table', 'root', '{mongo_pass}')"
    )

    assert node.query("SELECT COUNT() FROM simple_mongo_table") == "100\n"
    assert (
        node.query("SELECT sum(key) FROM simple_mongo_table")
        == f"{str(99 * 100 // 2)}\n"
    )
    assert (
        node.query("SELECT data from simple_mongo_table where key = 42")
        == f"{hex(42 * 42)}\n"
    )

    system_warnings_query = "SELECT count() >= 1 FROM system.warnings WHERE message LIKE '%MongoDB%path%ignored%'"

    # Need to restart to clear system.warning from previous run in flaky check
    # FIXME: we can do `TRUNCATE` after https://github.com/ClickHouse/ClickHouse/pull/82087
    node.restart_clickhouse()

    assert node.query(system_warnings_query) == "0\n"

    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables WHERE database='default' AND table='simple_mongo_table'"
    ).strip()
    node.stop_clickhouse()
    replace_text_in_metadata(
        node, metadata_path, "mongo1", "mongo1/ignored/path"
    )

    node.start_clickhouse()
    assert node.query("SELECT COUNT() FROM simple_mongo_table") == "100\n"
    assert node.query(system_warnings_query) == "1\n"

    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()


def test_simple_select_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table_uri")
    simple_mongo_table = db["simple_table_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE simple_table_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo1:27017/test', 'simple_table_uri')"
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table")
    drop_mongo_collection_if_exists(db, "simple_table_view")

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
        f"CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table_view', 'root', '{mongo_pass}')"
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "arrays_table")

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
                    bson.Binary(
                        uuid.UUID("f0e77736-91d1-48ce-8f01-15123ca1c7ed").bytes,
                        subtype=4,
                    ),
                    bson.Binary(
                        uuid.UUID("93376a07-c044-4281-a76e-ad27cf6973c5").bytes,
                        subtype=4,
                    ),
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
        f") ENGINE = MongoDB('mongo1:27017', 'test', 'arrays_table', 'root', '{mongo_pass}')"
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
        node.query("SELECT arr_date FROM arrays_mongo_table WHERE key = 42")
        == "['2002-10-27','2024-01-08']\n"
    )
    assert (
        node.query("SELECT arr_date32 FROM arrays_mongo_table WHERE key = 42")
        == "['2002-10-27','2024-01-08']\n"
    )
    assert (
        node.query("SELECT arr_datetime FROM arrays_mongo_table WHERE key = 42")
        == "['2023-03-31 06:03:12','1999-02-28 12:46:34']\n"
    )
    assert (
        node.query("SELECT arr_datetime64 FROM arrays_mongo_table WHERE key = 42")
        == "['2023-03-31 06:03:12.000','1999-02-28 12:46:34.000']\n"
    )
    assert (
        node.query("SELECT arr_string FROM arrays_mongo_table WHERE key = 42")
        == "['43','44','45']\n"
    )
    assert (
        node.query("SELECT arr_uuid FROM arrays_mongo_table WHERE key = 42")
        == "['f0e77736-91d1-48ce-8f01-15123ca1c7ed','93376a07-c044-4281-a76e-ad27cf6973c5']\n"
    )
    assert (
        node.query("SELECT arr_arr_bool FROM arrays_mongo_table WHERE key = 42")
        == "[[true,false,true],[true],[false],[false]]\n"
    )
    assert (
        node.query(
            "SELECT arr_arr_bool_nullable FROM arrays_mongo_table WHERE key = 42"
        )
        == "[[true,false,true],[true,NULL],[NULL],[false]]\n"
    )
    assert (
        node.query("SELECT arr_empty FROM arrays_mongo_table WHERE key = 42") == "[]\n"
    )
    assert (
        node.query("SELECT arr_null FROM arrays_mongo_table WHERE key = 42") == "[]\n"
    )
    assert (
        node.query("SELECT arr_arr_null FROM arrays_mongo_table WHERE key = 42")
        == "[]\n"
    )
    assert (
        node.query("SELECT arr_nullable FROM arrays_mongo_table WHERE key = 42")
        == "[]\n"
    )

    node.query("DROP TABLE arrays_mongo_table")
    arrays_mongo_table.drop()


def test_complex_data_type(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "complex_table")
    incomplete_mongo_table = db["complex_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i), "dict": {"a": i, "b": str(i)}})
    incomplete_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE incomplete_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'complex_table', 'root', '{mongo_pass}')"
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo_secure:27017', 'test', 'simple_table', 'root', '{mongo_pass}', 'tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true')"
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo_secure:27017', 'test', 'simple_table', 'root', '{mongo_pass}', 'tls=true')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM simple_mongo_table")

    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()


def test_secure_connection_uri(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster, secure=True)
    db = mongo_connection["test"]
    drop_mongo_collection_if_exists(db, "test_secure_connection_uri")
    simple_mongo_table = db["test_secure_connection_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE test_secure_connection_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo_secure:27017/test?tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true&authSource=admin', 'test_secure_connection_uri')"
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
    drop_mongo_collection_if_exists(db, "test_secure_connection_uri")
    simple_mongo_table = db["test_secure_connection_uri"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE test_secure_connection_uri(key UInt64, data String) ENGINE = MongoDB('mongodb://root:{urllib.parse.quote_plus(mongo_pass)}@mongo_secure:27017/test?tls=true', 'test_secure_connection_uri')"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM test_secure_connection_uri")

    node.query("DROP TABLE test_secure_connection_uri")

    simple_mongo_table.drop()


def test_predefined_connection_configuration(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table")
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table_uri")
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
    drop_mongo_collection_if_exists(db, "simple_table")
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
    drop_mongo_collection_if_exists(db, "simple_table_uri")
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
        pwd=mongo_pass,
        roles=[{"role": "userAdminAnyDatabase", "db": "admin"}, "readWriteAnyDatabase"],
    )
    drop_mongo_collection_if_exists(admin_db, "simple_table")
    simple_mongo_table = admin_db["simple_table"]
    data = []
    for i in range(0, 50):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)
    db = mongo_connection["test"]
    drop_mongo_collection_if_exists(db, "simple_table")
    simple_mongo_table = db["simple_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE simple_mongo_table_fail(key UInt64, data String) ENGINE = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', 'root', '{mongo_pass}')"
    )
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT count() FROM simple_mongo_table_fail")

    node.query(
        f"CREATE OR REPLACE TABLE simple_mongo_table_ok(key UInt64, data String) ENGINE = MongoDB('mongo_no_cred:27017', 'test', 'simple_table', 'root', '{mongo_pass}', 'authSource=admin')"
    )
    assert node.query("SELECT count() FROM simple_mongo_table_ok") == "100\n"

    node.query("DROP TABLE simple_mongo_table_fail")
    node.query("DROP TABLE simple_mongo_table_ok")
    simple_mongo_table.drop()


def test_missing_columns(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "simple_table")
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "strings_table")
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
        f"""CREATE OR REPLACE TABLE strings_table (
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
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'strings_table', 'root', '{mongo_pass}')"""
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "dates_table")
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
        f"""CREATE OR REPLACE TABLE dates_table (
             k_dateTime   DateTime,
             k_dateTime64 DateTime64,
             k_date       Date,
             k_date32     Date32
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'dates_table', 'root', '{mongo_pass}')"""
    )

    assert node.query("SELECT COUNT() FROM dates_table") == "1\n"
    assert (
        node.query("SELECT * FROM dates_table")
        == "1999-02-28 11:23:16\t1999-02-28 11:23:16.000\t1999-02-28\t1999-02-28\n"
    )

    # An `IN` list is pushed down as `$in`; the list's type has to reach each element.
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_dateTime IN (toDateTime64('1999-02-28 11:23:16', 3))"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_dateTime NOT IN (toDateTime64('1999-02-28 11:23:16', 3))"
        )
        == "0\n"
    )
    # A list is converted element by element.
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_dateTime IN (toDateTime64('1999-02-28 11:23:16', 3), toDateTime64('2000-01-01 00:00:00', 3))"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_date NOT IN (toDateTime('1999-02-28 00:00:00'), toDateTime('2000-01-01 00:00:00'))"
        )
        == "0\n"
    )
    # A sub-second bound is not pushed down truncated: the query is refused like any other predicate MongoDB
    # cannot take, and evaluated in ClickHouse when allowed to.
    assert "NOT_IMPLEMENTED" in node.query_and_get_error(
        "SELECT COUNT() FROM dates_table WHERE k_dateTime >= toDateTime64('1999-02-28 11:23:16.5', 3)"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_dateTime >= toDateTime64('1999-02-28 11:23:16.5', 3) SETTINGS mongodb_throw_on_unsupported_query = 0"
        )
        == "0\n"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_date32 < toDateTime('1999-02-28 12:00:00') SETTINGS mongodb_throw_on_unsupported_query = 0"
        )
        == "1\n"
    )

    # A member converts as `IN` converts it, and a sub-second member matches nothing. MongoDB compares the
    # stored value, so a `Date` member with a time of day is refused, and a midnight one matches the
    # document stored at midnight.
    dates_mongo_table.insert_one({k: datetime.datetime(2000, 1, 1) for k in data})
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_date IN (toDateTime('2000-01-01 00:00:00'))"
        )
        == "1\n"
    )
    assert "NOT_IMPLEMENTED" in node.query_and_get_error(
        "SELECT COUNT() FROM dates_table WHERE k_date IN (toDateTime('2000-01-01 12:00:00'))"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_date IN (toDateTime('2000-01-01 12:00:00')) SETTINGS mongodb_throw_on_unsupported_query = 0"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_date NOT IN (toDateTime('2000-01-01 12:00:00'))"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_dateTime IN (toDateTime64('1999-02-28 11:23:16.5', 3))"
        )
        == "0\n"
    )
    assert (
        node.query(
            "SELECT COUNT() FROM dates_table WHERE k_dateTime NOT IN (toDateTime64('1999-02-28 11:23:16.5', 3))"
        )
        == "2\n"
    )

    node.query("DROP TABLE dates_table")
    dates_mongo_table.drop()


def test_order_by(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "sort_table")
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
        f"""CREATE OR REPLACE TABLE sort_table (
             keyInt      Int,
             keyFloat    Float64,
             keyDateTime DateTime,
             keyDate     Date
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'sort_table', 'root', '{mongo_pass}')"""
    )

    assert node.query("SELECT COUNT() FROM sort_table") == "900\n"
    assert node.query("SELECT keyInt FROM sort_table ORDER BY keyInt LIMIT 1") == "1\n"
    assert node.query("SELECT keyInt FROM sort_table ORDER BY keyInt DESC LIMIT 1") == "30\n"
    assert node.query("SELECT keyInt, keyFloat FROM sort_table ORDER BY keyInt, keyFloat DESC LIMIT 1") == "1\t1.03\n"
    assert node.query("SELECT keyDateTime FROM sort_table ORDER BY keyDateTime DESC LIMIT 1") == "1999-12-30 11:23:16\n"
    assert node.query("SELECT keyDate FROM sort_table ORDER BY keyDate DESC LIMIT 1") == "1999-12-30\n"

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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "where_table")
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
                    "keyBool": d == 1,
                    "keyUuid": bson.Binary(
                        uuid.UUID("8d9c5028-3371-4941-943f-84a0c90c5149").bytes,
                        subtype=4,
                    ),
                }
            )
    where_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"""CREATE OR REPLACE TABLE where_table (
             id           String,
             keyInt       Int,
             keyFloat     Float64,
             keyString    String,
             keyDateTime  DateTime,
             keyDate      Date,
             keyNull      Nullable(UInt8),
             keyNotExists Nullable(Int),
             keyBool      Nullable(Bool),
             keyUuid      UUID
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'where_table', 'root', '{mongo_pass}')"""
    )

    assert node.query("SELECT COUNT() FROM where_table") == "4\n"

    assert node.query("SELECT keyString FROM where_table WHERE id = '11'") == "1string\n"
    assert (
        node.query(
            "SELECT keyString FROM where_table WHERE id != '11' ORDER BY keyFloat"
        )
        == "2string\n1string\n2string\n"
    )
    assert node.query("SELECT keyString FROM where_table WHERE id = '11' AND keyString = '1string'") == "1string\n"
    assert node.query("SELECT id FROM where_table WHERE keyInt = 1 AND keyFloat = 1.001") == "11\n"
    assert node.query("SELECT id FROM where_table WHERE keyInt = 0 OR keyFloat = 1.001") == "11\n"

    assert node.query("SELECT id FROM where_table WHERE keyInt BETWEEN 1 AND 2") == "11\n12\n21\n22\n"
    assert node.query("SELECT id FROM where_table WHERE keyInt > 10") == ""
    assert node.query("SELECT id FROM where_table WHERE keyInt < 10.1 ORDER BY keyFloat") == "11\n12\n21\n22\n"

    assert node.query("SELECT id FROM where_table WHERE id IN ('11')") == "11\n"
    assert node.query("SELECT id FROM where_table WHERE id IN ['11']") == "11\n"
    assert node.query("SELECT id FROM where_table WHERE id IN ('11', 100)") == "11\n"
    assert node.query("SELECT id FROM where_table WHERE id IN ('11', '22') ORDER BY keyFloat") == "11\n22\n"
    assert node.query("SELECT id FROM where_table WHERE keyInt IN (1.0, 2.0) ORDER BY id") == "11\n12\n21\n22\n"
    assert node.query("SELECT id FROM where_table WHERE id IN ['11', '22'] ORDER BY keyFloat") == "11\n22\n"

    assert node.query("SELECT id FROM where_table WHERE id NOT IN ('11') ORDER BY keyFloat") == "12\n21\n22\n"
    assert node.query("SELECT id FROM where_table WHERE id NOT IN ['11'] ORDER BY keyFloat") == "12\n21\n22\n"
    assert node.query("SELECT id FROM where_table WHERE id NOT IN ('11', 100) ORDER BY keyFloat") == "12\n21\n22\n"
    assert node.query("SELECT id FROM where_table WHERE id NOT IN ('11') AND id IN ('12')") == "12\n"
    assert node.query("SELECT id FROM where_table WHERE id NOT IN ['11'] AND id IN ('12')") == "12\n"

    assert node.query("SELECT id FROM where_table WHERE id NOT IN ['11', 100] ORDER BY keyFloat") == "12\n21\n22\n"

    assert node.query("SELECT id FROM where_table WHERE keyDateTime > now()") == ""
    assert (
        node.query(
            "SELECT keyInt FROM where_table WHERE keyDateTime < now() AND keyString = '1string' AND keyInt IS NOT NULL ORDER BY keyInt"
        )
        == "1\n2\n"
    )
    assert (
        node.query(
            "SELECT keyUuid FROM where_table WHERE keyUuid = toUUID('8d9c5028-3371-4941-943f-84a0c90c5149') LIMIT 1"
        )
        == "8d9c5028-3371-4941-943f-84a0c90c5149\n"
    )

    assert node.query("SELECT count() FROM where_table WHERE isNotNull(id)") == "4\n"
    assert node.query("SELECT count() FROM where_table WHERE isNotNull(keyNull)") == "0\n"
    assert node.query("SELECT count() FROM where_table WHERE isNull(keyNull)") == "4\n"
    assert node.query("SELECT count() FROM where_table WHERE isNotNull(keyNotExists)") == "0\n"
    assert node.query("SELECT count() FROM where_table WHERE isNull(keyNotExists)") == "4\n"
    assert node.query("SELECT count() FROM where_table WHERE keyNotExists = 0") == "0\n"
    assert node.query("SELECT count() FROM where_table WHERE keyNotExists != 0") == "0\n"
    assert node.query("SELECT count() FROM where_table WHERE keyBool = true") == "2\n"
    assert (
        node.query("SELECT count() FROM where_table WHERE keyBool IN (false)") == "2\n"
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM where_table WHERE keyInt = keyFloat")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM where_table WHERE equals(keyInt, keyFloat)")

    node.query("DROP TABLE where_table")
    where_mongo_table.drop()


@pytest.mark.parametrize("bool_type", ["Bool", "Nullable(Bool)"])
@pytest.mark.parametrize("layout", ["native", "swapped"])
def test_where_bool_number_comparisons(started_cluster, bool_type, layout):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "bool_number_comparisons"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # A `Bool` column also reads BSON numbers, and a numeric column also reads BSON bools.
    documents = {
        "native": [
            {"flag": False, "n": 0},
            {"flag": True, "n": 1},
            {"flag": True, "n": 1},
            {"flag": False, "n": 2},
        ],
        "swapped": [
            {"flag": 0, "n": False},
            {"flag": 1, "n": True},
            {"flag": 1, "n": True},
            {"flag": 0, "n": 2},
        ],
    }
    collection.insert_many(documents[layout])

    node = started_cluster.instances["node"]
    structure = f"flag {bool_type}, n UInt8"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            expected = {
                "flag = 1": "2\n",
                "flag = 0": "2\n",
                "flag > 0": "2\n",
                "flag >= 1": "2\n",
                "flag <= 0": "2\n",
                "0 < flag": "2\n",
                "n = true": "2\n",
                "true < n": "1\n",
                "flag IN (1)": "2\n",
                "flag != 0": "2\n",
                "flag = 2": "0\n",
                "flag != 2": "4\n",
                "flag = 2.0": "0\n",
                "flag != 2.0": "4\n",
                "flag != toFloat32(2)": "4\n",
                "flag IN (1, 2)": "2\n",
                "flag NOT IN (2)": "4\n",
                "flag NOT IN (0, 2)": "2\n",
                "flag IN (2.0)": "0\n",
                "flag NOT IN (2.0)": "4\n",
                "flag NOT IN (0, toFloat32(2))": "2\n",
                "n IN (true)": "2\n",
                "n NOT IN (true)": "2\n",
                "n IN (reinterpret(char(2), 'Bool'))": "2\n",
                "n NOT IN (reinterpret(char(2), 'Bool'), 0)": "1\n",
            }
            actual = {
                predicate: node.query(
                    f"SELECT count() FROM {source} WHERE {predicate}",
                    settings=(
                        {"mongodb_throw_on_unsupported_query": 0}
                        if "flag" in predicate or "reinterpret" in predicate
                        else {}
                    ),
                )
                for predicate in expected
            }
            assert actual == expected

            for predicate, count in {
                "flag < 2": "4\n",
                "flag < 2.0": "4\n",
                "flag < toFloat32(2)": "4\n",
                "flag > -1": "4\n",
                "flag > 0.5": "2\n",
                "flag = -1": "0\n",
                "flag != 256": "4\n",
                "flag != -1.0": "4\n",
            }.items():
                assert (
                    node.query(
                        f"SELECT count() FROM {source} WHERE {predicate}",
                        settings={"mongodb_throw_on_unsupported_query": 0},
                    )
                    == count
                )

        # A wide integer column takes a `Bool` bound as an `Int64`, since `fieldAsBSONValue` writes no wide integer.
        for wide_type in ("Int128", "UInt128", "Int256", "UInt256"):
            wide_source = f"mongodb({arguments}, 'flag {bool_type}, n {wide_type}')"
            for predicate, count in {
                "n = true": "2\n",
                "n != false": "3\n",
                "n > false": "3\n",
                "n >= true": "3\n",
            }.items():
                assert (
                    node.query(f"SELECT count() FROM {wide_source} WHERE {predicate}")
                    == count
                )

        # A `Bool` column keeps a BSON number outside `{0, 1}` as it is. `=` matches it, and `IN` matches it by its truth.
        collection.insert_one({"flag": 2, "n": 2})
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            expected = {
                "flag = 2.0": "1\n",
                "flag != 2.0": "4\n",
                "flag IN (2.0)": "0\n",
                "flag IN (0, 2.0)": "2\n",
                "flag IN (1, 2.0)": "3\n",
                "flag IN (1, 2)": "3\n",
                "flag NOT IN (1, 2.0)": "2\n",
                "flag NOT IN (2.0)": "5\n",
                "flag NOT IN (toFloat32(2))": "5\n",
                "flag NOT IN (0, toFloat32(2))": "3\n",
                "flag NOT IN (2)": "5\n",
            }
            actual = {
                predicate: node.query(
                    f"SELECT count() FROM {source} WHERE {predicate}",
                    settings={"mongodb_throw_on_unsupported_query": 0},
                )
                for predicate in expected
            }
            assert actual == expected
    finally:
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("bool_type", ["Bool", "Nullable(Bool)"])
def test_where_bool_wrapped_numbers(started_cluster, bool_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "bool_wrapped_numbers"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # The reader wraps a BSON number in a `Bool` column to `UInt8`: 256 is read as 0, and -1 as 255.
    collection.insert_many(
        [
            {"id": 1, "flag": 2},
            {"id": 2, "flag": False},
            {"id": 3, "flag": 256},
            {"id": 4, "flag": -1},
        ]
    )

    node = started_cluster.instances["node"]
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    structure = f"id Int32, flag {bool_type}"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for query, result in {
                "SELECT count() FROM {} WHERE flag != 256": "4\n",
                "SELECT count() FROM {} WHERE flag != -1": "4\n",
                "SELECT count() FROM {} WHERE flag = 256": "0\n",
                "SELECT id FROM {} WHERE flag = 0 ORDER BY id": "2\n3\n",
                "SELECT id FROM {} WHERE flag > 0 ORDER BY id": "1\n4\n",
                "SELECT id FROM {} WHERE flag = 0 ORDER BY id DESC LIMIT 1": "3\n",
                # The limit must not discard rows before local filtering.
                "SELECT id FROM {} WHERE flag IN (0, 2.0) ORDER BY id LIMIT 1": "2\n",
            }.items():
                assert node.query(query.format(reference)) == result
                assert (
                    node.query(
                        query.format(source),
                        settings={"mongodb_throw_on_unsupported_query": 0},
                    )
                    == result
                )
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("bool_type", ["Bool", "Nullable(Bool)"])
def test_where_bool_wide_constants(started_cluster, bool_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "bool_wide_constants"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many([{"id": 1, "flag": 1}, {"id": 2, "flag": True}])

    node = started_cluster.instances["node"]
    structure = f"id Int32, flag {bool_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for wide_type in ("Int128", "UInt128", "Int256", "UInt256"):
                query = f"SELECT id FROM {{}} WHERE flag = to{wide_type}(1) ORDER BY id"
                assert node.query(query.format(reference)) == "1\n2\n"
                assert (
                    node.query(
                        query.format(source),
                        settings={"mongodb_throw_on_unsupported_query": 0},
                    )
                    == "1\n2\n"
                )
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize(
    "column_type", ["Int128", "UInt128", "Int256", "UInt256", "Nullable(Int128)"]
)
def test_where_wide_membership_bool_members(started_cluster, column_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "wide_membership_bool_members"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many([{"id": 1, "n": True}, {"id": 2, "n": 1}, {"id": 3, "n": 2}])

    node = started_cluster.instances["node"]
    structure = f"id Int32, n {column_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for query, result in {
                "SELECT id FROM {} WHERE n IN (true) ORDER BY id": "1\n2\n",
                "SELECT id FROM {} WHERE n NOT IN (true) ORDER BY id": "3\n",
                "SELECT id FROM {} WHERE n IN (true, 2) ORDER BY id": "1\n2\n3\n",
                "SELECT id FROM {} WHERE n IN (true) ORDER BY id LIMIT 1": "1\n",
            }.items():
                assert node.query(query.format(reference)) == result
                for throw_on_unsupported_query in (0, 1):
                    assert (
                        node.query(
                            query.format(source),
                            settings={
                                "mongodb_throw_on_unsupported_query": throw_on_unsupported_query
                            },
                        )
                        == result
                    )
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize(
    "column_type",
    ["Int64", "Int128", "Nullable(Int128)", "UInt256", "Nullable(UInt256)"],
)
def test_where_wide_scalar_bounds(started_cluster, column_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "wide_scalar_bounds"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many([{"id": 1, "n": 1}, {"id": 2, "n": 2}, {"id": 3, "n": 3}])

    node = started_cluster.instances["node"]
    structure = f"id Int32, n {column_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for query, result in {
                "SELECT id FROM {} WHERE n = 1 ORDER BY id": "1\n",
                "SELECT id FROM {} WHERE n = toInt128(1) ORDER BY id": "1\n",
                "SELECT id FROM {} WHERE n != toUInt256(2) ORDER BY id": "1\n3\n",
                "SELECT id FROM {} WHERE n > toUInt128(1) ORDER BY id": "2\n3\n",
                "SELECT id FROM {} WHERE toInt256(2) >= n ORDER BY id": "1\n2\n",
            }.items():
                assert node.query(query.format(reference)) == result
                for throw_on_unsupported_query in (0, 1):
                    assert (
                        node.query(
                            query.format(source),
                            settings={
                                "mongodb_throw_on_unsupported_query": throw_on_unsupported_query
                            },
                        )
                        == result
                    )

            # A bound that `Int64` cannot hold is evaluated locally.
            query = f"SELECT id FROM {source} WHERE n != toInt128('100000000000000000000') ORDER BY id"
            assert (
                node.query(query, settings={"mongodb_throw_on_unsupported_query": 0})
                == "1\n2\n3\n"
            )
            with pytest.raises(QueryRuntimeException):
                node.query(query, settings={"mongodb_throw_on_unsupported_query": 1})
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize(
    "column_type",
    [
        "UInt8",
        "Nullable(UInt8)",
        "Int128",
        "Nullable(Int128)",
        "Bool",
        "Nullable(Bool)",
    ],
)
def test_order_by_mixed_numeric_encodings(started_cluster, column_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "mixed_numeric_order"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many([{"id": 1, "n": True}, {"id": 2, "n": 2}])

    node = started_cluster.instances["node"]
    structure = f"id Int32, n {column_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        bound = "0" if "Bool" in column_type else "false"
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for direction, expected in (("ASC", "1\t1\n"), ("DESC", "2\t2\n")):
                for predicate in ("", f"WHERE n > {bound}"):
                    query = (
                        f"SELECT id, toUInt8(n) FROM {{}} {predicate} "
                        f"ORDER BY n {direction} LIMIT 1"
                    )
                    assert node.query(query.format(reference)) == expected
                    assert (
                        node.query(
                            query.format(source),
                            settings={"mongodb_throw_on_unsupported_query": 0},
                        )
                        == expected
                    )
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("bool_type", ["Bool", "Nullable(Bool)"])
@pytest.mark.parametrize("use_table_function", [False, True])
@pytest.mark.parametrize(
    "predicate, expected",
    [
        ("flag IN (1, 2.0) ORDER BY id", "1\n2\n3\n"),
        ("flag NOT IN (1, 2.0) ORDER BY id LIMIT 1", "4\n"),
    ],
)
def test_where_bool_membership_reader_values(
    started_cluster, bool_type, use_table_function, predicate, expected
):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "bool_membership_reader_values"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many(
        [
            {"id": 1, "flag": 3},
            {"id": 2, "flag": 2},
            {"id": 3, "flag": True},
            {"id": 4, "flag": False},
        ]
    )

    node = started_cluster.instances["node"]
    structure = f"id Int32, flag {bool_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    source = (
        f"mongodb({arguments}, '{structure}')"
        if use_table_function
        else collection_name
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        query = f"SELECT id FROM {{}} WHERE {predicate}"
        assert node.query(query.format(reference)) == expected
        assert (
            node.query(
                query.format(source),
                settings={"mongodb_throw_on_unsupported_query": 0},
            )
            == expected
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("bool_type", ["Bool", "Nullable(Bool)"])
def test_where_bool_default_settings(started_cluster, bool_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "bool_default_settings"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # The filter only decides the BSON bools. The other values come first, so a remote limit would drop the matches.
    collection.insert_many(
        [
            {"id": 1, "flag": 2},
            {"id": 2, "flag": 256},
            {"id": 3},
            {"id": 4, "flag": None},
            {"id": 5, "flag": "1"},
            {"id": 6, "flag": 0.5},
            {"id": 7, "flag": True},
            {"id": 8, "flag": False},
            {"id": 9, "flag": True},
        ]
    )

    node = started_cluster.instances["node"]
    structure = f"id Int32, flag {bool_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        predicates = [
            "flag = true",
            "flag = 1",
            "flag = 0",
            "flag != true",
            "flag > 0",
            "flag >= 1",
            "flag < 1",
            "flag <= 0.5",
            "0 < flag",
            "flag = 2",
            "flag != 2.0",
            "flag IN (true)",
            "flag IN (false, 2)",
            "flag IN (1, 2.0)",
            "flag NOT IN (true)",
            "flag NOT IN (0, 2)",
            "flag IN (reinterpret(char(2), 'Bool'))",
            "flag IN (reinterpret(char(2), 'Bool'), 2)",
            "flag NOT IN (reinterpret(char(2), 'Bool'))",
            "flag = true OR id = 8",
            "flag = true AND id > 7",
        ]
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for predicate in predicates:
                for query in (
                    "SELECT id FROM {} WHERE " + predicate + " ORDER BY id",
                    "SELECT count() FROM (SELECT id FROM {} WHERE "
                    + predicate
                    + " LIMIT 1)",
                ):
                    expected = node.query(query.format(reference))
                    for throw_on_unsupported_query in (0, 1):
                        assert (
                            node.query(
                                query.format(source),
                                settings={
                                    "mongodb_throw_on_unsupported_query": throw_on_unsupported_query
                                },
                            )
                            == expected
                        ), (query, source)

            assert (
                node.query(
                    f"SELECT count() FROM (SELECT id FROM {source} WHERE flag = true LIMIT 1)"
                )
                == "1\n"
            )
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


def test_where_nullable_negative_limit(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "nullable_negative_limit"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many(
        [
            {"id": 1, "s": "a"},
            {"id": 2, "s": "b", "v": None, "w": 1},
            {"id": 3, "s": "c", "v": "y", "w": "y"},
            {"id": 4, "s": "d", "v": "z", "w": "z"},
            {"id": 5, "s": "e", "v": "x", "w": "x"},
        ]
    )

    node = started_cluster.instances["node"]
    structure = "id Int32, s String, v Nullable(String), w String"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            # A missing `w` is read as '', and the number 1 as '1'.
            for predicate, first, second in (
                ("v != 'x'", "3", "4"),
                ("'x' != v", "3", "4"),
                ("v NOT IN ('x')", "3", "4"),
                ("w != ''", "2", "3"),
                ("w NOT IN ('', '1')", "3", "4"),
            ):
                for suffix, expected in (
                    ("ORDER BY s LIMIT 1", f"{first}\n"),
                    ("ORDER BY s LIMIT 1 OFFSET 1", f"{second}\n"),
                ):
                    query = f"SELECT id FROM {{}} WHERE {predicate} {suffix}"
                    assert node.query(query.format(reference)) == expected
                    assert node.query(query.format(source)) == expected
                query = (
                    f"SELECT count() FROM (SELECT id FROM {{}} "
                    f"WHERE {predicate} LIMIT 1)"
                )
                assert node.query(query.format(reference)) == "1\n"
                assert node.query(query.format(source)) == "1\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize(
    "column_type",
    ["UInt8", "Nullable(Int32)", "Int128", "Float32"],
)
def test_where_numeric_reader_values(started_cluster, column_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "numeric_reader_values"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # The reader wraps or truncates numbers, reads bools as 0 and 1, parses strings, and reads a missing field as the
    # default. The values that MongoDB compares differently come first, so a remote limit would drop the matches.
    collection.insert_many(
        [
            {"id": 1, "n": 256},
            {"id": 2, "n": False},
            {"id": 3, "n": -1},
            {"id": 4, "n": True},
            {"id": 5},
            {"id": 6, "n": "0"},
            {"id": 7, "n": 0.5},
            {"id": 8, "n": None},
            {"id": 9, "n": 1.9},
            {"id": 10, "n": -0.5},
            {"id": 11, "n": 300},
            {"id": 12, "n": 0.1},
            {"id": 13, "n": 16777217},
            {"id": 14, "n": "2"},
            {"id": 15, "n": 2**40},
            {"id": 16, "n": 0},
            {"id": 17, "n": 2},
            {"id": 18, "n": 1},
        ]
    )

    node = started_cluster.instances["node"]
    structure = f"id Int32, n {column_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        predicates = [
            "n = 0",
            "0 = n",
            "n = '0'",
            "n > 0",
            "n IN (0)",
            "n IN (0, 2)",
            "n != 0",
            "n NOT IN (0, 2)",
            "n < 1",
            "n <= 1",
            "n >= 2",
            "n = 1",
            "n = true",
            "n > 1.5",
            "n < 1.5",
            "n < 0.5",
            "n >= -1",
            "n = -1",
            "n = 255",
            "n = 44",
            "n BETWEEN 1 AND 2",
            "n = toFloat32(0.1)",
            "n = 16777216",
            "n = 0 OR id = 3",
            "n > 0 AND id > 2",
        ]
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for predicate in predicates:
                for query in (
                    "SELECT id FROM {} WHERE " + predicate + " ORDER BY id",
                    "SELECT count() FROM (SELECT id FROM {} WHERE "
                    + predicate
                    + " LIMIT 1)",
                ):
                    expected = node.query(query.format(reference))
                    for throw_on_unsupported_query in (0, 1):
                        assert (
                            node.query(
                                query.format(source),
                                settings={
                                    "mongodb_throw_on_unsupported_query": throw_on_unsupported_query
                                },
                            )
                            == expected
                        ), (query, source)
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("column_type", ["String", "Nullable(String)"])
def test_where_string_reader_values(started_cluster, column_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "string_reader_values"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # The reader writes every BSON value as text and reads a missing field as the default. The values that MongoDB
    # compares differently come first, so a remote limit would drop the matches.
    collection.insert_many(
        [
            {"id": 1, "s": 1},
            {"id": 2, "s": True},
            {"id": 3, "s": 1.5},
            {"id": 4, "s": bson.Int64(1)},
            {"id": 5},
            {"id": 6, "s": None},
            {"id": 7, "s": ["a"]},
            {"id": 8, "s": bson.Binary(b"a")},
            {"id": 9, "s": {"k": "a"}},
            {"id": 10, "s": bson.Decimal128("1.5")},
            {"id": 11, "s": datetime.datetime(2020, 1, 1)},
            {"id": 12, "s": bson.ObjectId("5f0000000000000000000000")},
            {"id": 13, "s": "1"},
            {"id": 14, "s": "a"},
            {"id": 15, "s": ""},
            {"id": 16, "s": "b"},
        ]
    )

    node = started_cluster.instances["node"]
    structure = f"id Int32, s {column_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        predicates = [
            "s = '1'",
            "'1' = s",
            "s = 'true'",
            "s = 'a'",
            "s = ''",
            "s = '1.5'",
            "s = '[\"a\"]'",
            "s IN ('[\"a\"]', 'b')",
            "s = '5f0000000000000000000000'",
            "s IN ('1', 'a')",
            "s != 'a'",
            "s NOT IN ('a', '1')",
            "s < 'b'",
            "s < '['",
            "s <= '['",
            "s < '[z'",
            "s >= '['",
            "s > '[z'",
            "s > 'z'",
            "s > '1'",
            "s >= 'a'",
            "s <= ''",
            "s > ''",
            "s IN ('', 'b')",
            "s = 'a' OR id = 3",
            "s > '0' AND id > 2",
        ]
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for predicate in predicates:
                for query in (
                    "SELECT id FROM {} WHERE " + predicate + " ORDER BY id",
                    "SELECT count() FROM (SELECT id FROM {} WHERE "
                    + predicate
                    + " LIMIT 1)",
                ):
                    expected = node.query(query.format(reference))
                    for throw_on_unsupported_query in (0, 1):
                        assert (
                            node.query(
                                query.format(source),
                                settings={
                                    "mongodb_throw_on_unsupported_query": throw_on_unsupported_query
                                },
                            )
                            == expected
                        ), (query, source)
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("with_symbol", [False, True])
def test_where_string_reader_index(started_cluster, with_symbol):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "string_reader_index"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many(
        [{"id": i, "s": f"v{i:06d}"} for i in range(2000)]
        + [{"id": 2000, "s": ["unrelated"]}]
    )
    if with_symbol:
        # `PyMongo` has no symbol encoder. Append a BSON symbol to a raw document.
        document = (
            bson.BSON.encode({"id": 2001})[4:-1]
            + b"\x0es\x00\x02\x00\x00\x00z\x00\x00"
        )
        collection.insert_one(
            RawBSONDocument((len(document) + 4).to_bytes(4, "little") + document)
        )
    collection.create_index("s")

    node = started_cluster.instances["node"]
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} (id Int32, s String) "
        f"ENGINE = MongoDB({arguments})"
    )
    previous_profile = db.command("profile", -1)
    try:
        for source in (
            collection_name,
            f"mongodb({arguments}, 'id Int32, s String')",
        ):
            try:
                db.command("profile", 2)
                assert (
                    node.query(f"SELECT id FROM {source} WHERE s = 'v000123'")
                    == "123\n"
                )
            finally:
                db.command(
                    "profile",
                    previous_profile["was"],
                    slowms=previous_profile["slowms"],
                    sampleRate=previous_profile["sampleRate"],
                    filter=previous_profile.get("filter", "unset"),
                )
            profile = db["system.profile"].find_one(
                {"ns": f"test.{collection_name}", "command.find": collection_name},
                sort=[("$natural", -1)],
            )
            assert profile is not None
            assert profile["keysExamined"] < 20, profile
            assert profile["docsExamined"] < 20, profile
    finally:
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


@pytest.mark.parametrize("column_type", ["Int64", "Nullable(Int64)", "String"])
def test_where_noncanonical_bool_constants(started_cluster, column_type):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "noncanonical_bool_constants"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many(
        [
            {"id": 1, "n": 0},
            {"id": 2, "n": True},
            {"id": 3, "n": 1},
            {"id": 4, "n": 2},
        ]
    )

    node = started_cluster.instances["node"]
    structure = f"id Int32, n {column_type}"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        predicates = [
            "n IN (0, reinterpret(char(2), 'Bool'))",
            "n IN (reinterpret(char(2), 'Bool'), 0)",
            "n NOT IN (0, reinterpret(char(2), 'Bool'))",
            "n IN (reinterpret(char(2), 'Bool'))",
        ]
        if column_type != "String":
            predicates += [
                "n = reinterpret(char(2), 'Bool')",
                "n != reinterpret(char(2), 'Bool')",
                "reinterpret(char(2), 'Bool') = n",
            ]
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for predicate in predicates:
                for suffix in ("ORDER BY id", "LIMIT 1"):
                    query = f"SELECT id FROM {{}} WHERE {predicate} {suffix}"
                    assert node.query(
                        query.format(source),
                        settings={"mongodb_throw_on_unsupported_query": 0},
                    ) == node.query(query.format(reference)), (query, source)
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


def test_where_nullable_null_limit(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "nullable_null_limit"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    collection.insert_many(
        [
            {"n": None, "a": []} if i % 2 == 0 else {"n": 1, "a": [None]}
            for i in range(100)
        ]
    )

    node = started_cluster.instances["node"]
    structure = "n Nullable(Int64), a Array(Nullable(Int32))"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for predicate, expected in (
                ("isNull(n)", "\\N\n"),
                ("isNotNull(n)", "1\n"),
                ("empty(a)", "\\N\n"),
                ("notEmpty(a)", "1\n"),
            ):
                query_id = str(uuid.uuid4())
                assert node.query(
                    f"SELECT n FROM {source} WHERE {predicate} LIMIT 1",
                    query_id=query_id,
                    settings={"log_queries": 1, "max_block_size": 100},
                ) == expected
                node.query("SYSTEM FLUSH LOGS")
                assert node.query(
                    "SELECT read_rows FROM system.query_log "
                    f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
                ) == "1\n"
    finally:
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


def test_order_by_reader_values_limit(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "order_by_reader_values_limit"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # MongoDB sorts a `null` first and a number before a string, but the reader reads `null` as `NULL` and 10 as '10'.
    collection.insert_many([{"id": 1, "s": None, "t": 10}, {"id": 2, "s": "a", "t": "0"}])

    node = started_cluster.instances["node"]
    structure = "id UInt8, s Nullable(String), t String"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for order_by, expected in (
                ("s", "2\n"),
                ("s DESC NULLS FIRST", "1\n"),
                ("t", "2\n"),
                ("t DESC", "1\n"),
            ):
                query = f"SELECT id FROM {{}} ORDER BY {order_by} LIMIT 1"
                assert node.query(query.format(reference)) == expected
                assert node.query(query.format(source)) == expected, (query, source)
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


def test_where_unary_predicates(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    collection_name = "unary_predicates"
    drop_mongo_collection_if_exists(db, collection_name)
    collection = db[collection_name]
    # An empty binary is read as '', the arrays in `v` and `w` as non-empty strings, and `[None]` in `a` as `[0]`.
    collection.insert_many(
        [
            {"id": 1, "s": "a", "v": bson.Binary(b""), "w": [None], "a": [None]},
            {"id": 2, "s": "b", "v": "x", "w": "x", "a": []},
            {"id": 3, "s": "c", "v": [""], "w": None, "a": [1]},
            {"id": 4, "s": "d", "v": "", "a": None},
            {"id": 5, "s": "e", "v": ""},
        ]
    )
    # A BSON `undefined` in `v` and `w` is read as 'undefined', and `$nin: [null]` does not match it.
    # PyMongo cannot encode `undefined`, so the document is written as raw BSON.
    elements = bson.encode({"id": 6, "s": "f"})[4:-1] + b"\x06v\x00\x06w\x00"
    collection.insert_one(
        RawBSONDocument((len(elements) + 5).to_bytes(4, "little") + elements + b"\x00")
    )

    node = started_cluster.instances["node"]
    structure = "id Int32, s String, v String, w Nullable(String), a Array(Int32)"
    arguments = f"'mongo1:27017', 'test', '{collection_name}', 'root', '{mongo_pass}'"
    reference = f"{collection_name}_reference"
    node.query(
        f"CREATE OR REPLACE TABLE {collection_name} ({structure}) "
        f"ENGINE = MongoDB({arguments})"
    )
    try:
        node.query(
            f"CREATE OR REPLACE TABLE {reference} ENGINE = Memory "
            f"AS SELECT * FROM {collection_name}"
        )
        for source in (collection_name, f"mongodb({arguments}, '{structure}')"):
            for predicate, expected in (
                ("empty(v)", "1\n4\n5\n"),
                ("notEmpty(v)", "2\n3\n6\n"),
                ("v != ''", "2\n3\n6\n"),
                ("isNull(w)", "3\n4\n5\n"),
                ("isNotNull(w)", "1\n2\n6\n"),
                ("empty(a)", "2\n4\n5\n6\n"),
                ("notEmpty(a)", "1\n3\n"),
            ):
                for suffix, result in (
                    ("ORDER BY id", expected),
                    ("ORDER BY s LIMIT 1", expected.split("\n")[0] + "\n"),
                    ("LIMIT 1", expected.split("\n")[0] + "\n"),
                ):
                    query = f"SELECT id FROM {{}} WHERE {predicate} {suffix}"
                    assert node.query(query.format(reference)) == result
                    assert node.query(query.format(source)) == result, (query, source)
    finally:
        node.query(f"DROP TABLE IF EXISTS {reference}")
        node.query(f"DROP TABLE {collection_name}")
        collection.drop()


def test_defaults(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "defaults_table")
    defaults_mongo_table = db["defaults_table"]
    defaults_mongo_table.insert_one({"key": "key"})

    node = started_cluster.instances["node"]
    node.query(
        f"""
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
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'defaults_table', 'root', '{mongo_pass}')
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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "nulls_table")
    nulls_mongo_table = db["nulls_table"]
    nulls_mongo_table.insert_one({"key": "key"})

    node = started_cluster.instances["node"]
    node.query(
        f"""
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
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'nulls_table', 'root', '{mongo_pass}')
        """
    )

    assert node.query("SELECT COUNT() FROM nulls_table") == "1\n"
    # A date-family constant is pushed down against a `Nullable` column as against a plain one.
    assert node.query("SELECT COUNT() FROM nulls_table WHERE k_datetime = toDate('2024-01-02')") == "0\n"

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
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "oid_table")
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
    node.query(
        f"""
        CREATE OR REPLACE TABLE oid_table(
        _id  String,
        key  String
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'oid_table', 'root', '{mongo_pass}')
        """
    )

    assert node.query("SELECT COUNT() FROM oid_table") == "3\n"

    assert node.query(f"SELECT _id FROM oid_table WHERE _id = '{str(ids[0])}'") == f"{str(ids[0])}\n"
    assert node.query(f"SELECT key FROM oid_table WHERE _id = '{str(ids[0])}'") == "oid1\n"
    assert (node.query(f"SELECT key FROM oid_table WHERE _id != '{str(ids[0])}' ORDER BY key") ==
            "oid2\noid3\n")
    assert node.query(f"SELECT key FROM oid_table WHERE _id in ['{ids[0]}', '{ids[1]}'] ORDER BY key") == "oid1\noid2\n"
    assert node.query(f"SELECT key FROM oid_table WHERE _id not in ['{ids[0]}', '{ids[1]}'] ORDER BY key") == "oid3\n"

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id = 'not-oid'")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id != 'not-oid'")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id = 1234567")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE _id != 1234567")
    with pytest.raises(QueryRuntimeException):
        node.query(f"SELECT key FROM oid_table WHERE _id in ['{ids[0]}', 'not-oid']")
    with pytest.raises(QueryRuntimeException):
        node.query(f"SELECT key FROM oid_table WHERE _id not in ['{ids[0]}', 'not-oid']")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT key FROM oid_table WHERE _id in ['nope', 'not-oid']")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT key FROM oid_table WHERE _id not in ['nope', 'not-oid']")

    node.query(
        """
        CREATE OR REPLACE TABLE oid_table(
        _id  String,
        key  String
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'oid_table', 'root', 'clickhouse', '', 'key')
        """
    )
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE key = 'not-oid'")

    node.query(
        """
        CREATE OR REPLACE TABLE oid_table(
        _id  String,
        key  String
        ) ENGINE = MongoDB('mongodb://mongo1:27017/test', 'oid_table', 'key')
        """
    )
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM oid_table WHERE key = 'not-oid'")

    node.query("DROP TABLE oid_table")
    oid_mongo_table.drop()


def test_uuid(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "uuid_table")
    uuid_mongo_table = db["uuid_table"]
    uuid_mongo_table.insert_many(
        [
            {"isValid": 0, "kUUID": "bad_uuid_string"},
            {
                "isValid": 1,
                "kUUID": bson.Binary(
                    uuid.UUID("f0e77736-91d1-48ce-8f01-15123ca1c7ed").bytes, subtype=0
                ),
            },
            {
                "isValid": 2,
                "kUUID": bson.Binary(
                    uuid.UUID("f0e77736-91d1-48ce-8f01-15123ca1c7ed").bytes, subtype=4
                ),
            },
        ]
    )

    node = started_cluster.instances["node"]
    node.query(
        f"""
        CREATE OR REPLACE TABLE uuid_table(
        isValid UInt8,
        kUUID   UUID
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'uuid_table', 'root', '{mongo_pass}')
        """
    )

    assert node.query("SELECT kUUID FROM uuid_table WHERE isValid = 2") == "f0e77736-91d1-48ce-8f01-15123ca1c7ed\n"

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM uuid_table WHERE isValid = 0")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM uuid_table WHERE isValid = 1")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT * FROM uuid_table")

    node.query("DROP TABLE uuid_table")
    uuid_mongo_table.drop()


def test_no_fail_on_unsupported_clauses(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "unsupported_clauses")
    unsupported_clauses_table = db["unsupported_clauses"]

    node = started_cluster.instances["node"]
    node.query(
        f"""
        CREATE OR REPLACE TABLE unsupported_clauses(
        a UInt64,
        b UInt64
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'unsupported_clauses', 'root', '{mongo_pass}')
        """
    )

    node.query(
        "SELECT * FROM unsupported_clauses WHERE a > rand() SETTINGS mongodb_throw_on_unsupported_query = 0"
    )
    node.query(
        "SELECT * FROM unsupported_clauses WHERE a / 1000 > 0 SETTINGS mongodb_throw_on_unsupported_query = 0"
    )
    node.query(
        "SELECT * FROM unsupported_clauses WHERE toFloat64(a) < 6.66 > rand() SETTINGS mongodb_throw_on_unsupported_query = 0"
    )
    node.query(
        "SELECT * FROM unsupported_clauses ORDER BY a, b LIMIT 2 BY a SETTINGS mongodb_throw_on_unsupported_query = 0"
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


def test_json_serialization(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "json_serialization_table")
    json_serialization_table = db["json_serialization_table"]

    date = datetime.datetime.strptime("2025-05-17 13:14:15", "%Y-%m-%d %H:%M:%S")

    def create_dataset(mongo, level) -> dict:
        return {
            "type_string": "Type string",
            "type_oid": bson.ObjectId("60f7e65e16b1c1d1c8a2b6b3") if mongo else "60f7e65e16b1c1d1c8a2b6b3",
            "type_binary": bson.Binary(b"binarydata", subtype=0) if mongo else base64.b64encode(b"binarydata").decode(),
            "type_bool": True,
            "type_int32": 123,
            "type_int64": bson.int64.Int64(2**63 - 1) if mongo else int(2**63 - 1),
            "type_double": float(3.141592653589793238),
            "type_date": date if mongo else date.strftime("%Y-%m-%d %H:%M:%S"),
            "type_timestamp": bson.timestamp.Timestamp(date, 1) if mongo else date.strftime("%Y-%m-%d %H:%M:%S"),
            "type_document": {"nested_doc": create_dataset(mongo, level - 1)} if level > 0 else {},
            "type_array": [create_dataset(mongo, level - 1)] if level > 0 else [],
            "type_regex": bson.regex.Regex(r"^pattern.*$", "i") if mongo else {"^pattern.*$": "i"},
            "type_null": None,
        }

    json_serialization_table.insert_one({"dataset": create_dataset(True, 10)})
    node = started_cluster.instances["node"]
    node.query(
        f"""
        CREATE OR REPLACE TABLE json_serialization_table(
            dataset String
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'json_serialization_table', 'root', '{mongo_pass}')
        """
    )

    assert node.query("SELECT COUNT() FROM json_serialization_table") == "1\n"
    assert (node.query("SELECT dataset FROM json_serialization_table")[:-1]
            == json.dumps(create_dataset(False, 10), separators=(',', ':')))

    node.query("DROP TABLE json_serialization_table")
    json_serialization_table.drop()


def test_numbers_parsing(started_cluster):
    cases = [
        {"type": "Int16", "value": "32767", "ok": True},
        {"type": "Int16", "value": "-32768", "ok": True},
        {"type": "Int16", "value": "32767.0", "ok": False},
        {"type": "Int16", "value": "-32768.0", "ok": False},
        {"type": "Int16", "value": "12.5", "ok": False},
        {"type": "Int16", "value": "-0.1", "ok": False},
        {"type": "Int16", "value": "abc", "ok": False},

        {"type": "UInt16", "value": "65535", "ok": True},
        {"type": "UInt16", "value": "0", "ok": True},
        {"type": "UInt16", "value": "65535.0", "ok": False},
        {"type": "UInt16", "value": "0.0", "ok": False},
        {"type": "UInt16", "value": "12.34", "ok": False},
        {"type": "UInt16", "value": "-1.0", "ok": False},
        {"type": "UInt16", "value": "-32768", "ok": False},

        {"type": "Int32", "value": "2147483647", "ok": True},
        {"type": "Int32", "value": "-2147483648", "ok": True},
        {"type": "Int32", "value": "2147483647.0", "ok": False},
        {"type": "Int32", "value": "-2147483648.0", "ok": False},
        {"type": "Int32", "value": "12.5", "ok": False},
        {"type": "Int32", "value": "-0.1", "ok": False},
        {"type": "Int32", "value": "abc", "ok": False},

        {"type": "UInt32", "value": "4294967295", "ok": True},
        {"type": "UInt32", "value": "0", "ok": True},
        {"type": "UInt32", "value": "4294967295.0", "ok": False},
        {"type": "UInt32", "value": "0.0", "ok": False},
        {"type": "UInt32", "value": "12.34", "ok": False},
        {"type": "UInt32", "value": "-1.0", "ok": False},
        {"type": "UInt32", "value": "-2147483648", "ok": False},

        {"type": "Int64", "value": "9223372036854775807", "ok": True},
        {"type": "Int64", "value": "-9223372036854775808", "ok": True},
        {"type": "Int64", "value": "9223372036854775807.0", "ok": False},
        {"type": "Int64", "value": "-9223372036854775808.0", "ok": False},
        {"type": "Int64", "value": "12.5", "ok": False},
        {"type": "Int64", "value": "-0.1", "ok": False},
        {"type": "Int64", "value": "abc", "ok": False},

        {"type": "UInt64", "value": "18446744073709551615", "ok": True},
        {"type": "UInt64", "value": "0", "ok": True},
        {"type": "UInt64", "value": "18446744073709551615.0", "ok": False},
        {"type": "UInt64", "value": "0.0", "ok": False},
        {"type": "UInt64", "value": "12.34", "ok": False},
        {"type": "UInt64", "value": "-1.0", "ok": False},
        {"type": "UInt64", "value": "-9223372036854775808", "ok": False},

        {"type": "Int128", "value": "170141183460469231731687303715884105727",  "ok": True},
        {"type": "Int128", "value": "-170141183460469231731687303715884105728", "ok": True},
        {"type": "Int128", "value": "170141183460469231731687303715884105727.0", "ok": False},
        {"type": "Int128", "value": "-170141183460469231731687303715884105728.0", "ok": False},
        {"type": "Int128", "value": "12.5", "ok": False},
        {"type": "Int128", "value": "-0.1", "ok": False},
        {"type": "Int128", "value": "abc", "ok": False},

        {"type": "UInt128", "value": "340282366920938463463374607431768211455", "ok": True},
        {"type": "UInt128", "value": "0", "ok": True},
        {"type": "UInt128", "value": "340282366920938463463374607431768211455.0", "ok": False},
        {"type": "UInt128", "value": "0.0", "ok": False},
        {"type": "UInt128", "value": "12.34", "ok": False},
        {"type": "UInt128", "value": "-1.0", "ok": False},
        {"type": "UInt128", "value": "-170141183460469231731687303715884105728", "ok": False},

        {"type": "Int256", "value": "57896044618658097711785492504343953926634992332820282019728792003956564819967", "ok": True},
        {"type": "Int256", "value": "-57896044618658097711785492504343953926634992332820282019728792003956564819968", "ok": True},
        {"type": "Int256", "value": "57896044618658097711785492504343953926634992332820282019728792003956564819967.0", "ok": False},
        {"type": "Int256", "value": "-57896044618658097711785492504343953926634992332820282019728792003956564819968.0", "ok": False},
        {"type": "Int256", "value": "12.5", "ok": False},
        {"type": "Int256", "value": "-0.1", "ok": False},
        {"type": "Int256", "value": "abc", "ok": False},

        {"type": "UInt256", "value": "115792089237316195423570985008687907853269984665640564039457584007913129639935", "ok": True},
        {"type": "UInt256", "value": "0", "ok": True},
        {"type": "UInt256", "value": "115792089237316195423570985008687907853269984665640564039457584007913129639935.0", "ok": False},
        {"type": "UInt256", "value": "0.0", "ok": False},
        {"type": "UInt256", "value": "12.34", "ok": False},
        {"type": "UInt256", "value": "-1.0", "ok": False},
        {"type": "UInt256", "value": "-57896044618658097711785492504343953926634992332820282019728792003956564819968", "ok": False},

        {"type": "Float32", "value": "3.14",     "ok": True},
        {"type": "Float32", "value": "-2.71",    "ok": True},
        {"type": "Float32", "value": "0.0",      "ok": True},
        {"type": "Float32", "value": "-0.0",     "ok": True},
        {"type": "Float32", "value": "1e38",     "ok": True},
        {"type": "Float32", "value": "-1e38",    "ok": True},
        {"type": "Float32", "value": "abc",      "ok": False},
        {"type": "Float32", "value": "",         "ok": False},

        {"type": "Float64", "value": "3.14",     "ok": True},
        {"type": "Float64", "value": "-2.71",    "ok": True},
        {"type": "Float64", "value": "0.0",      "ok": True},
        {"type": "Float64", "value": "-0.0",     "ok": True},
        {"type": "Float64", "value": "1e308",    "ok": True},
        {"type": "Float64", "value": "-1e308",   "ok": True},
        {"type": "Float64", "value": "abc",      "ok": False},
        {"type": "Float64", "value": "",         "ok": False},
    ]

    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "numbers_parsing_table")
    numbers_parsing_table = db["numbers_parsing_table"]

    node = started_cluster.instances["node"]
    node.query(
        f"""
        CREATE OR REPLACE TABLE numbers_parsing_table(
            caseNum UInt8,
            
            Int8_ Int8,
            UInt8_ UInt8,
            Int16_ Int16,
            UInt16_ UInt16,
            Int32_ Int32,
            UInt32_ UInt32,
            Int64_ Int64,
            UInt64_ UInt64,
            Int128_ Int128,
            UInt128_ UInt128,
            Int256_ Int256,
            UInt256_ UInt256,
            Float32_ Float32,
            Float64_ Float64
        ) ENGINE = MongoDB('mongo1:27017', 'test', 'numbers_parsing_table', 'root', '{mongo_pass}')
        """
    )

    for num, case in enumerate(cases):
        numbers_parsing_table.insert_one({"caseNum": num, f"{case['type']}_": case['value']})
        query = f"SELECT {case['type']}_ FROM numbers_parsing_table WHERE caseNum = {num}"
        if case['ok']:
            if case['value'] in ["0.0"]:
                assert node.query(query) == "0\n"
            elif case['value'] in ["-0.0"]:
                assert node.query(query) == "-0\n"
            else:
                assert node.query(query) == f"{case['value']}\n"
        else:
            with pytest.raises(QueryRuntimeException):
                node.query(query)

    node.query("DROP TABLE numbers_parsing_table")
    numbers_parsing_table.drop()


def test_url_validation(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root@aa.com", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "url_validation_table")
    url_validation_table = db["url_validation_table"]
    data = []
    for i in range(0, 100):
        data.append({"key": i, "data": hex(i * i)})
    url_validation_table.insert_many(data)

    node = started_cluster.instances["node"]
    node.query(
        f"CREATE OR REPLACE TABLE url_validation_table(key UInt64, data String) ENGINE = MongoDB('mongo1', 'test', 'url_validation_table', 'root@aa.com', '{mongo_pass}')"
    )

    assert node.query("SELECT COUNT() FROM url_validation_table") == "100\n"


def test_handshake_metadata(started_cluster):
    # ClickHouse identifies itself in the MongoDB connection handshake as a wrapping library
    # (https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.md#wrapping-libraries),
    # so that MongoDB server operators can tell ClickHouse connections apart in mongod/mongos logs.
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])
    drop_mongo_collection_if_exists(db, "handshake_metadata_table")
    handshake_metadata_table = db["handshake_metadata_table"]
    handshake_metadata_table.insert_many([{"key": i} for i in range(0, 10)])

    node = started_cluster.instances["node"]
    clickhouse_version = node.query("SELECT version()").strip()

    node.query(
        f"CREATE OR REPLACE TABLE handshake_metadata_table(key UInt64) ENGINE = MongoDB('mongo1', 'test', 'handshake_metadata_table', 'root', '{mongo_pass}')"
    )

    # mongod logs the metadata received in the handshake of every new connection as a JSON line
    # with `"msg": "client metadata"` and the handshake document under `attr.doc`.
    # Earlier tests in this module have already opened ClickHouse connections to mongo1, so
    # snapshot the container log first and only look at what gets appended by the SELECT below.
    logs_before = started_cluster.get_container_logs("mongo1")

    assert node.query("SELECT COUNT() FROM handshake_metadata_table") == "10\n"

    def clickhouse_handshakes_since_snapshot():
        new_logs = started_cluster.get_container_logs("mongo1")[len(logs_before) :]
        drivers = []
        for line in new_logs.splitlines():
            if '"client metadata"' not in line:
                continue
            driver = json.loads(line)["attr"]["doc"]["driver"]
            # Connections from other clients (e.g. pymongo) are logged too.
            if driver["name"] == "mongoc / mongocxx / ClickHouse":
                drivers.append(driver)
        return drivers

    # Container logs are observed eventually consistently, so retry until the handshake
    # of the connection opened by the SELECT above shows up.
    drivers = wait_condition(
        clickhouse_handshakes_since_snapshot,
        lambda drivers: len(drivers) > 0,
        max_attempts=50,
        delay=0.2,
    )
    assert all(
        driver["version"].endswith(f" / {clickhouse_version}") for driver in drivers
    ), drivers

    node.query("DROP TABLE handshake_metadata_table")
    handshake_metadata_table.drop()

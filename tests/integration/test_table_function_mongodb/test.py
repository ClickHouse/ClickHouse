import pymongo

import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster
import datetime


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            "node",
            with_mongo=True,
            main_configs=[
                "configs_secure/config.d/ssl_conf.xml",
            ],
            user_configs=["configs/users.xml"],
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
            "SELECT COUNT() FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='ssl=true')"
        )
        == "100\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='ssl=true')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            "SELECT sum(key) FROM mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', 'key UInt64, data String', 'ssl=true')"
        )
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query(
            "SELECT data from mongodb('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='ssl=true') where key = 42"
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
            "SELECT count() FROM mongodb('mongo2:27017', 'test', 'simple_table', '', '', structure='key UInt64, data String')"
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
        "SELECT count() FROM mongodb('mongo2:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String')"
    )

    assert (
        node.query(
            "SELECT count() FROM mongodb('mongo2:27017', 'test', 'simple_table', 'root', 'clickhouse', structure='key UInt64, data String', options='authSource=admin')"
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


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_schema_inference(started_cluster):
    mongo_connection = get_mongo_connection(started_cluster)
    db = mongo_connection["test"]
    db.add_user("root", "clickhouse")
    inference_mongo_table = db["inference_table"]
    data = []
    for i in range(0, 100):
        data.append(
            {
                "key": i,
                "int64": -(i + 4294967295),
                "int32": -(i + 1),
                "int16": -(i + 1),
                "int8": -(i + 1),
                "uint64": i + 4294967295,
                "uint32": i + 1,
                "uint16": i + 1,
                "uint8": i + 1,
                "float32": i + 3.750,
                "float64": i + 3.750,
                "date": datetime.datetime(2002, 10, 27),
                "datetime": datetime.datetime(2023, 3, 31, 6, 3, 12),
                "string": str(i + 1),
                "uuid": "f0e77736-91d1-48ce-8f01-15123ca1c7ed",
                "bool": True,
                "arr_float64": [i + 1.125, i + 2.5, i + 3.750],
                "arr_datetime": [
                    datetime.datetime(2023, 3, 31, 6, 3, 12),
                    datetime.datetime(1999, 2, 28, 12, 46, 34),
                ],
                "tuple": {
                    "float": i + 3.750,
                    "tuple": {
                        "datetime": datetime.datetime(2023, 3, 31, 6, 3, 12),
                        "string": str(i + 1),
                    },
                    "array": [True, False, True],
                },
                "arr_tuple_arr": [
                    {
                        "int": i,
                        "array": [
                            {
                                "float32": i + 3.750,
                                "date": datetime.datetime(2002, 10, 27),
                            },
                            {
                                "float32": i + 3.750,
                                "date": datetime.datetime(2002, 10, 27),
                            },
                        ],
                    },
                    {
                        "int": i,
                        "array": [
                            {
                                "float32": i + 3.750,
                                "date": datetime.datetime(2002, 10, 27),
                            },
                            {
                                "float32": i + 3.750,
                                "date": datetime.datetime(2002, 10, 27),
                            },
                        ],
                    },
                ],
                "nullable_int": i if i % 2 == 0 else None,
            }
        )

    inference_mongo_table.insert_many(data)

    node = started_cluster.instances["node"]
    result = node.query(
        "SELECT count() FROM mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse')"
    )
    assert result == "100\n"

    table_description = "_id\tString\t\t\t\t\t\nkey\tInt32\t\t\t\t\t\nint64\tInt64\t\t\t\t\t\nint32\tInt32\t\t\t\t\t\nint16\tInt32\t\t\t\t\t\nint8\tInt32\t\t\t\t\t\nuint64\tInt64\t\t\t\t\t\nuint32\tInt32\t\t\t\t\t\nuint16\tInt32\t\t\t\t\t\nuint8\tInt32\t\t\t\t\t\nfloat32\tFloat64\t\t\t\t\t\nfloat64\tFloat64\t\t\t\t\t\ndate\tDateTime\t\t\t\t\t\ndatetime\tDateTime\t\t\t\t\t\nstring\tString\t\t\t\t\t\nuuid\tString\t\t\t\t\t\nbool\tBool\t\t\t\t\t\narr_float64\tArray(Float64)\t\t\t\t\t\narr_datetime\tArray(DateTime)\t\t\t\t\t\ntuple\tTuple(float Float64, tuple Tuple(datetime DateTime, string String), array Array(Bool))\t\t\t\t\t\narr_tuple_arr\tArray(Tuple(int Int32, array Array(Tuple(float32 Float64, date DateTime))))\t\t\t\t\t\nnullable_int\tInt32\t\t\t\t\t\n"
    assert table_description == node.query(
        "DESCRIBE TABLE mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse')"
    )
    assert table_description == node.query(
        "DESCRIBE TABLE mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse', 'auto')"
    )
    assert table_description == node.query(
        "DESCRIBE TABLE mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse', structure='auto')"
    )

    table_description_subset = "int64\tInt64\t\t\t\t\t\n"
    assert table_description_subset == node.query(
        "DESCRIBE TABLE mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse', structure='int64 Int64')"
    )

    node.query(
        "INSERT INTO FUNCTION mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse') SELECT * FROM mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse')"
    )

    result = node.query(
        "SELECT count() FROM mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse')"
    )
    assert result == "200\n"

    result = node.query(
        "SELECT count(DISTINCT *) FROM mongodb('mongo1:27017', 'test', 'inference_table', 'root', 'clickhouse')"
    )
    assert result == "100\n"

    inference_mongo_table.drop()

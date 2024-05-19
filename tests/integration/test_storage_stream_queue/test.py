import pymongo

import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster
import datetime
import time


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
            user_configs=["configs/users.xml"],
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

    node = started_cluster.instances["node"]
    node.query(
        "CREATE TABLE simple_mongo_table(id UInt32, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse')"
    )
    node.query(
        "CREATE TABLE simple_stream_queue_table(id UInt32, data String) ENGINE = StreamQueue(simple_mongo_table, id)"
    )
    node.query("CREATE TABLE simple_result(id UInt32, data String) ORDER BY id")
    node.query(
        "CREATE MATERIALIZED VIEW simple_consumer TO simple_result AS SELECT id, data FROM simple_stream_queue_table;"
    )

    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})
    simple_mongo_table.insert_many(data)

    time.sleep(3)

    assert node.query("SELECT COUNT() FROM simple_result") == "100\n"
    assert (
        node.query("SELECT sum(id) FROM simple_result")
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query("SELECT data from simple_result where key = 42")
        == hex(42 * 42) + "\n"
    )
    node.query("DROP TABLE simple_consumer")
    node.query("DROP TABLE simple_stream_queue_table")
    node.query("DROP TABLE simple_result")
    node.query("DROP TABLE simple_mongo_table")
    simple_mongo_table.drop()

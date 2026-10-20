from __future__ import print_function

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/zookeeper_config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_fetch_partition_with_outdated_parts(start_cluster):
    node.query(
        "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )
    node.query("INSERT INTO simple VALUES ('2020-08-27', 1)")

    node.query(
        "CREATE TABLE simple2 (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )
    node.query("INSERT INTO simple2 VALUES ('2020-08-27', 2)")
    node.query("INSERT INTO simple2 VALUES ('2020-08-27', 3)")
    node.query("OPTIMIZE TABLE simple2 FINAL")

    # until now both tables will have the same part

    node.query(
        "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zookeeper2:/clickhouse/tables/0/simple';"
    )

    node.query("ALTER TABLE simple2 ATTACH PARTITION '2020-08-27';")

    assert node.query("SELECT id FROM simple2 order by id").strip() == "1\n2\n3"


def test_missing_part_error_includes_table_name(start_cluster):
    node.query("CREATE DATABASE parts_exchange_errors")
    node.query(
        "CREATE TABLE parts_exchange_errors.source (id UInt32) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/parts_exchange_errors/source', 'node') "
        "ORDER BY id"
    )

    response = requests.post(
        f"http://{node.ip_address}:9009/",
        params={
            "endpoint": "DataPartsExchange:/clickhouse/tables/parts_exchange_errors/source/replicas/node",
            "part": "all_0_0_0",
            "compress": "false",
        },
        timeout=10,
    )

    assert response.status_code == 500
    assert "No part all_0_0_0 in table parts_exchange_errors.source" in response.text

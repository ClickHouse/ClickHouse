import time
import pytest
import os

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_reload_auxiliary_zookeepers(start_cluster):
    node.query(
        "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )
    node.query("INSERT INTO simple VALUES ('2020-08-27', 1)")

    node.query(
        "CREATE TABLE simple2 (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )

    # Add an auxiliary zookeeper
    new_config = """<clickhouse>
    <zookeeper>
        <implementation>fdbkeeper</implementation>
        <fdb_prefix>fdbkeeper</fdb_prefix>
        <fdb_cluster>/etc/foundationdb/fdb.cluster</fdb_cluster>
    </zookeeper>
    <auxiliary_zookeepers>
        <zookeeper2>
            <implementation>fdbkeeper</implementation>
            <fdb_prefix>fdbkeeper</fdb_prefix>
            <fdb_cluster>/etc/foundationdb/fdb.cluster</fdb_cluster>
        </zookeeper2>
    </auxiliary_zookeepers>
</clickhouse>"""
    node.replace_config(
        "/etc/clickhouse-server/conf.d/fdb_config.xml", new_config
    )

    node.query("SYSTEM RELOAD CONFIG")

    time.sleep(5)

    node.query(
        "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zookeeper2:/clickhouse/tables/0/simple';"
    )
    node.query("ALTER TABLE simple2 ATTACH PARTITION '2020-08-27';")
    assert node.query("SELECT id FROM simple2").strip() == "1"

    new_config = """<clickhouse>
    <zookeeper>
        <implementation>fdbkeeper</implementation>
        <fdb_prefix>fdbkeeper</fdb_prefix>
        <fdb_cluster>/etc/foundationdb/fdb.cluster</fdb_cluster>
    </zookeeper>
</clickhouse>"""
    node.replace_config(
        "/etc/clickhouse-server/conf.d/fdb_config.xml", new_config
    )
    node.query("SYSTEM RELOAD CONFIG")
    time.sleep(5)

    with pytest.raises(QueryRuntimeException):
        node.query(
            "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zookeeper2:/clickhouse/tables/0/simple';"
        )
    assert node.query("SELECT id FROM simple2").strip() == "1"

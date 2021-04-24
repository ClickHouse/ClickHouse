import time
import pytest
import os

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")
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
    new_config = """<yandex>
    <zookeeper>
        <node index="1">
            <host>zoo1</host>
            <port>2181</port>
        </node>
        <node index="2">
            <host>zoo2</host>
            <port>2181</port>
        </node>
            <node index="3">
            <host>zoo3</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
    <auxiliary_zookeepers>
        <zookeeper2>
            <node index="1">
                <host>zoo1</host>
                <port>2181</port>
            </node>
            <node index="2">
                <host>zoo2</host>
                <port>2181</port>
            </node>
        </zookeeper2>
    </auxiliary_zookeepers>
</yandex>"""
    node.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)

    # Hopefully it has finished the configuration reload
    time.sleep(2)

    node.query(
        "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zookeeper2:/clickhouse/tables/0/simple';"
    )
    node.query("ALTER TABLE simple2 ATTACH PARTITION '2020-08-27';")
    assert node.query("SELECT id FROM simple2").strip() == "1"

    new_config = """<yandex>
    <zookeeper>
        <node index="1">
            <host>zoo2</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</yandex>"""
    node.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
    time.sleep(2)
    with pytest.raises(QueryRuntimeException):
        node.query(
            "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zookeeper2:/clickhouse/tables/0/simple';"
        )
    assert node.query("SELECT id FROM simple2").strip() == "1"

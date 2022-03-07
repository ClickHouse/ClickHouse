import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="ast_rewrite")
nodes = [
    cluster.add_instance(
        name='node1', with_zookeeper=True,
        macros={"shard": 1, "replica": 1}, main_configs=["configs/ast_rewrite.xml"],
        image='yandex/clickhouse-server', tag='21.1.9', with_installed_binary=True,

    ),
    cluster.add_instance(
        name='node2', with_zookeeper=True,
        macros={"shard": 2, "replica": 1}, main_configs=["configs/ast_rewrite.xml"],
    ),
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_query(query):
    for node in nodes:
        node.query(query)


def test_backward_compatability(start_cluster):
    nodes[0].query("""
        CREATE TABLE default.table ON CLUSTER ast_rewrite
        (
            `reportTime` UInt32,
            `appId` UInt32,
            `platform` UInt16,
            `firstIFrameTs` UInt16,
            `day` Date DEFAULT toDate(rtime),
            `rtime` DateTime DEFAULT toDateTime(reportTime)
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test/table', '{replica}')
        PARTITION BY toYYYYMMDD(day)
        ORDER BY (rtime, reportTime, appId)
        TTL day + toIntervalMonth(3)
        SETTINGS index_granularity = 8192
    """)

    nodes[0].query("""
        CREATE TABLE IF NOT EXISTS default.table_all ON CLUSTER ast_rewrite AS default.table
        ENGINE = Distributed(ast_rewrite, default, table, rand())
    """)

    nodes[0].query("""
        INSERT INTO default.table (`reportTime`, `appId`, `platform`, `firstIFrameTs`) VALUES (1, 1, 1, 1);
    """)

    nodes[1].query("""
        INSERT INTO default.table (`reportTime`, `appId`, `platform`, `firstIFrameTs`) VALUES (2, 2, 2, 2);
    """)

    run_query("""
        SELECT
            toStartOfMinute(toDateTime(reportTime)) AS rtime,
            sumIf(multiIf((firstIFrameTs > 0) AND (firstIFrameTs <= 100), 1, 0), (appId = 60) AND (firstIFrameTs > 0)) AS _in1000ms,
            sumIf(1, (appId = 60) AND (firstIFrameTs > 0)) AS total_secondout
        FROM default.table_all
        GROUP BY rtime
    """)

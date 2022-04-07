import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="21.12.3.32",
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()

def test_checksum_mismatch_after_upgrade(start_cluster):
    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS mt")
        node.query(
            """CREATE TABLE mt (
                `RealDate` Date,
                `MaxRealTime` AggregateFunction(max, DateTime),
                `Rows` AggregateFunction(count),
                `MaxHitTime` AggregateFunction(max, DateTime)
            ) ENGINE=ReplicatedAggregatingMergeTree('/clickhouse/tables/mt', '{}', RealDate, tuple(RealDate), 8192)
            SETTINGS detach_not_byte_identical_parts=1
            """.format(
                node.name
            )
        )

    node1.query("""INSERT INTO mt SELECT
                        addMonths(today(), number) as RealDate,
                        maxState(toDateTime(RealDate)) as MaxRealTime,
                        countState() as Rows,
                        maxState(toDateTime(RealDate)) as MaxHitTime
                    FROM numbers(100) GROUP BY number""")

    node2.query("""INSERT INTO mt SELECT
                        subtractMonths(today(), number) as RealDate,
                        maxState(toDateTime(RealDate)) as MaxRealTime,
                        countState() as Rows,
                        maxState(toDateTime(RealDate)) as MaxHitTime
                    FROM numbers(100) GROUP BY number""")

    node1.query("SYSTEM SYNC REPLICA default.mt")

    node2.query("OPTIMIZE TABLE mt FINAL")

    assert not node2.contains_in_log("CHECKSUM_DOESNT_MATCH"), "Checksum doesn't match"

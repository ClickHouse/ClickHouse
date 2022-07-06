import pytest
import helpers.client
import helpers.cluster
from helpers.test_tools import assert_eq_with_retry


cluster = helpers.cluster.ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/cleanup_thread.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_empty_parts_alter_delete(started_cluster):
    node1.query(
        "CREATE TABLE empty_parts_delete (d Date, key UInt64, value String) \
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/empty_parts_delete', 'r1') PARTITION BY toYYYYMM(d) ORDER BY key"
    )

    node1.query("INSERT INTO empty_parts_delete VALUES (toDate('2020-10-10'), 1, 'a')")
    node1.query(
        "ALTER TABLE empty_parts_delete DELETE WHERE 1 SETTINGS mutations_sync = 2"
    )

    print(node1.query("SELECT count() FROM empty_parts_delete"))
    assert_eq_with_retry(
        node1,
        "SELECT count() FROM system.parts WHERE table = 'empty_parts_delete' AND active",
        "0",
    )


def test_empty_parts_summing(started_cluster):
    node1.query(
        "CREATE TABLE empty_parts_summing (d Date, key UInt64, value Int64) \
        ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/empty_parts_summing', 'r1') PARTITION BY toYYYYMM(d) ORDER BY key"
    )

    node1.query("INSERT INTO empty_parts_summing VALUES (toDate('2020-10-10'), 1, 1)")
    node1.query("INSERT INTO empty_parts_summing VALUES (toDate('2020-10-10'), 1, -1)")
    node1.query("OPTIMIZE TABLE empty_parts_summing FINAL")

    assert_eq_with_retry(
        node1,
        "SELECT count() FROM system.parts WHERE table = 'empty_parts_summing' AND active",
        "0",
    )

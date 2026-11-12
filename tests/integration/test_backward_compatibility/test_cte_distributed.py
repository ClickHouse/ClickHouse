import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=False)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_cte_distributed(start_cluster):
    node2.query(
        """
WITH
    quantile(0.05)(cnt) as p05,
    quantile(0.95)(cnt) as p95,
    p95 - p05 as inter_percentile_range
SELECT
    sum(cnt) as total_requests,
    count() as data_points,
    inter_percentile_range
FROM (
    SELECT
        count() as cnt
    FROM remote('node{1,2}', numbers(10))
    GROUP BY number
)"""
    )

    node1.query(
        """
WITH
    quantile(0.05)(cnt) as p05,
    quantile(0.95)(cnt) as p95,
    p95 - p05 as inter_percentile_range
SELECT
    sum(cnt) as total_requests,
    count() as data_points,
    inter_percentile_range
FROM (
    SELECT
        count() as cnt
    FROM remote('node{1,2}', numbers(10))
    GROUP BY number
)"""
    )


def test_merge_virtual_columns_over_old_shard(start_cluster):
    # The `_database`/`_table` of a `Merge` child that is a remote table are sent to the shard as a
    # constant: an older shard has to accept the expression the newer initiator writes for it.
    node2.query("DROP TABLE IF EXISTS t_merge_virtual_columns SYNC")
    node2.query(
        "CREATE TABLE t_merge_virtual_columns (x UInt32) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("INSERT INTO t_merge_virtual_columns VALUES (1), (2)")

    node1.query("DROP TABLE IF EXISTS m_merge_virtual_columns")
    node1.query("DROP TABLE IF EXISTS remote_merge_virtual_columns")
    node1.query(
        "CREATE TABLE remote_merge_virtual_columns AS remote('node2', default, t_merge_virtual_columns)"
    )
    node1.query(
        "CREATE TABLE m_merge_virtual_columns (x UInt32) ENGINE = Merge(default, '^remote_merge_virtual_columns$')"
    )

    row = "default\tremote_merge_virtual_columns"
    assert (
        node1.query("SELECT _database, _table, x FROM m_merge_virtual_columns ORDER BY x")
        == f"{row}\t1\n{row}\t2\n"
    )
    assert (
        node1.query(
            "SELECT count() FROM m_merge_virtual_columns WHERE _table = 'remote_merge_virtual_columns' AND _database = 'default'"
        )
        == "2\n"
    )
    assert (
        node1.query("SELECT _table, count() FROM m_merge_virtual_columns GROUP BY _table")
        == "remote_merge_virtual_columns\t2\n"
    )
    # `ARRAY JOIN` makes the `Merge` read its children at `FetchColumns`.
    assert (
        node1.query(
            "SELECT _table, x, one FROM m_merge_virtual_columns ARRAY JOIN [1] AS one WHERE _table = 'remote_merge_virtual_columns' ORDER BY x"
        )
        == "remote_merge_virtual_columns\t1\t1\nremote_merge_virtual_columns\t2\t1\n"
    )

    node1.query("DROP TABLE m_merge_virtual_columns")
    node1.query("DROP TABLE remote_merge_virtual_columns")
    node2.query("DROP TABLE t_merge_virtual_columns SYNC")

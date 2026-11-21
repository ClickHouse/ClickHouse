import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# 26.8 rebuilds a normal projection after a lightweight DELETE without dropping the deleted rows.
node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag="26.8.37.168",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def projection_rows_per_partition():
    return node.query(
        "SELECT partition_id, sum(rows) FROM system.projection_parts "
        "WHERE table = 't' AND active GROUP BY partition_id ORDER BY partition_id"
    )


def test_normal_projection_with_deleted_rows_after_upgrade(started_cluster):
    node.query(
        "CREATE TABLE t (k UInt64, g UInt8, v UInt64, PROJECTION p (SELECT * ORDER BY v)) "
        "ENGINE = MergeTree PARTITION BY intDiv(k, 10000) ORDER BY k "
        "SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 128"
    )
    node.query(
        "INSERT INTO t SELECT number, number % 4, number % 10000 FROM numbers(10000)"
    )
    node.query("DELETE FROM t WHERE g <= 1")
    assert projection_rows_per_partition() == "0\t10000\n"
    assert node.query("SELECT count() FROM t") == "5000\n"

    node.restart_with_latest_version()

    node.query(
        "INSERT INTO t SELECT number, number % 4, number % 10000 FROM numbers(10000, 10000)"
    )
    node.query("DELETE FROM t WHERE k >= 10000 AND g <= 1")
    assert projection_rows_per_partition() == "0\t10000\n1\t5000\n"
    assert (
        node.query(
            "SELECT partition_id, is_broken, exception_code, exception LIKE '%may contain lightweight-deleted rows%' "
            "FROM system.projection_parts WHERE table = 't' AND active ORDER BY partition_id"
        )
        == "0\t1\t713\t1\n1\t0\t0\t0\n"
    )

    query = "SELECT count() FROM t WHERE v < 1000"
    assert node.query(query, settings={"optimize_use_projections": 0}) == "1000\n"
    assert node.query(query, settings={"force_optimize_projection": 1}) == "1000\n"
    assert (
        node.query("CHECK TABLE t SETTINGS check_query_single_value_result = 1")
        == "0\n"
    )

    node.query("ALTER TABLE t MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2")
    assert projection_rows_per_partition() == "0\t5000\n1\t5000\n"
    assert (
        node.query(
            "SELECT count() FROM system.projection_parts WHERE table = 't' AND active AND is_broken"
        )
        == "0\n"
    )
    assert node.query(query, settings={"force_optimize_projection": 1}) == "1000\n"
    assert (
        node.query("CHECK TABLE t SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )

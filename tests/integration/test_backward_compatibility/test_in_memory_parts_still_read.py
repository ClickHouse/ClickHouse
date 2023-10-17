import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

# Version 23.4 is the latest version to support writing in-memory parts.
node = cluster.add_instance(
    "node_old",
    image="clickhouse/clickhouse-server",
    tag="23.4",
    stay_alive=True,
    with_installed_binary=True,
    allow_analyzer=False,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_in_memory_parts_still_read(start_cluster):
    node.query(
        "CREATE TABLE t (x UInt64, s String, a Array(Tuple(Map(String, LowCardinality(String)), Date32, DateTime64(3)))) ENGINE = MergeTree ORDER BY s SETTINGS min_rows_for_compact_part = 1000000, min_bytes_for_compact_part = '1G', in_memory_parts_enable_wal = 1"
    )
    node.query("INSERT INTO t SELECT * FROM generateRandom() LIMIT 100")

    assert node.query("SELECT count() FROM t WHERE NOT ignore(*)") == "100\n"

    node.restart_with_latest_version()
    assert node.query("SELECT count() FROM t WHERE NOT ignore(*)") == "100\n"

    node.query("INSERT INTO t SELECT * FROM generateRandom() LIMIT 100")

    assert node.query("SELECT count() FROM t WHERE NOT ignore(*)") == "200\n"

    node.restart_with_original_version()
    assert node.query("SELECT count() FROM t WHERE NOT ignore(*)") == "200\n"

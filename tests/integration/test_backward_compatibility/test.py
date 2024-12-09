import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/wide_parts_only.xml", "configs/no_compress_marks.xml"],
    with_zookeeper=True,
    use_old_analyzer=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        create_query = """CREATE TABLE t(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/t', '{}')
            PARTITION BY toYYYYMM(date)
            ORDER BY id"""
        node1.query(create_query.format(1))
        node1.query("DETACH TABLE t")  # stop being leader
        node2.query(create_query.format(2))
        node1.query("ATTACH TABLE t")
        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability1(start_cluster):
    node2.query("INSERT INTO t VALUES (today(), 1)")
    node1.query("SYSTEM SYNC REPLICA t", timeout=10)

    assert node1.query("SELECT count() FROM t") == "1\n"

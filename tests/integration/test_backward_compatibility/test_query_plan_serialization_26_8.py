import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# 26.8.16 is the first 26.8 release that writes the plan-level execution limits at version 10.
old = cluster.add_instance(
    "old",
    image="clickhouse/clickhouse-server",
    tag="26.8.16.41",
    with_installed_binary=True,
)
new = cluster.add_instance("new")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_serialized_plan_between_26_8_and_current(start_cluster):
    for node in (old, new):
        node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")
        node.query("INSERT INTO t SELECT number FROM numbers(1000)")

    query = "SELECT sum(x), count() FROM remote('{}', default.t) WHERE x % 2 = 0"
    settings = {"serialize_query_plan": 1, "receive_timeout": 30}
    assert old.query(query.format("new"), settings=settings) == "249500\t500\n"
    assert new.query(query.format("old"), settings=settings) == "249500\t500\n"

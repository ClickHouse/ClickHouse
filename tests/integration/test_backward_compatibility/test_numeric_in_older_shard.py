import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster


cluster = ClickHouseCluster(__file__)
new_node = cluster.add_instance("new_node")
old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_numeric_scalar_membership_on_older_shard(start_cluster):
    old_node.query("CREATE TABLE numeric_in_old_shard (x UInt8) ENGINE = Memory")
    old_node.query("INSERT INTO numeric_in_old_shard VALUES (1)")

    assert (
        new_node.query(
            "SELECT count() FROM remote('old_node', default, numeric_in_old_shard) "
            "WHERE x IN (materialize(x)) SETTINGS prefer_localhost_replica = 0"
        )
        == "1\n"
    )
    assert (
        new_node.query(
            "SELECT count() FROM remote('old_node', default, numeric_in_old_shard) "
            "WHERE x IN (materialize(x)) SETTINGS prefer_localhost_replica = 0",
            settings={"transform_null_in": 1},
        )
        == "1\n"
    )

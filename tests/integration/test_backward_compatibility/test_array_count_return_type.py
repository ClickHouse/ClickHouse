import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
new_node = cluster.add_instance("new_node", with_zookeeper=False)
old_node = cluster.add_instance(
    "old_node",
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


# Since 26.8 `arrayCount` returns `UInt64` (`UInt32` before). A new coordinator
# with `array_count_legacy_uint32_result = 1` (or `compatibility` <= '26.7')
# must be able to query a mix of old and new servers without a type mismatch:
# the coordinator builds a `UInt32` header, new servers compute `UInt32`
# because the setting is forwarded to them, and old servers compute `UInt32`
# natively while ignoring the unknown setting.
def test_array_count_mixed_version_remote(start_cluster):
    for node in (new_node, old_node):
        node.query("CREATE TABLE tab (arr Array(UInt64)) ENGINE = Memory")
    new_node.query("INSERT INTO tab VALUES ([1, 2, 3])")
    old_node.query("INSERT INTO tab VALUES ([2, 3, 4, 5])")

    query = (
        "SELECT sum(c), any(toTypeName(c)) FROM "
        "(SELECT arrayCount(x -> x >= 2, arr) AS c "
        "FROM remote('old_node,new_node', default, tab))"
    )

    assert (
        new_node.query(query, settings={"array_count_legacy_uint32_result": 1})
        == "6\tUInt32\n"
    )
    assert (
        new_node.query(query, settings={"compatibility": CLICKHOUSE_CI_MIN_TESTED_VERSION})
        == "6\tUInt32\n"
    )

    for node in (new_node, old_node):
        node.query("DROP TABLE tab")

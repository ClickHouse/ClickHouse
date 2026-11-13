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

    for transform_null_in in (0, 1):
        assert (
            new_node.query(
                "SELECT count() FROM remote('old_node', default, numeric_in_old_shard) "
                "WHERE x IN (materialize(x)) SETTINGS prefer_localhost_replica = 0",
                settings={"allow_lossy_numeric_supertype": 1, "transform_null_in": transform_null_in},
            )
            == "1\n"
        )
        assert (
            new_node.query(
                "SELECT count() FROM remote('old_node', default, numeric_in_old_shard) "
                "WHERE x NOT IN (materialize(x)) SETTINGS prefer_localhost_replica = 0",
                settings={"allow_lossy_numeric_supertype": 1, "transform_null_in": transform_null_in},
            )
            == "0\n"
        )

    old_node.query("CREATE TABLE numeric_decimal_old_shard (d Decimal(18, 0), x Float64) ENGINE = Memory")
    old_node.query("INSERT INTO numeric_decimal_old_shard VALUES ('9007199254740993', 9007199254740992.)")
    for transform_null_in in (0, 1):
        assert (
            new_node.query(
                "SELECT count() FROM remote('old_node', default, numeric_decimal_old_shard) "
                "WHERE d IN (materialize(x)) SETTINGS prefer_localhost_replica = 0",
                settings={"allow_lossy_numeric_supertype": 1, "transform_null_in": transform_null_in},
            )
            == "0\n"
        )

    old_node.query("INSERT INTO numeric_decimal_old_shard VALUES (1, 1)")
    for predicate in (
        "toInt64(d) NOT IN (x, 0.)",
        "d NOT IN (x, toDecimal64(0, 0))",
    ):
        for transform_null_in in (0, 1):
            assert (
                new_node.query(
                    "SELECT count() FROM remote('old_node', default, numeric_decimal_old_shard) "
                    f"WHERE {predicate} SETTINGS prefer_localhost_replica = 0",
                    settings={"allow_lossy_numeric_supertype": 1, "transform_null_in": transform_null_in},
                )
                == "1\n"
            )

import pytest

from helpers.cluster import ClickHouseCluster

# With `distributed_ddl_use_initial_user_and_roles` the hosts run `OPTIMIZE ... ON CLUSTER` as the user, so they check
# `SELECT` on the `DEDUPLICATE BY` columns themselves. A stateless test cannot enable a server setting, hence this test.
# The initiator's user is carried in the DDL entry only from entry format version 8.
ENTRY_VERSION = {"distributed_ddl_entry_format_version": 8}
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def rows():
    return node.query("SELECT count() FROM t").strip()


def test_hosts_check_deduplicate_by_columns_as_the_user():
    node.query(
        "CREATE TABLE t (sort_key UInt64, id UInt64, secret String) ENGINE = MergeTree ORDER BY sort_key"
    )
    node.query(
        "INSERT INTO t VALUES (1, 1, 'A'), (1, 1, 'B'), (1, 2, 'A'), (2, 3, 'A'), (2, 3, 'C')"
    )
    node.query("CREATE USER u")
    node.query("GRANT CLUSTER ON *.* TO u")
    node.query("GRANT SELECT(sort_key, id), OPTIMIZE ON default.t TO u")

    # The host refuses a column the user cannot read, and no row is removed.
    error = node.query_and_get_error(
        "OPTIMIZE TABLE t ON CLUSTER cluster FINAL DEDUPLICATE BY sort_key, secret",
        user="u",
        settings=ENTRY_VERSION,
    )
    assert "Not enough privileges" in error
    assert rows() == "5"

    # The listed columns are enough: the whole-table `SELECT` required when the hosts do not keep the user is not.
    node.query(
        "OPTIMIZE TABLE t ON CLUSTER cluster FINAL DEDUPLICATE BY sort_key, id",
        user="u",
        settings=ENTRY_VERSION,
    )
    assert rows() == "3"

    node.query("DROP TABLE t SYNC")
    node.query("DROP USER u")

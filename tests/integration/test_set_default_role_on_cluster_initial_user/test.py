import pytest

from helpers.cluster import ClickHouseCluster

# With `distributed_ddl_use_initial_user_and_roles` the DDL worker executes a distributed query as the user
# who issued it. `SET DEFAULT ROLE ... TO CURRENT_USER ON CLUSTER` reaches the worker with the tag replaced by
# that user's name, and must not demand `ALTER USER` there: setting one's own default roles needs no grant.
# A stateless test cannot enable a server setting, hence this test. The initiator's user is carried in the
# DDL entry only from entry format version 8.
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


@pytest.fixture(autouse=True)
def cleanup():
    yield
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query("DROP USER IF EXISTS u, other ON CLUSTER cluster")
    node.query("DROP ROLE IF EXISTS r ON CLUSTER cluster")


def default_roles(user):
    return node.query(
        f"SELECT default_roles_list FROM system.users WHERE name = '{user}'"
    ).strip()


def test_current_user_needs_no_alter_user():
    node.query("CREATE ROLE r ON CLUSTER cluster")
    node.query("CREATE USER u, other ON CLUSTER cluster")
    node.query("GRANT ON CLUSTER cluster r TO u, other")
    # Enough to issue an `ON CLUSTER` query; notably not `ALTER USER`.
    node.query("GRANT ON CLUSTER cluster CLUSTER ON *.* TO u")

    node.query(
        "SET DEFAULT ROLE r TO CURRENT_USER ON CLUSTER cluster",
        user="u",
        settings=ENTRY_VERSION,
    )
    assert default_roles("u") == "['r']"

    # The relaxation is for the user's own name only: another user still needs `ALTER USER`.
    error = node.query_and_get_error(
        "SET DEFAULT ROLE r TO other ON CLUSTER cluster",
        user="u",
        settings=ENTRY_VERSION,
    )
    assert "ACCESS_DENIED" in error
    assert default_roles("other") == "[]"


def rows():
    return node.query("SELECT count() FROM t").strip()


# The hosts run `OPTIMIZE ... ON CLUSTER` as the user, so they check `SELECT` on the compared columns themselves.
def test_hosts_check_deduplicate_columns_as_the_user():
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
    for query in [
        "OPTIMIZE TABLE t ON CLUSTER cluster FINAL DEDUPLICATE BY sort_key, secret",
        "OPTIMIZE TABLE t ON CLUSTER cluster FINAL DEDUPLICATE",
    ]:
        error = node.query_and_get_error(query, user="u", settings=ENTRY_VERSION)
        assert "Not enough privileges" in error
    assert rows() == "5"

    # The listed columns are enough: the whole-table `SELECT` required when the hosts do not keep the user is not.
    node.query(
        "OPTIMIZE TABLE t ON CLUSTER cluster FINAL DEDUPLICATE BY sort_key, id",
        user="u",
        settings=ENTRY_VERSION,
    )
    assert rows() == "3"

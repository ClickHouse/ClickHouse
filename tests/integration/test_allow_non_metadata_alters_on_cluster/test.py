import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# The profile pins allow_non_metadata_alters to 0 with a <const/> constraint on every node. That
# is the configuration an operator uses to enforce the restriction rather than merely default it,
# and it is what a DDL worker applies to a queued entry: the worker runs as the default user
# (distributed_ddl_use_initial_user_and_roles is off by default), so a constraint attached to the
# issuing user alone would never reach it. A stateless test cannot set a profile, hence this test.
cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/users.xml"],
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
def table():
    ch1.query(
        "CREATE TABLE t ON CLUSTER 'cluster' (key UInt64, value UInt64) "
        "ENGINE = MergeTree ORDER BY tuple()"
    )
    ch1.query("INSERT INTO t SELECT number, number FROM numbers(10)")
    ch2.query("INSERT INTO t SELECT number, number FROM numbers(10)")
    yield
    ch1.query("DROP TABLE IF EXISTS t ON CLUSTER 'cluster' SYNC")


def test_setting_is_constrained():
    for node in (ch1, ch2):
        assert node.query("SELECT getSetting('allow_non_metadata_alters')") == "false\n"
        with pytest.raises(QueryRuntimeException, match="SETTING_CONSTRAINT_VIOLATION"):
            node.query("SELECT 1 SETTINGS allow_non_metadata_alters = 1")


def test_delete_from_on_cluster_is_allowed():
    ch1.query("DELETE FROM t ON CLUSTER 'cluster' WHERE key = 1")
    for node in (ch1, ch2):
        assert node.query("SELECT count() FROM t") == "9\n"


def test_delete_from_on_cluster_without_settings_in_entry():
    # distributed_ddl_entry_format_version = 1 ships no settings packet at all, so a worker can
    # only use its own profile value.
    ch1.query(
        "DELETE FROM t ON CLUSTER 'cluster' WHERE key = 2",
        settings={"distributed_ddl_entry_format_version": 1},
    )
    for node in (ch1, ch2):
        assert node.query("SELECT count() FROM t") == "9\n"


@pytest.mark.parametrize(
    "query",
    [
        "ALTER TABLE t ON CLUSTER 'cluster' UPDATE `_row_exists` = 0 WHERE key = 3",
        "ALTER TABLE t ON CLUSTER 'cluster' DELETE WHERE key = 3",
        "ALTER TABLE t ON CLUSTER 'cluster' UPDATE value = 5 WHERE key = 3",
        "ALTER TABLE t ON CLUSTER 'cluster' MODIFY COLUMN value String",
    ],
)
def test_alter_on_cluster_is_refused(query):
    # A user-written assignment must not borrow the exemption that DELETE FROM gets, including the
    # one that writes the same deletion mask the rewrite would.
    with pytest.raises(QueryRuntimeException, match="ALTER_OF_COLUMN_IS_FORBIDDEN"):
        ch1.query(query)
    for node in (ch1, ch2):
        assert node.query("SELECT count() FROM t") == "10\n"

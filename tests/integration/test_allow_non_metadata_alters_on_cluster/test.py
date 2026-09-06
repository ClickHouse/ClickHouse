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
    macros={"shard": "1", "replica": "ch1"},
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/users.xml"],
    macros={"shard": "1", "replica": "ch2"},
    with_zookeeper=True,
)
# A `Replicated` database routes a DELETE through the database log only when it has more than one
# shard, and the routing is observable only with more than one replica per shard, so the arm below
# needs four hosts. They take no cluster definition: a `Replicated` database derives its topology
# from its own Keeper registration rather than from `remote_servers`.
ch3 = cluster.add_instance(
    "ch3",
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
)
ch4 = cluster.add_instance(
    "ch4",
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


@pytest.fixture
def udf():
    # The default user-defined-function store is not replicated (IUserDefinedSQLObjectsStorage::
    # isReplicated() is false), and CREATE FUNCTION is rejected with ON CLUSTER only when it is
    # replicated, so a function legitimately exists on one host alone. Drop before and after, so
    # an interrupted run cannot poison the next one.
    for node in (ch1, ch2):
        node.query("DROP FUNCTION IF EXISTS udf_key")
    yield
    for node in (ch1, ch2):
        node.query("DROP FUNCTION IF EXISTS udf_key")


def test_delete_from_on_cluster_expands_udf_absent_on_other_host(udf):
    # The initiator's body decides which rows go on every host, so assert the surviving keys
    # rather than their count: a count also passes when the wrong row was deleted.
    ch1.query("CREATE FUNCTION udf_key AS (x) -> x = 1")
    ch1.query("DELETE FROM t ON CLUSTER 'cluster' WHERE udf_key(key)")
    for node in (ch1, ch2):
        assert (
            node.query("SELECT arraySort(groupArray(key)) FROM t")
            == "[0,2,3,4,5,6,7,8,9]\n"
        )


def test_delete_from_on_cluster_expands_udf_defined_differently(udf):
    # Divergent bodies delete different rows on each host without raising anywhere, so the row
    # counts still match. Asserting the initiator's key set on both hosts catches that divergence
    # and a uniformly wrong answer, which comparing the two hosts to each other would not.
    ch1.query("CREATE FUNCTION udf_key AS (x) -> x = 1")
    ch2.query("CREATE FUNCTION udf_key AS (x) -> x = 2")
    ch1.query("DELETE FROM t ON CLUSTER 'cluster' WHERE udf_key(key)")
    for node in (ch1, ch2):
        assert (
            node.query("SELECT arraySort(groupArray(key)) FROM t")
            == "[0,2,3,4,5,6,7,8,9]\n"
        )


@pytest.fixture
def replicated_table():
    # ch1 and ch2 are two replicas of one shard, so the two hosts share a single dataset and a
    # single Keeper mutation log. Insert once and sync, so a doubled mutation is observable
    # instead of being absorbed by each host owning private rows.
    ch1.query(
        "CREATE TABLE tr ON CLUSTER 'cluster' (key UInt64, value UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/tr', '{replica}') "
        "ORDER BY tuple()"
    )
    ch1.query("INSERT INTO tr SELECT number, number FROM numbers(10)")
    ch2.query("SYSTEM SYNC REPLICA tr")
    yield
    ch1.query("DROP TABLE IF EXISTS tr ON CLUSTER 'cluster' SYNC")


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


def test_delete_from_on_cluster_replicated_runs_once_per_shard(replicated_table):
    ch1.query("DELETE FROM tr ON CLUSTER 'cluster' WHERE key = 1")
    for node in (ch1, ch2):
        assert (
            node.query(
                "SELECT count() FROM system.mutations "
                "WHERE database = currentDatabase() AND table = 'tr'"
            )
            == "1\n"
        )
        assert node.query("SELECT count() FROM tr") == "9\n"


@pytest.fixture
def replicated_database():
    hosts = (ch1, ch2, ch3, ch4)
    for node in hosts:
        node.query("DROP DATABASE IF EXISTS rdb SYNC")
    for node, shard, replica in zip(hosts, ("s1", "s1", "s2", "s2"), ("r1", "r2", "r1", "r2")):
        node.query(
            f"CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/databases/rdb', '{shard}', '{replica}')"
        )
    ch1.query(
        "CREATE TABLE rdb.tr (key UInt64, value UInt64) "
        "ENGINE = ReplicatedMergeTree ORDER BY tuple()"
    )
    for node in hosts:
        node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    # Give each shard its own rows, so a delete that reaches only one shard is visible as rows that
    # outlive it rather than being masked by both shards holding the same data.
    ch1.query("INSERT INTO rdb.tr SELECT number, number FROM numbers(10)")
    ch3.query("INSERT INTO rdb.tr SELECT number + 100, number FROM numbers(10)")
    for node in hosts:
        node.query("SYSTEM SYNC REPLICA rdb.tr")
    yield
    for node in hosts:
        node.query("DROP DATABASE IF EXISTS rdb SYNC")


def test_delete_on_replicated_database_runs_once_per_shard(replicated_database):
    # A multi-shard `Replicated` database replicates the DELETE at database level, so both replicas
    # of a shard receive it and each would mutate the shared table without leader routing.
    ch1.query("DELETE FROM rdb.tr WHERE key = 1")
    for node in (ch1, ch2, ch3, ch4):
        node.query("SYSTEM SYNC DATABASE REPLICA rdb")
        node.query("SYSTEM SYNC REPLICA rdb.tr")
        assert (
            node.query(
                "SELECT count() FROM system.mutations WHERE database = 'rdb' AND table = 'tr'"
            )
            == "1\n"
        )
    for node in (ch1, ch2):
        assert node.query("SELECT count() FROM rdb.tr") == "9\n"
    for node in (ch3, ch4):
        assert node.query("SELECT count() FROM rdb.tr") == "10\n"


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

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2")
node3 = cluster.add_instance("node3")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_replica_fallback(started_cluster):
    # The database exists only on node2. On node1 the replica that points to node1 itself is a local
    # shard, so the metadata lookup prefers the local catalog; when the local replica does not have
    # the database, the lookup must fall back to the remote replica, like the read path of the
    # `Distributed` storage does, instead of hiding the tables of the remote replica.
    node2.query("CREATE DATABASE fallback_src")
    node2.query(
        "CREATE TABLE fallback_src.t (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("INSERT INTO fallback_src.t VALUES (1), (2), (3)")

    node1.query(
        "CREATE DATABASE fallback_proxy ENGINE = Remote('node1|node2', 'fallback_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM fallback_proxy") == "t\n"
    assert node1.query("EXISTS TABLE fallback_proxy.t") == "1\n"
    description = node1.query("DESCRIBE TABLE fallback_proxy.t").split("\t")
    assert description[0] == "x"
    assert description[1].strip() == "UInt64"
    assert node1.query("SELECT count(), sum(x) FROM fallback_proxy.t") == "3\t6\n"

    node1.query("DROP DATABASE fallback_proxy")
    node2.query("DROP DATABASE fallback_src")


def test_local_only_shard_is_not_dropped_by_fallback(started_cluster):
    # `Remote('node1,node2', db)` on node1 describes two shards, and the first one consists of the
    # local replica only. When the database exists only on node2, there is no same-shard replica to
    # fall back to for the first shard; substituting a cluster without it would silently read and
    # write only node2, i.e. a subset of the configured shards. The table must be reported as
    # missing instead.
    node2.query("CREATE DATABASE mixed_src")
    node2.query("CREATE TABLE mixed_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO mixed_src.t VALUES (1)")

    node1.query(
        "CREATE DATABASE mixed_proxy ENGINE = Remote('node1,node2', 'mixed_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM mixed_proxy") == ""
    assert "UNKNOWN_TABLE" in node1.query_and_get_error("SELECT * FROM mixed_proxy.t")

    node1.query("DROP DATABASE mixed_proxy")
    node2.query("DROP DATABASE mixed_src")


def test_replica_fallback_needs_no_local_grants(started_cluster):
    # On the remote-replica fallback no local object is touched, so a user with rights on the proxy
    # database only (and none on the missing local counterpart of the remote database) must be able
    # to resolve and read the table.
    node2.query("CREATE DATABASE grants_src")
    node2.query("CREATE TABLE grants_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO grants_src.t VALUES (7)")

    node1.query(
        "CREATE DATABASE grants_proxy ENGINE = Remote('node1|node2', 'grants_src', 'default', '')"
    )
    node1.query("CREATE USER restricted_user IDENTIFIED WITH no_password")
    node1.query("GRANT SHOW, SELECT ON grants_proxy.* TO restricted_user")

    assert (
        node1.query("SELECT x FROM grants_proxy.t", user="restricted_user") == "7\n"
    )

    node1.query("DROP USER restricted_user")
    node1.query("DROP DATABASE grants_proxy")
    node2.query("DROP DATABASE grants_src")


def test_all_remote_shards_must_have_the_table(started_cluster):
    # `Remote('node1,node2', db)` on node3 describes two remote shards, and the proxy table queries
    # both of them, so a table is exposed only when every shard has it. Here `only_on_node2` exists
    # on one shard only: exposing it would advertise a proxy whose `SELECT` then fails on node1, so
    # it must be reported as missing instead. A table present on both shards is served, aggregating
    # over both.
    node1.query("CREATE DATABASE sharded_src")
    node1.query("CREATE TABLE sharded_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO sharded_src.both VALUES (1)")
    node2.query("CREATE DATABASE sharded_src")
    node2.query("CREATE TABLE sharded_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO sharded_src.both VALUES (2)")
    node2.query(
        "CREATE TABLE sharded_src.only_on_node2 (x UInt64) ENGINE = MergeTree ORDER BY x"
    )

    node3.query(
        "CREATE DATABASE sharded_proxy ENGINE = Remote('node1,node2', 'sharded_src', 'default', '')"
    )

    assert node3.query("SHOW TABLES FROM sharded_proxy") == "both\n"
    assert node3.query("EXISTS TABLE sharded_proxy.both") == "1\n"
    assert node3.query("EXISTS TABLE sharded_proxy.only_on_node2") == "0\n"
    assert node3.query("SELECT count(), sum(x) FROM sharded_proxy.both") == "2\t3\n"
    assert "UNKNOWN_TABLE" in node3.query_and_get_error(
        "SELECT * FROM sharded_proxy.only_on_node2"
    )

    node3.query("DROP DATABASE sharded_proxy")
    node1.query("DROP DATABASE sharded_src")
    node2.query("DROP DATABASE sharded_src")


def test_local_shard_does_not_hide_a_missing_remote_shard(started_cluster):
    # The symmetric case: `Remote('node1,node2', db)` on node1 resolves the first shard through the
    # local catalog, but the second shard must still be consulted. A table that only the local shard
    # has must be reported as missing rather than exposed as a proxy that fails on node2.
    node1.query("CREATE DATABASE partial_src")
    node1.query(
        "CREATE TABLE partial_src.only_local (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    node1.query("CREATE TABLE partial_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO partial_src.both VALUES (10)")
    node2.query("CREATE DATABASE partial_src")
    node2.query("CREATE TABLE partial_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO partial_src.both VALUES (20)")

    node1.query(
        "CREATE DATABASE partial_proxy ENGINE = Remote('node1,node2', 'partial_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM partial_proxy") == "both\n"
    assert node1.query("EXISTS TABLE partial_proxy.only_local") == "0\n"
    assert "UNKNOWN_TABLE" in node1.query_and_get_error(
        "SELECT * FROM partial_proxy.only_local"
    )
    assert node1.query("SELECT count(), sum(x) FROM partial_proxy.both") == "2\t30\n"

    node1.query("DROP DATABASE partial_proxy")
    node1.query("DROP DATABASE partial_src")
    node2.query("DROP DATABASE partial_src")


def test_local_replica_preferred(started_cluster):
    # When the local replica does have the database, the metadata comes from the local catalog
    # without a self-connection, and queries read the local replica.
    node1.query("CREATE DATABASE local_src")
    node1.query("CREATE TABLE local_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO local_src.t VALUES (1), (2)")

    node1.query(
        "CREATE DATABASE local_proxy ENGINE = Remote('node1|node2', 'local_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM local_proxy") == "t\n"
    assert node1.query("SELECT count(), sum(x) FROM local_proxy.t") == "2\t3\n"

    node1.query("DROP DATABASE local_proxy")
    node1.query("DROP DATABASE local_src")

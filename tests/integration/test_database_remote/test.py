import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2")


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

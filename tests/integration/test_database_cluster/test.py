import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/clusters.xml", "configs/reloadable_cluster.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance("node2", main_configs=["configs/clusters.xml"])

RELOADABLE_CLUSTER_CONFIG_PATH = (
    "/etc/clickhouse-server/config.d/reloadable_cluster.xml"
)

RELOADABLE_ONE_SHARD = """<clickhouse>
    <remote_servers>
        <reloadable>
            <shard>
                <replica>
                    <host>node1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </reloadable>
    </remote_servers>
</clickhouse>
"""

RELOADABLE_TWO_SHARDS = """<clickhouse>
    <remote_servers>
        <reloadable>
            <shard>
                <replica>
                    <host>node1</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>node2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </reloadable>
    </remote_servers>
</clickhouse>
"""

RELOADABLE_ONE_SHARD_TWO_REPLICAS = """<clickhouse>
    <remote_servers>
        <reloadable>
            <shard>
                <replica>
                    <host>node1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>node2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </reloadable>
    </remote_servers>
</clickhouse>
"""

RELOADABLE_NODE2_ONLY = """<clickhouse>
    <remote_servers>
        <reloadable>
            <shard>
                <replica>
                    <host>node2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </reloadable>
    </remote_servers>
</clickhouse>
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cluster_database(started_cluster):
    # The database exists on both shards of the named cluster; the proxy reads from all of them and
    # forwards an INSERT to some shard.
    for node, value in ((node1, 1), (node2, 2)):
        node.query("CREATE DATABASE src")
        node.query("CREATE TABLE src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
        node.query(f"INSERT INTO src.t VALUES ({value})")

    node1.query("CREATE DATABASE proxy ENGINE = Cluster('two_shards', 'src')")

    assert node1.query("SHOW TABLES FROM proxy") == "t\n"
    assert node1.query("EXISTS TABLE proxy.t") == "1\n"
    assert node1.query("SELECT count(), sum(x) FROM proxy.t") == "2\t3\n"

    # The implicit `rand()` key is insert-only in a database proxy, but it would be a read
    # sharding key in a standalone `Distributed` table, so no equivalent CREATE query exists.
    assert "THERE_IS_NO_QUERY" in node1.query_and_get_error("SHOW CREATE TABLE proxy.t")

    node1.query("INSERT INTO proxy.t VALUES (10)")
    assert node1.query("SELECT sum(x) FROM proxy.t") == "13\n"

    # DDL against the database is rejected.
    assert "NOT_IMPLEMENTED" in node1.query_and_get_error("DROP TABLE proxy.t")

    node1.query("DROP DATABASE proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE src")


def test_clickhouse_local_uses_configured_loopback_cluster(started_cluster):
    node1.query("CREATE DATABASE local_src")
    node1.query("CREATE TABLE local_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO local_src.t VALUES (1), (2), (3)")

    # The local tool does not listen on TCP port 9000. A configured loopback address must therefore
    # be a remote replica, rather than resolving its database in the local tool. This holds both for
    # a replica that spells out `<port>` and for one that inherits it from the top-level `tcp_port`
    # (the shape of the built-in `default` cluster): the tool fills `tcp_port` in with the default
    # value even though it listens on no port, so the inherited value says nothing about it.
    # The configuration of the server is used, because it is the one that declares both the
    # cluster and `tcp_port`; a separate working directory keeps the tool away from the data
    # directory of the running server.
    for cluster_name in ("loopback", "loopback_default_port"):
        assert (
            node1.exec_in_container(
                [
                    "clickhouse",
                    "local",
                    "--config-file=/etc/clickhouse-server/config.xml",
                    "--path=/var/lib/clickhouse-local-cluster-test",
                    "--multiquery",
                    f"--query=CREATE DATABASE proxy ENGINE = Cluster('{cluster_name}', 'local_src'); SELECT count() FROM proxy.t",
                ]
            )
            == "3\n"
        )

    node1.query("DROP DATABASE local_src")


def test_replica_fallback(started_cluster):
    # The database exists only on node2. On node1 the replica of the shard that points to node1
    # itself is a local one, so the metadata lookup prefers the local catalog; when the local
    # replica does not have the database, the lookup must fall back to the remote replica of the
    # shard, like the read path of the `Distributed` storage does.
    node2.query("CREATE DATABASE fb_src")
    node2.query("CREATE TABLE fb_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO fb_src.t VALUES (1), (2), (3)")

    node1.query(
        "CREATE DATABASE fb_proxy ENGINE = Cluster('one_shard_two_replicas', 'fb_src')"
    )

    assert node1.query("SHOW TABLES FROM fb_proxy") == "t\n"
    assert node1.query("EXISTS TABLE fb_proxy.t") == "1\n"
    assert node1.query("SELECT count(), sum(x) FROM fb_proxy.t") == "3\t6\n"

    # In this state the live proxy is bound to the remote-only fallback cluster, and a
    # `Distributed` table over the whole named cluster would recreate a different object (it
    # performs no such fallback on the metadata lookup), so there is no equivalent re-executable
    # definition, and SHOW CREATE TABLE reports that instead of emitting a misleading one.
    assert "THERE_IS_NO_QUERY" in node1.query_and_get_error(
        "SHOW CREATE TABLE fb_proxy.t"
    )
    # The best-effort paths (e.g. the `create_table_query` column of `system.tables`) must not
    # fail because of it.
    assert (
        node1.query(
            "SELECT create_table_query FROM system.tables WHERE database = 'fb_proxy' AND name = 't'"
        )
        == "\n"
    )

    node1.query("DROP DATABASE fb_proxy")
    node2.query("DROP DATABASE fb_src")


def test_nested_cluster_missing_from_config_uses_remote_replica(started_cluster):
    # The outer proxy first resolves `chain_inner` locally. Once that intermediate `Cluster`
    # database loses its named cluster, it must treat the local answer as unavailable and use node2,
    # the other replica of its shard, rather than leaking `CLUSTER_DOESNT_EXIST` from node1.
    for node, value in ((node1, 1), (node2, 2)):
        node.query("CREATE DATABASE chain_src")
        node.query("CREATE TABLE chain_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
        node.query(f"INSERT INTO chain_src.t VALUES ({value})")

    node1.query(
        "CREATE DATABASE chain_inner ENGINE = Cluster('reloadable', 'chain_src')"
    )
    node2.query(
        "CREATE DATABASE chain_inner ENGINE = Cluster('one_shard_two_replicas', 'chain_src')"
    )
    node1.query(
        "CREATE DATABASE chain_outer ENGINE = Cluster('one_shard_two_replicas', 'chain_inner')"
    )

    try:
        node1.replace_config(
            RELOADABLE_CLUSTER_CONFIG_PATH, "<clickhouse></clickhouse>\n"
        )
        node1.query("SYSTEM RELOAD CONFIG")

        assert node1.query("SHOW TABLES FROM chain_outer") == "t\n"
        assert node1.query("EXISTS TABLE chain_outer.t") == "1\n"
        # The fallback has only node2. `chain_inner` is a one-shard cluster, so it selects the
        # local replica there rather than reading both independently populated replicas.
        assert node1.query("SELECT sum(x) FROM chain_outer.t") == "2\n"
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")
        node1.query("DROP DATABASE IF EXISTS chain_outer")
        node1.query("DROP DATABASE IF EXISTS chain_inner")
        node2.query("DROP DATABASE IF EXISTS chain_inner")
        for node in (node1, node2):
            node.query("DROP DATABASE IF EXISTS chain_src")


def test_nested_local_cycle_uses_remote_replica(started_cluster):
    # `cycle_outer` reaches a live local cycle through `cycle_inner`. The cycle is not a property
    # of `cycle_outer`, so its local replica cannot answer but node2, the other replica of the
    # outer shard, can. In contrast, querying a database of the cycle itself still reports the
    # configuration error.
    node2.query("CREATE DATABASE cycle_src")
    node2.query("CREATE TABLE cycle_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO cycle_src.t VALUES (42)")

    node1.query("CREATE DATABASE cycle_a ENGINE = Cluster('reloadable', 'cycle_b')")
    node1.query(
        "CREATE DATABASE cycle_inner ENGINE = Cluster('one_shard_two_replicas', 'cycle_a')"
    )
    node2.query(
        "CREATE DATABASE cycle_inner ENGINE = Cluster('one_shard_two_replicas', 'cycle_src')"
    )
    node1.query(
        "CREATE DATABASE cycle_outer ENGINE = Cluster('one_shard_two_replicas', 'cycle_inner')"
    )

    try:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_NODE2_ONLY)
        node1.query("SYSTEM RELOAD CONFIG")
        node1.query("CREATE DATABASE cycle_b ENGINE = Cluster('reloadable', 'cycle_a')")
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")

        assert "INFINITE_LOOP" in node1.query_and_get_error("SELECT * FROM cycle_a.t")
        assert node1.query("SHOW TABLES FROM cycle_outer") == "t\n"
        assert node1.query("EXISTS TABLE cycle_outer.t") == "1\n"
        assert node1.query("SELECT x FROM cycle_outer.t") == "42\n"
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")
        node1.query("DROP DATABASE IF EXISTS cycle_outer")
        node1.query("DROP DATABASE IF EXISTS cycle_inner")
        node2.query("DROP DATABASE IF EXISTS cycle_inner")
        node1.query("DROP DATABASE IF EXISTS cycle_b")
        node1.query("DROP DATABASE IF EXISTS cycle_a")
        node2.query("DROP DATABASE IF EXISTS cycle_src")


def test_interserver_secret_cluster(started_cluster):
    # A cluster with an inter-server secret: the connections authenticate with the secret and run
    # under the initial user. The database exists only on node2, so this also checks that the
    # remote-only fallback cluster derived for the metadata lookup preserves the secret.
    node2.query("CREATE DATABASE sec_src")
    node2.query("CREATE TABLE sec_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO sec_src.t VALUES (5)")

    node1.query(
        "CREATE DATABASE sec_proxy ENGINE = Cluster('secret_two_replicas', 'sec_src')"
    )

    assert node1.query("SHOW TABLES FROM sec_proxy") == "t\n"
    assert node1.query("SELECT x FROM sec_proxy.t") == "5\n"

    node1.query("DROP DATABASE sec_proxy")
    node2.query("DROP DATABASE sec_src")


def test_follows_config_reload(started_cluster):
    # The cluster is resolved from the configuration on every access, so a configuration reload that
    # changes the cluster is picked up by an existing database without a restart.
    for node, value in ((node1, 1), (node2, 2)):
        node.query("CREATE DATABASE rel_src")
        node.query("CREATE TABLE rel_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
        node.query(f"INSERT INTO rel_src.t VALUES ({value})")

    node1.query("CREATE DATABASE rel_proxy ENGINE = Cluster('reloadable', 'rel_src')")
    assert node1.query("SELECT sum(x) FROM rel_proxy.t") == "1\n"
    # A definition created while this cluster has one shard would stop accepting INSERT after a
    # reload adds a shard, whereas the live proxy gains an implicit insert-only `rand()` key.
    assert "THERE_IS_NO_QUERY" in node1.query_and_get_error("SHOW CREATE TABLE rel_proxy.t")

    try:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_TWO_SHARDS)
        node1.query("SYSTEM RELOAD CONFIG")

        assert node1.query("SELECT sum(x) FROM rel_proxy.t") == "3\n"
        assert "THERE_IS_NO_QUERY" in node1.query_and_get_error("SHOW CREATE TABLE rel_proxy.t")
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")

    assert node1.query("SELECT sum(x) FROM rel_proxy.t") == "1\n"

    node1.query("DROP DATABASE rel_proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE rel_src")


def test_cycle_formed_by_config_reload(started_cluster):
    # A chain of proxy databases that refers back to itself is rejected at CREATE, but the check is
    # inherently racy against the configuration: a reload can turn a shard of a `Cluster` database
    # into a local one after the databases were created. Such a live cycle must not recurse and,
    # crucially, must not fail whole-server scans (`system.tables` and the like) for unrelated
    # queries; only resolution against the cyclic databases themselves reports the cycle.
    node1.query("CREATE DATABASE cyc_a ENGINE = Cluster('reloadable', 'cyc_b')")
    try:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_NODE2_ONLY)
        node1.query("SYSTEM RELOAD CONFIG")

        # No shard of `reloadable` is local now, so the chain does not pass through this server
        # and completing it is allowed.
        node1.query("CREATE DATABASE cyc_b ENGINE = Cluster('reloadable', 'cyc_a')")

        # The reload makes the shard local again: the cycle is now live.
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")

        # A whole-server scan is unaffected, and the listing of the cyclic databases terminates
        # with an empty result instead of an error or an infinite recursion.
        node1.query("SELECT name FROM system.tables FORMAT Null")
        assert node1.query("SHOW TABLES FROM cyc_a") == ""

        # Resolution against a database of the cycle reports it.
        assert "INFINITE_LOOP" in node1.query_and_get_error("SELECT * FROM cyc_a.t")

        # Completing yet another chain into the cycle is rejected eagerly again.
        assert "INFINITE_LOOP" in node1.query_and_get_error(
            "CREATE DATABASE cyc_c ENGINE = Cluster('reloadable', 'cyc_a')"
        )

        node1.query("DROP DATABASE cyc_b")
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")
        node1.query("DROP DATABASE IF EXISTS cyc_b")
        node1.query("DROP DATABASE cyc_a")


def test_cycle_with_healthy_remote_replica_lists_no_tables(started_cluster):
    # A live local cycle whose shard also has a healthy remote replica. The listing of a database
    # of the cycle must stay empty instead of being completed from the remote replica, and
    # `EXISTS TABLE` must not fall back to it either: resolving the same names refuses the
    # remote-replica fallback with `INFINITE_LOOP`, so a completed listing would show table names
    # that can be neither described nor read.
    for name in ("leak_a", "leak_b"):
        node2.query(f"CREATE DATABASE {name}")
        node2.query(f"CREATE TABLE {name}.t (x UInt64) ENGINE = MergeTree ORDER BY x")

    try:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_NODE2_ONLY)
        node1.query("SYSTEM RELOAD CONFIG")

        # No shard of `reloadable` is local now, so completing the chain is allowed.
        node1.query("CREATE DATABASE leak_a ENGINE = Cluster('reloadable', 'leak_b')")
        node1.query("CREATE DATABASE leak_b ENGINE = Cluster('reloadable', 'leak_a')")

        # The reload turns one replica of the shard local, making the cycle live, while node2
        # stays a healthy remote replica of the same shard that does serve both database names.
        node1.replace_config(
            RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD_TWO_REPLICAS
        )
        node1.query("SYSTEM RELOAD CONFIG")

        # A whole-server scan is unaffected.
        node1.query("SELECT name FROM system.tables FORMAT Null")

        for name in ("leak_a", "leak_b"):
            assert node1.query(f"SHOW TABLES FROM {name}") == ""
            assert node1.query(f"EXISTS TABLE {name}.t") == "0\n"
            assert "INFINITE_LOOP" in node1.query_and_get_error(
                f"SELECT * FROM {name}.t"
            )
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")
        for name in ("leak_a", "leak_b"):
            node1.query(f"DROP DATABASE IF EXISTS {name}")
            node2.query(f"DROP DATABASE IF EXISTS {name}")


def test_missing_cluster(started_cluster):
    # A mistyped cluster name fails the CREATE right away.
    assert "CLUSTER_DOESNT_EXIST" in node1.query_and_get_error(
        "CREATE DATABASE no_proxy ENGINE = Cluster('no_such_cluster', 'default')"
    )

    # A database whose cluster has disappeared from the configuration must not prevent the server
    # from starting; its queries report the missing cluster until the configuration brings it back.
    node1.query("CREATE DATABASE m_src")
    node1.query("CREATE TABLE m_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("CREATE DATABASE m_proxy ENGINE = Cluster('reloadable', 'm_src')")

    try:
        node1.replace_config(
            RELOADABLE_CLUSTER_CONFIG_PATH, "<clickhouse></clickhouse>\n"
        )
        node1.restart_clickhouse()

        assert (
            node1.query("SELECT count() FROM system.databases WHERE name = 'm_proxy'")
            == "1\n"
        )
        assert "CLUSTER_DOESNT_EXIST" in node1.query_and_get_error(
            "SHOW TABLES FROM m_proxy"
        )
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")

    # The configuration is back; the same database serves the tables again without a restart.
    assert node1.query("SHOW TABLES FROM m_proxy") == "t\n"

    node1.query("DROP DATABASE m_proxy")
    node1.query("DROP DATABASE m_src")

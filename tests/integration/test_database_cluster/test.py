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

    # The table is exposed as a re-executable `Distributed` definition over the named cluster,
    # including the implicit rand() sharding key of a multi-shard database.
    create_query = node1.query("SHOW CREATE TABLE proxy.t FORMAT TSVRaw").strip()
    assert "Distributed('two_shards', 'src', 't', rand())" in create_query

    node1.query("INSERT INTO proxy.t VALUES (10)")
    assert node1.query("SELECT sum(x) FROM proxy.t") == "13\n"

    # DDL against the database is rejected.
    assert "NOT_IMPLEMENTED" in node1.query_and_get_error("DROP TABLE proxy.t")

    node1.query("DROP DATABASE proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE src")


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

    node1.query("DROP DATABASE fb_proxy")
    node2.query("DROP DATABASE fb_src")


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

    try:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_TWO_SHARDS)
        node1.query("SYSTEM RELOAD CONFIG")

        assert node1.query("SELECT sum(x) FROM rel_proxy.t") == "3\n"
        # The database is now multi-shard, so its proxy tables gain the implicit sharding key.
        create_query = node1.query(
            "SHOW CREATE TABLE rel_proxy.t FORMAT TSVRaw"
        ).strip()
        assert "Distributed('reloadable', 'rel_src', 't', rand())" in create_query
    finally:
        node1.replace_config(RELOADABLE_CLUSTER_CONFIG_PATH, RELOADABLE_ONE_SHARD)
        node1.query("SYSTEM RELOAD CONFIG")

    assert node1.query("SELECT sum(x) FROM rel_proxy.t") == "1\n"

    node1.query("DROP DATABASE rel_proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE rel_src")


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

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
)

node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
)

config1 = """<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <replica>
                    <host>node1</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>node3</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
    </remote_servers>
</clickhouse>"""

config2 = """<clickhouse>
    <remote_servers>
        <test_cluster>
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
            <shard>
                <replica>
                    <host>node3</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
    </remote_servers>
</clickhouse>
"""

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for _, node in cluster.instances.items():
            node.query(
                f"""
                create table dist_local (c1 Int32, c2 String) engine=MergeTree() order by c1;
                create table dist (c1 Int32, c2 String) engine=Distributed(test_cluster, currentDatabase(), dist_local, intHash32(c1));
                """
    )
        yield cluster
    finally:
        cluster.shutdown()


def test_distributed_async_insert(started_cluster):
        node1.query("insert into dist select number,'A' from system.numbers limit 10;")
        node1.query("system flush distributed dist;")

        assert int(node3.query("select count() from dist_local where c2 = 'A'")) == 5
        assert int(node1.query("select count() from dist_local where c2 = 'A'")) == 5

        # Add node2
        node1.replace_config(
            "/etc/clickhouse-server/config.d/remote_servers.xml",
            config2)
        node1.query("SYSTEM RELOAD CONFIG;")

        node2.replace_config(
        "/etc/clickhouse-server/config.d/remote_servers.xml",
        config2)
        node2.query("SYSTEM RELOAD CONFIG;")

        node3.replace_config(
            "/etc/clickhouse-server/config.d/remote_servers.xml",
            config2)
        node3.query("SYSTEM RELOAD CONFIG;")

        node1.query("insert into dist select number,'B' from system.numbers limit 12;")
        node1.query("system flush distributed dist;")

        assert int(node1.query("select count() from dist_local where c2 = 'B'")) == 4
        assert int(node2.query("select count() from dist_local where c2 = 'B'")) == 4
        assert int(node3.query("select count() from dist_local where c2 = 'B'")) == 4

        # Delete node2
        node1.replace_config(
            "/etc/clickhouse-server/config.d/remote_servers.xml",
            config1)
        node1.query("SYSTEM RELOAD CONFIG;")

        node2.replace_config(
            "/etc/clickhouse-server/config.d/remote_servers.xml",
            config1)
        node2.query("SYSTEM RELOAD CONFIG;")

        node3.replace_config(
            "/etc/clickhouse-server/config.d/remote_servers.xml",
            config1)
        node3.query("SYSTEM RELOAD CONFIG;")

        node1.query("insert into dist select number,'C' from system.numbers limit 10;")
        node1.query("system flush distributed dist;")

        assert int(node1.query("select count() from dist_local where c2 = 'C'")) == 5
        assert int(node2.query("select count() from dist_local where c2 = 'C'")) == 0
        assert int(node3.query("select count() from dist_local where c2 = 'C'")) == 5

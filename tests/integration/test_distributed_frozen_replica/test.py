import pytest
import time

from helpers.cluster import ClickHouseCluster, QueryRuntimeException


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/listen_host.xml"],
    macros={"replica": "1", "shard": "1"},
    with_zookeeper=True,
    ipv4_address="10.5.172.11",
    ipv6_address="2001:3984:3989::11",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/listen_host.xml"],
    macros={"replica": "2", "shard": "1"},
    with_zookeeper=True,
    ipv4_address="10.5.172.12",
    ipv6_address="2001:3984:3989::12",
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml", "configs/listen_host.xml"],
    macros={"replica": "1", "shard": "2"},
    with_zookeeper=True,
    ipv4_address="10.5.172.13",
    ipv6_address="2001:3984:3989::13",
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml", "configs/listen_host.xml"],
    macros={"replica": "2", "shard": "2"},
    with_zookeeper=True,
    ipv4_address="10.5.172.14",
    ipv6_address="2001:3984:3989::14",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for idx, node in enumerate([node1, node2, node3, node4]):
            node.query(
                """
                CREATE TABLE local_table(i Int64, d DateTime)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/local_table/{shard}', '{replica}')
                PARTITION BY toYYYYMMDD(d) ORDER BY d
                """
            )
            node.query(
                """
                CREATE TABLE distributed_table(i Int64)
                ENGINE = Distributed(test_cluster, default, local_table, rand())
                """
            )
            node.query(f"INSERT INTO local_table VALUES ({idx}, now())")
        yield cluster
    finally:
        cluster.shutdown()


def test_cluster_all_replicas_query(started_cluster):
    # Make sure that both IPv4 and IPv6 will be resolved freshly
    node3.query("SYSTEM DROP DNS CACHE")
    error = ""
    with cluster.pause_container("node1"):
        try:
            node3.query(
                """
                SELECT count() FROM clusterAllReplicas('test_cluster', default.local_table)
                SETTINGS handshake_timeout_ms=857
                """,
                timeout=10,  # the query should fail fast because of the low handshake timeout
            )
        except QueryRuntimeException as e:
            error = str(e)
    assert "ALL_CONNECTION_TRIES_FAILED" in error
    assert "receive timeout 857 ms" in error


def test_distributed_query(started_cluster):
    # Make sure that both IPv4 and IPv6 will be resolved freshly
    node3.query("SYSTEM DROP DNS CACHE")
    # Stop fetches on node2 so that it will be considered stale and node1 is tried first
    node2.query("SYSTEM STOP FETCHES")
    try:
        node1.query("DELETE FROM local_table WHERE i = 5")
        node1.query("INSERT INTO local_table VALUES (5, now())")
        time.sleep(10)
        assert (
            int(
                node2.query(
                    "SELECT absolute_delay FROM system.replicas WHERE database = 'default' AND table = 'local_table'"
                ).strip()
            )
            >= 5
        )
        with cluster.pause_container("node1"):
            assert (
                int(
                    node3.query(
                        """
                SELECT count() FROM distributed_table
                SETTINGS handshake_timeout_ms=100, max_replica_delay_for_distributed_queries=1
                """,
                        timeout=10,
                    ).strip()
                )
                == 4
            )
    finally:
        node2.query("SYSTEM START FETCHES")

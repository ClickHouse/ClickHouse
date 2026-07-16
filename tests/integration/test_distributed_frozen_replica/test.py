import json
import pytest
import time
import uuid

from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.network import PartitionManager


main_configs = [
    "configs/remote_servers.xml",
    "configs/listen_host.xml",
    "configs/ssl_conf.xml",
    "configs/dhparam.pem",
    "configs/server.crt",
    "configs/server.key",
]

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    macros={"replica": "1", "shard": "1"},
    with_zookeeper=True,
    ipv4_address="10.5.172.11",
    ipv6_address="2001:3984:3989::11",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    macros={"replica": "2", "shard": "1"},
    with_zookeeper=True,
    ipv4_address="10.5.172.12",
    ipv6_address="2001:3984:3989::12",
)
node3 = cluster.add_instance(
    "node3",
    main_configs=main_configs,
    macros={"replica": "1", "shard": "2"},
    with_zookeeper=True,
    ipv4_address="10.5.172.13",
    ipv6_address="2001:3984:3989::13",
)
node4 = cluster.add_instance(
    "node4",
    main_configs=main_configs,
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
            node.query(
                """
                CREATE TABLE distributed_table_secure(i Int64)
                ENGINE = Distributed(test_cluster_secure, default, local_table, rand())
                """
            )
            node.query(f"INSERT INTO local_table VALUES ({idx}, now())")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("cluster_name", ["test_cluster", "test_cluster_secure"])
@pytest.mark.parametrize("connections_with_failover_max_tries", [1, 3])
def test_cluster_all_replicas_query(
    started_cluster, cluster_name, connections_with_failover_max_tries
):
    # Make sure that both IPv4 and IPv6 will be resolved freshly
    node3.query("SYSTEM DROP DNS CACHE")
    error = ""
    with cluster.pause_container("node1"):
        try:
            node3.query(
                f"""
                SELECT count() FROM clusterAllReplicas({cluster_name}, default.local_table)
                SETTINGS
                    handshake_timeout_ms=857,
                    connect_timeout_with_failover_ms=100,
                    connect_timeout_with_failover_secure_ms=100,
                    connections_with_failover_max_tries={connections_with_failover_max_tries}
                """,
                timeout=10,  # the query should fail fast because of the low handshake and connection timeouts
            )
        except QueryRuntimeException as e:
            error = str(e)
    assert "ALL_CONNECTION_TRIES_FAILED" in error
    assert "receive timeout 857 ms" in error


@pytest.mark.parametrize(
    "distributed_table", ["distributed_table", "distributed_table_secure"]
)
@pytest.mark.parametrize("connections_with_failover_max_tries", [1, 3])
def test_distributed_query(
    started_cluster, distributed_table, connections_with_failover_max_tries
):
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
        query_id = str(uuid.uuid4())
        with cluster.pause_container("node1"):
            assert (
                int(
                    node3.query(
                        f"""
                SELECT count() FROM {distributed_table}
                SETTINGS
                    handshake_timeout_ms=100,
                    connect_timeout_with_failover_ms=100,
                    connect_timeout_with_failover_secure_ms=100,
                    connections_with_failover_max_tries={connections_with_failover_max_tries},
                    max_replica_delay_for_distributed_queries=1
                """,
                        timeout=10,  # the query should succeed fast despite the paused replica being tried first because of the low handshake and connection timeouts
                        query_id=query_id,
                    ).strip()
                )
                == 4
            )
        node3.query("SYSTEM FLUSH LOGS")
        profile_events = json.loads(
            node3.query(
                f"SELECT ProfileEvents FROM system.query_log WHERE query_id = '{query_id}' AND is_initial_query AND type = 'QueryFinish' LIMIT 1 FORMAT JSONEachRow"
            )
        )["ProfileEvents"]
        assert (
            profile_events["DistributedConnectionTries"]
            == connections_with_failover_max_tries + 1
        )
        assert profile_events["DistributedConnectionFailAtAll"] == 1
        assert profile_events["DistributedConnectionStaleReplica"] == 1
    finally:
        node2.query("SYSTEM START FETCHES")


@pytest.mark.parametrize(
    "distributed_table", ["distributed_table", "distributed_table_secure"]
)
@pytest.mark.parametrize("connections_with_failover_max_tries", [1, 3])
def test_distributed_query_network_timeout(
    started_cluster, distributed_table, connections_with_failover_max_tries
):
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
        query_id = str(uuid.uuid4())
        with PartitionManager() as pm:
            port = 9440 if "secure" in distributed_table else 9000
            pm.partition_instances(node1, node3, port=port)
            assert (
                int(
                    node3.query(
                        f"""
                SELECT count() FROM {distributed_table}
                SETTINGS
                    handshake_timeout_ms=100,
                    connect_timeout_with_failover_ms=100,
                    connect_timeout_with_failover_secure_ms=100,
                    connections_with_failover_max_tries={connections_with_failover_max_tries},
                    max_replica_delay_for_distributed_queries=1
                """,
                        timeout=10,  # the query should succeed fast despite the unreachable replica being tried first because of the low handshake and connection timeouts
                        query_id=query_id,
                    ).strip()
                )
                == 4
            )
        node3.query("SYSTEM FLUSH LOGS")
        profile_events = json.loads(
            node3.query(
                f"SELECT ProfileEvents FROM system.query_log WHERE query_id = '{query_id}' AND is_initial_query AND type = 'QueryFinish' LIMIT 1 FORMAT JSONEachRow"
            )
        )["ProfileEvents"]
        assert (
            profile_events["DistributedConnectionTries"]
            == connections_with_failover_max_tries + 1
        )
        assert profile_events["DistributedConnectionFailAtAll"] == 1
        assert profile_events["DistributedConnectionStaleReplica"] == 1
    finally:
        node2.query("SYSTEM START FETCHES")

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
    )
    for i in (1, 2, 3, 4)
]
# Two independent data clusters, joined by the outer cluster through one server of each.
DATA_CLUSTERS = [("default_cluster", nodes[0:2]), ("inner_cluster", nodes[2:4])]

TABLE = "logs"
INNER_DIST = "logs_dist"
OUTER_DIST = "logs_dist_dist"

SERVICE = "id-api-auth-service"
WINDOW_START = "2026-09-28 15:40:00"
WINDOW_END = "2026-09-28 15:45:00"

QUERY = f"""
    SELECT count(), avg(length(Body))
    FROM {OUTER_DIST}
    WHERE (ServiceName = '{SERVICE}')
      AND (Timestamp >= '{WINDOW_START}') AND (Timestamp < '{WINDOW_END}')
    """

# The default cluster holds 10 matching rows with a body of 10 bytes, the inner cluster holds 30 with a
# body of 20, so a cluster read twice or not at all moves both the count and the average:
# (10 * 10 + 30 * 20) / 40.
EXPECTED = "40\t17.5\n"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        create_tables()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables():
    for data_cluster, data_nodes in DATA_CLUSTERS:
        for i, node in enumerate(data_nodes):
            node.query(f"""
                CREATE TABLE {TABLE}
                (
                    Timestamp DateTime,
                    ServiceName LowCardinality(String),
                    Body String
                )
                Engine=ReplicatedMergeTree('/test_pr_dist_over_dist/{data_cluster}/{TABLE}', 'r{i}')
                ORDER BY (ServiceName, Timestamp)
                """)

        # The first server of each data cluster is the one the outer cluster addresses, so that is
        # where the distributed table over that cluster lives.
        data_nodes[0].query(f"""
            CREATE TABLE {INNER_DIST} AS {TABLE}
            Engine=Distributed({data_cluster}, currentDatabase(), {TABLE}, rand())
            """)

    nodes[0].query(f"""
        CREATE TABLE {OUTER_DIST} AS {INNER_DIST}
        Engine=Distributed(outer_cluster, currentDatabase(), {INNER_DIST}, rand())
        """)

    insert_data(nodes[0], matching_rows=10, body_length=10)
    insert_data(nodes[2], matching_rows=30, body_length=20)

    for _, data_nodes in DATA_CLUSTERS:
        for node in data_nodes:
            node.query(f"SYSTEM SYNC REPLICA {TABLE}")


def insert_data(node, matching_rows, body_length):
    node.query(f"""
        INSERT INTO {TABLE}
        SELECT toDateTime('{WINDOW_START}') + number, '{SERVICE}', repeat('a', {body_length})
        FROM numbers({matching_rows})
        """)
    # Rows the WHERE has to exclude. Their bodies are far longer than any matching row, so letting one
    # through shows up in the average and not only in the count.
    node.query(f"""
        INSERT INTO {TABLE} VALUES
            ('{WINDOW_START}', 'some-other-service', '{'x' * 100}'),
            (toDateTime('{WINDOW_START}') - 1, '{SERVICE}', '{'x' * 100}'),
            ('{WINDOW_END}', '{SERVICE}', '{'x' * 100}')
        """)


def parallel_replicas_query_count(query_id):
    """Number of (sub)queries that read with parallel replicas, summed over the whole cluster.

    The event is counted where the reading coordinator lives, which in a chain of distributed tables is
    the leaf replica and not the initiator, so every node has to be looked at."""
    total = 0
    for node in nodes:
        # SYSTEM FLUSH LOGS is not cluster-aware, it has to be issued on each node separately.
        node.query("SYSTEM FLUSH LOGS")
        total += int(
            node.query(
                f"""
                SELECT sum(ProfileEvents['ParallelReplicasQueryCount'])
                FROM system.query_log
                WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish'
                SETTINGS enable_parallel_replicas = 0
                """
            )
        )
    return total


@pytest.mark.parametrize("prefer_localhost_replica", [0, 1])
def test_parallel_replicas_over_distributed_over_distributed(
    start_cluster, prefer_localhost_replica
):
    # Without parallel replicas the query is an ordinary two-hop distributed read, which is the oracle
    # for the run below. The expected value is spelled out as well, so a bug that affects both paths
    # equally still fails the test.
    assert nodes[0].query(QUERY, settings={"enable_parallel_replicas": 0}) == EXPECTED

    query_id = str(uuid.uuid4())
    assert (
        nodes[0].query(
            QUERY,
            query_id=query_id,
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 2,
                "prefer_localhost_replica": prefer_localhost_replica,
            },
        )
        == EXPECTED
    )

    # One read per data cluster, and both of them must have used parallel replicas: the outer cluster
    # cannot use them for its own hop, since each of its shards has a single replica, but that says
    # nothing about the two-replica clusters below it. Without this the test would also pass when
    # parallel replicas are silently turned off somewhere along the chain.
    assert parallel_replicas_query_count(query_id) == 2

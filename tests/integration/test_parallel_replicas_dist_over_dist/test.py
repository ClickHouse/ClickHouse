import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Two instances in a hub-and-spoke arrangement. Each carries its own `remote_servers`, where the
# cluster named `default` - the one a Cloud instance always has - is that instance's own pair of
# replicas. Only the hub knows `outer_cluster`, the entry point that fans a query out to both.
# One `ClickHouseCluster` is used for both: separate `ClickHouseCluster` objects get separate docker
# compose projects and therefore separate networks, so their nodes could not reach each other.
hub_nodes = [
    cluster.add_instance(f"n{i}", main_configs=["configs/hub.xml"], with_zookeeper=True)
    for i in (1, 2)
]
spoke_nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/spoke.xml"], with_zookeeper=True
    )
    for i in (3, 4)
]
INSTANCES = [("hub", hub_nodes), ("spoke", spoke_nodes)]
nodes = hub_nodes + spoke_nodes

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

# The hub holds 1000 matching rows with a body of 10 bytes, the spoke holds 3000 with a body of 20, so
# an instance read twice or not at all moves both the count and the average:
# (1000 * 10 + 3000 * 20) / 4000.
EXPECTED = "4000\t17.5\n"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        create_tables()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables():
    for instance, instance_nodes in INSTANCES:
        for i, node in enumerate(instance_nodes):
            node.query(f"""
                CREATE TABLE {TABLE}
                (
                    Timestamp DateTime,
                    ServiceName LowCardinality(String),
                    Body String
                )
                Engine=ReplicatedMergeTree('/test_pr_dist_over_dist/{instance}/{TABLE}', 'r{i}')
                ORDER BY (ServiceName, Timestamp)
                -- Small granules so there are mark segments to spread over the replicas of a shard.
                SETTINGS index_granularity = 8
                """)

            # Every server reads its own instance through the same cluster name.
            node.query(f"""
                CREATE TABLE {INNER_DIST} AS {TABLE}
                Engine=Distributed(default, currentDatabase(), {TABLE}, rand())
                """)

    hub_nodes[0].query(f"""
        CREATE TABLE {OUTER_DIST} AS {INNER_DIST}
        Engine=Distributed(outer_cluster, currentDatabase(), {INNER_DIST}, rand())
        """)

    insert_data(hub_nodes[0], matching_rows=1000, body_length=10)
    insert_data(spoke_nodes[0], matching_rows=3000, body_length=20)

    for node in nodes:
        node.query(f"SYSTEM SYNC REPLICA {TABLE}")


def insert_data(node, matching_rows, body_length):
    node.query(f"""
        INSERT INTO {TABLE}
        SELECT toDateTime('{WINDOW_START}') + (number % 300), '{SERVICE}', repeat('a', {body_length})
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


def parallel_replicas_coordinators(query_id):
    """Names of the nodes that hosted a parallel-replicas reading coordinator """

    coordinators = []
    for node in nodes:
        # SYSTEM FLUSH LOGS is not cluster-aware, it has to be issued on each node separately.
        node.query("SYSTEM FLUSH LOGS")
        reads = int(
            node.query(
                f"""
                SELECT countIf(ProfileEvents['ParallelReplicasNumRequests'] > 0)
                FROM system.query_log
                WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish'
                SETTINGS enable_parallel_replicas = 0
                """
            )
        )
        coordinators += [node.name] * reads
    return coordinators


@pytest.mark.parametrize("prefer_localhost_replica", [0, 1])
def test_parallel_replicas_over_distributed_over_distributed(
    start_cluster, prefer_localhost_replica
):
    # Without parallel replicas
    assert (
        hub_nodes[0].query(QUERY, settings={"enable_parallel_replicas": 0}) == EXPECTED
    )

    query_id = str(uuid.uuid4())
    assert (
        hub_nodes[0].query(
            QUERY,
            query_id=query_id,
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 2,
                "prefer_localhost_replica": prefer_localhost_replica,
                # The automatic decision must not turn parallel replicas off on this small data, and
                # one mark segment per granule lets the reading spread over both replicas.
                "automatic_parallel_replicas_mode": 0,
                "parallel_replicas_mark_segment_size": 1,
            },
        )
        == EXPECTED
    )

    # Exactly one parallel-replicas read inside each instance, over that instance's own `default`
    # cluster of two replicas
    coordinators = parallel_replicas_coordinators(query_id)
    assert [
        sum(name in [node.name for node in instance_nodes] for name in coordinators)
        for _, instance_nodes in INSTANCES
    ] == [1, 1], coordinators

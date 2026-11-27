"""
Per-node ports for the distributed-plan engine (`make_distributed_plan`).

The 3 worker nodes use different task-dispatch (interserver) and streaming-exchange
ports, declared per replica (`stateless_worker_port` / `streaming_exchange_port`):

    node1  interserver 9009  exchange 9223
    node2  interserver 9010  exchange 9224
    node3  interserver 9011  exchange 9225

Streaming queries dispatch each task to its node's interserver port and dial each
producer on its own exchange port, so they succeed only when the per-node ports are
honored; the pre-fix single-port assumption cannot reach node2/node3.
"""

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

CLUSTER_CONFIG_PATH = "/etc/clickhouse-server/config.d/cluster.xml"

pytestmark = pytest.mark.timeout(300)

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/cluster.xml", "configs/node1.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/cluster.xml", "configs/node2.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/cluster.xml", "configs/node3.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 3},
)

NODES = [node1, node2, node3]
INITIATOR = node1

# Streaming exchange exercises both per-node ports: task dispatch (interserver)
# and worker-to-worker exchange connections. 3 buckets match the 3-node cluster.
STREAMING_SETTINGS = ", ".join(
    [
        "make_distributed_plan = 1",
        "enable_parallel_replicas = 0",
        "distributed_plan_default_shuffle_join_bucket_count = 3",
        "distributed_plan_default_reader_bucket_count = 3",
        "distributed_plan_force_exchange_kind = 'Streaming'",
        "query_plan_use_new_logical_join_step = 1",
        "enable_join_runtime_filters = 0",
    ]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        _create_table_and_load_data()
        yield cluster
    finally:
        cluster.shutdown()


def _create_table_and_load_data():
    """Create a ReplicatedMergeTree table on every node so all replicas hold an
    identical set of parts (required by the bucketed distributed read), and load
    several parts."""
    for node in NODES:
        node.query(
            """
            CREATE TABLE big (id UInt64, group_key UInt32, payload String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/big', '{replica}')
            ORDER BY id
            """
        )
        node.query("SYSTEM STOP MERGES big")

    # Load on a single replica; replication propagates to the others. Multiple
    # inserts produce multiple parts so the parallel read splits real work per bucket.
    for batch in range(4):
        offset = batch * 25_000
        INITIATOR.query(
            f"""
            INSERT INTO big
            SELECT number + {offset} AS id,
                   (number + {offset}) % 100 AS group_key,
                   concat('p_', toString(number + {offset})) AS payload
            FROM numbers(25000)
            """
        )

    for node in NODES:
        node.query("SYSTEM SYNC REPLICA big")
        assert int(node.query("SELECT count() FROM big").strip()) == 100_000


def _assert_distributed_matches_baseline(query: str, extra_settings: str = ""):
    settings = STREAMING_SETTINGS
    if extra_settings:
        settings += ", " + extra_settings
    distributed = INITIATOR.query(f"{query} SETTINGS {settings}")
    baseline = INITIATOR.query(f"{query} SETTINGS make_distributed_plan = 0")
    assert distributed == baseline


def test_parallel_read(started_cluster):
    """Bucketed scan across the 3 nodes, gathered to the initiator; each task is
    dispatched to its node's own interserver port."""
    _assert_distributed_matches_baseline(
        "SELECT count(), sum(id), sum(group_key) FROM big"
    )


def test_shuffle_aggregation(started_cluster):
    """GROUP BY shuffled by hash across the 3 nodes: every node both produces and
    consumes streams, so every node is dialed on its own exchange port."""
    _assert_distributed_matches_baseline(
        "SELECT group_key, count(), sum(id) FROM big GROUP BY group_key ORDER BY group_key",
        extra_settings="distributed_plan_force_shuffle_aggregation = 1",
    )


def test_shuffle_hash_join(started_cluster):
    """Self-join on a non-key expression forces a shuffle of both inputs by the
    join key, fanning streams across all per-node exchange ports."""
    _assert_distributed_matches_baseline(
        """
        SELECT count()
        FROM big AS a
        INNER JOIN big AS b ON a.id = b.id + 1
        WHERE a.group_key < 5
        """
    )


def test_wrong_per_node_exchange_port_fails(started_cluster):
    """Break node2's declared exchange port in the initiator's cluster view: consumers
    are then told to dial a dead port and the streaming query fails. Confirms the
    per-node exchange port is actually consulted (the pre-fix single-port assumption
    ignores it). Runs last and restores the config in `finally`."""
    INITIATOR.replace_in_config(
        CLUSTER_CONFIG_PATH,
        "<streaming_exchange_port>9224</streaming_exchange_port>",
        "<streaming_exchange_port>9999</streaming_exchange_port>",
    )
    INITIATOR.query("SYSTEM RELOAD CONFIG")
    try:
        with pytest.raises(QueryRuntimeException):
            INITIATOR.query(
                "SELECT group_key, count(), sum(id) FROM big GROUP BY group_key ORDER BY group_key"
                f" SETTINGS {STREAMING_SETTINGS}, distributed_plan_force_shuffle_aggregation = 1"
            )
    finally:
        INITIATOR.replace_in_config(
            CLUSTER_CONFIG_PATH,
            "<streaming_exchange_port>9999</streaming_exchange_port>",
            "<streaming_exchange_port>9224</streaming_exchange_port>",
        )
        INITIATOR.query("SYSTEM RELOAD CONFIG")

"""
The tasks of a distributed query that run on one server are scheduled as a part of the query: they
share its scheduling group, and so its query slot. With `max_concurrent_queries = 1` a task waits
for the slot held by its own initiator unless it joins it.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/stateless_worker.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/stateless_worker.xml"],
    stay_alive=True,
)

NODES = [node1, node2]

DISTRIBUTED_SETTINGS = (
    "make_distributed_plan = 1, "
    "enable_parallel_replicas = 0, "
    "distributed_plan_default_shuffle_join_bucket_count = 2, "
    "distributed_plan_default_reader_bucket_count = 2, "
    "distributed_plan_max_rows_to_broadcast = 0, "
    "distributed_plan_fallback_to_local_execution = 0"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in NODES:
            node.query(
                "CREATE TABLE test_query_parts (id UInt64) ENGINE = MergeTree() ORDER BY id"
            )
            node.query("INSERT INTO test_query_parts SELECT number FROM numbers(10000)")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def single_query_slot(started_cluster):
    for node in NODES:
        node.query(
            """
            CREATE RESOURCE query (QUERY);
            CREATE WORKLOAD all SETTINGS max_concurrent_queries = 1;
            """
        )
    yield
    for node in NODES:
        node.query(
            """
            DROP WORKLOAD IF EXISTS all;
            DROP RESOURCE IF EXISTS query;
            """
        )


def joined_slots(node):
    return int(
        node.query(
            "SELECT value FROM system.events WHERE event = 'ConcurrentQuerySlotsJoined'"
        )
        or 0
    )


def test_tasks_share_query_slot(started_cluster):
    joined_before = joined_slots(node1)
    query_id = str(uuid.uuid4())

    result = node1.query(
        f"""
        SELECT count(), sum(c)
        FROM (SELECT id % 100 AS k, count() AS c FROM test_query_parts GROUP BY k)
        SETTINGS workload = 'all', workload_admission_timeout_ms = 30000, {DISTRIBUTED_SETTINGS}
        """,
        query_id=query_id,
        timeout=120,
    )
    assert result == "100\t10000\n"

    # The initiator holds the only slot of the workload on node1 for the whole query, so the tasks
    # that run on node1 could only proceed by joining it.
    assert joined_slots(node1) > joined_before


def test_distributed_subqueries_share_query_slot(started_cluster):
    # Every distributed subquery has its own distributed plan; the tasks of all of them are parts of
    # the same query and share its slot. The outer query reads a table too, because a plan that reads
    # `system.one` cannot be distributed.
    joined_before = joined_slots(node1)
    result = node1.query(
        f"""
        SELECT
            count(),
            (SELECT count() FROM test_query_parts WHERE id % 2 = 0),
            (SELECT sum(id) FROM test_query_parts WHERE id % 2 = 1)
        FROM test_query_parts
        SETTINGS workload = 'all', workload_admission_timeout_ms = 30000, {DISTRIBUTED_SETTINGS}
        """,
        timeout=120,
    )
    assert result == "10000\t5000\t25000000\n"
    assert joined_slots(node1) > joined_before

"""
Distributed query execution against ReplicatedMergeTree tables.

Spins up a 3-node cluster, creates several ReplicatedMergeTree tables (filled
with multiple parts on each replica), and exercises:
  - parallel read of an RMT table,
  - shuffle hash join,
  - broadcast join,
  - shuffle aggregation,
  - distributed sort,
  - aggregation feeding a join.

Each distributed-plan query is compared against the same query without
make_distributed_plan to confirm the distributed path produces matching results.
The EXPLAIN PLAN of the distributed query is also compared against a baked-in
reference; if optimizer changes reshape a plan the test fails with a diff so
the reference can be updated deliberately.
"""

import logging
import textwrap
from typing import Optional

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

pytestmark = pytest.mark.timeout(300)

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.d/stateless_worker.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 3},
)

NODES = [node1, node2, node3]
INITIATOR = node1

# Settings common to all distributed-plan queries in this test. The 3 buckets
# match the 3-node cluster size. distributed_plan_max_rows_to_broadcast = 0
# prevents the optimizer from broadcasting the join's right side by default;
# individual tests override it when broadcast is the path under test.
#
# Several settings are pinned to keep the EXPLAIN snapshot stable against
# unrelated default-value changes elsewhere in the codebase: anything that
# can swap join sides, change join-side conversion, or fuse/split exchange
# steps would otherwise produce a different plan shape from one ClickHouse
# version to the next without indicating a real regression in this feature.
DISTRIBUTED_SETTINGS = ", ".join([
    "make_distributed_plan = 1",
    "enable_parallel_replicas = 0",
    "distributed_plan_default_shuffle_join_bucket_count = 3",
    "distributed_plan_default_reader_bucket_count = 3",
    "distributed_plan_max_rows_to_broadcast = 0",
    "distributed_plan_optimize_exchanges = 1",
    "query_plan_join_swap_table = 'false'",
    "query_plan_optimize_join_order_limit = 0",
    "query_plan_use_new_logical_join_step = 1",
    "query_plan_convert_join_to_in = 0",
    "query_plan_convert_outer_join_to_inner_join = 0",
    "query_plan_convert_any_join_to_semi_or_anti_join = 0",
    # Runtime filters are not yet implemented for distributed queries.
    "enable_join_runtime_filters = 0",
])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        _create_tables_and_load_data()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables_and_load_data():
    """Create RMT tables on every node and insert data in multiple batches so
    each replica ends up with several data parts."""
    for node in NODES:
        node.query(
            """
            CREATE TABLE big (id UInt64, group_key UInt32, payload String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/big', '{replica}')
            ORDER BY id
            """
        )
        node.query(
            """
            CREATE TABLE small (id UInt64, label String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/small', '{replica}')
            ORDER BY id
            """
        )

    # Stop background merges so the inserted parts are not collapsed before the
    # sanity check below — that check expects each replica to see > 1 part.
    for node in NODES:
        node.query("SYSTEM STOP MERGES big")
        node.query("SYSTEM STOP MERGES small")

    # Load data on a single replica; replication propagates it to the others.
    # Multiple inserts produce multiple data parts so the parallel read path
    # has more than one part to split per worker bucket.
    for batch in range(4):
        offset = batch * 25_000
        INITIATOR.query(
            f"""
            INSERT INTO big
            SELECT
                number + {offset} AS id,
                (number + {offset}) % 100 AS group_key,
                concat('p_', toString(number + {offset})) AS payload
            FROM numbers(25000)
            """
        )

    INITIATOR.query(
        """
        INSERT INTO small
        SELECT number AS id, concat('lbl_', toString(number)) AS label
        FROM numbers(500)
        """
    )

    # Wait for replication so all replicas see the full data set.
    for node in NODES:
        node.query("SYSTEM SYNC REPLICA big")
        node.query("SYSTEM SYNC REPLICA small")

    # Sanity-check: every replica sees all rows and more than one part on big.
    for node in NODES:
        assert int(node.query("SELECT count() FROM big").strip()) == 100_000
        assert int(node.query("SELECT count() FROM small").strip()) == 500
        parts_count = int(
            node.query(
                "SELECT count() FROM system.parts "
                "WHERE table = 'big' AND active = 1"
            ).strip()
        )
        assert parts_count >= 2, (
            f"expected >= 2 active parts on {node.name}, got {parts_count}"
        )


def _explain_and_check(query: str, settings: str, expected_plan: str):
    """Run EXPLAIN PLAN on the query and compare its output to the expected
    plan, line-by-line after stripping the common indentation. On mismatch
    the failure message includes both plans so the test author can update
    the reference.
    """
    actual = INITIATOR.query(f"EXPLAIN PLAN {query} SETTINGS {settings}").strip("\n")
    expected = textwrap.dedent(expected_plan).strip("\n")
    if actual != expected:
        pytest.fail(
            "distributed plan does not match the expected reference\n"
            "--- ACTUAL PLAN ---\n" + actual
            + "\n--- EXPECTED PLAN ---\n" + expected
        )
    return actual


def _run_both_ways(
    query: str,
    settings_override: str = "",
    expected_plan: Optional[str] = None,
):
    """Run the query in distributed and non-distributed modes and return both
    results for the caller to compare. If expected_plan is provided, also
    EXPLAIN PLAN the distributed query and assert the dumped plan matches.
    """
    settings = DISTRIBUTED_SETTINGS
    if settings_override:
        settings = settings + ", " + settings_override
    if expected_plan is not None:
        _explain_and_check(query, settings, expected_plan)
    distributed = INITIATOR.query(f"{query} SETTINGS {settings}")
    baseline = INITIATOR.query(f"{query} SETTINGS make_distributed_plan = 0")
    logging.info("distributed result:\n%s", distributed)
    logging.info("baseline result:\n%s", baseline)
    return distributed, baseline


EXCHANGE_KINDS = pytest.mark.parametrize("exchange_kind", ["Streaming", "Persisted"])


def _override(exchange_kind: str, *extra: str) -> str:
    parts = [f"distributed_plan_force_exchange_kind = '{exchange_kind}'"]
    parts.extend(extra)
    return ", ".join(parts)


@EXCHANGE_KINDS
def test_parallel_read(started_cluster, exchange_kind):
    """A scan of the RMT table goes through the distributed-read path with
    one task per reader bucket and a GatherExchange feeding the initiator."""
    distributed, baseline = _run_both_ways(
        "SELECT count(), sum(id), sum(group_key) FROM big",
        settings_override=_override(exchange_kind),
        expected_plan="""\
            Expression ((Project names + Projection))
              MergingAggregated (merge)
                GatherExchange
                  Aggregating (partial)
                    Expression ((Before GROUP BY + Change column names to column identifiers))
                      ReadFromMergeTree (default.big)
        """,
    )
    assert distributed == baseline


def test_parallel_read_missing_part_on_worker_errors(started_cluster):
    """A distributed read buckets over the parts the coordinator selected. If a
    worker replica is missing one of those parts (replication lag), the read must
    fail cleanly instead of silently returning a divergent slice of the data.

    Stop fetches on node2/node3 and add a new part only on the coordinator
    (node1); the read assigns a bucket to a lagging replica, which cannot find
    the coordinator-selected part and raises NO_SUCH_DATA_PART."""
    table = "big_lagging"
    for node in NODES:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"""
            CREATE TABLE {table} (id UInt64, group_key UInt32, payload String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}')
            ORDER BY id
            """
        )
        node.query(f"SYSTEM STOP MERGES {table}")

    # Initial data, replicated to every node (multiple parts).
    for batch in range(2):
        offset = batch * 25_000
        INITIATOR.query(
            f"INSERT INTO {table} SELECT number + {offset}, (number + {offset}) % 100, "
            f"concat('p_', toString(number + {offset})) FROM numbers(25000)"
        )
    for node in NODES:
        node.query(f"SYSTEM SYNC REPLICA {table}")

    node2.query(f"SYSTEM STOP FETCHES {table}")
    node3.query(f"SYSTEM STOP FETCHES {table}")
    try:
        # New part lands only on the coordinator; node2/node3 stay behind.
        INITIATOR.query(
            f"INSERT INTO {table} SELECT number + 50000, (number + 50000) % 100, "
            f"concat('p_', toString(number + 50000)) FROM numbers(25000)"
        )
        assert int(INITIATOR.query(f"SELECT count() FROM {table}").strip()) == 75_000
        assert int(node2.query(f"SELECT count() FROM {table}").strip()) == 50_000
        assert int(node3.query(f"SELECT count() FROM {table}").strip()) == 50_000

        settings = DISTRIBUTED_SETTINGS + ", distributed_plan_force_exchange_kind = 'Persisted'"
        with pytest.raises(QueryRuntimeException) as exc:
            INITIATOR.query(f"SELECT count(), sum(id) FROM {table} SETTINGS {settings}")
        assert "is not available on this replica" in str(exc.value)
    finally:
        node2.query(f"SYSTEM START FETCHES {table}")
        node3.query(f"SYSTEM START FETCHES {table}")
        for node in NODES:
            node.query(f"SYSTEM SYNC REPLICA {table}")
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")


@EXCHANGE_KINDS
def test_shuffle_hash_join(started_cluster, exchange_kind):
    """Self-join on a non-key expression so the optimizer cannot reuse the
    primary-key sort order and must shuffle both inputs by the join key."""
    distributed, baseline = _run_both_ways(
        """
        SELECT count()
        FROM big AS a
        INNER JOIN big AS b ON a.id = b.id + 1
        WHERE a.group_key < 5
        """,
        settings_override=_override(exchange_kind),
        expected_plan="""\
            Expression ((Project names + Projection))
              MergingAggregated (merge)
                GatherExchange
                  Aggregating (partial)
                    Expression ((Before GROUP BY + ))
                      JoinLogical
                        ShuffleExchange (by hash([__table1.id]))
                          Expression ((WHERE + Change column names to column identifiers))
                            ReadFromMergeTree (default.big)
                        ShuffleExchange (by hash([plus(__table2.id, 1_UInt8)]))
                          Expression (Calculate right join keys)
                            Expression (Change column names to column identifiers)
                              ReadFromMergeTree (default.big)
        """,
    )
    assert distributed == baseline


@EXCHANGE_KINDS
def test_broadcast_join(started_cluster, exchange_kind):
    """Join a large left side with a small right side; the small side is
    broadcast to every worker when the estimator returns its row count."""
    distributed, baseline = _run_both_ways(
        """
        SELECT count(), sum(b.id)
        FROM big AS b
        INNER JOIN small AS s ON b.id = s.id
        """,
        # Raise the broadcast threshold high enough to include `small`.
        settings_override=_override(
            exchange_kind,
            "distributed_plan_max_rows_to_broadcast = 10000",
        ),
        expected_plan="""\
            Expression ((Project names + Projection))
              MergingAggregated (merge)
                GatherExchange
                  Aggregating (partial)
                    Expression (Before GROUP BY)
                      JoinLogical
                        Expression (Change column names to column identifiers)
                          ReadFromMergeTree (default.big)
                        BroadcastExchange
                          Expression (Change column names to column identifiers)
                            ReadFromMergeTree (default.small)
        """,
    )
    assert distributed == baseline


@EXCHANGE_KINDS
def test_shuffle_aggregation(started_cluster, exchange_kind):
    """GROUP BY with a moderate number of groups so the optimizer picks the
    one-stage shuffle path (scatter by hash → aggregate → gather)."""
    distributed, baseline = _run_both_ways(
        """
        SELECT group_key, count(), sum(id)
        FROM big
        GROUP BY group_key
        ORDER BY group_key
        """,
        settings_override=_override(
            exchange_kind,
            "distributed_plan_force_shuffle_aggregation = 1",
        ),
        expected_plan="""\
            Expression (Project names)
              GatherExchange (sorted by (__table1.group_key ASC))
                Sorting (Sorting for ORDER BY)
                  Expression ((Before ORDER BY + Projection))
                    Aggregating
                      ShuffleExchange (by hash([__table1.group_key]))
                        Expression ((Before GROUP BY + Change column names to column identifiers))
                          ReadFromMergeTree (default.big)
        """,
    )
    assert distributed == baseline


@EXCHANGE_KINDS
def test_distributed_sort(started_cluster, exchange_kind):
    """ORDER BY ... LIMIT exercises the distributed sort path: each worker
    performs a partial sort, results are gathered and merged on the initiator."""
    distributed, baseline = _run_both_ways(
        """
        SELECT id, group_key
        FROM big
        WHERE group_key > 10
        ORDER BY id DESC, group_key ASC
        LIMIT 50
        """,
        settings_override=_override(exchange_kind),
        expected_plan="""\
            Expression (Project names)
              Limit (preliminary LIMIT)
                GatherExchange (sorted by (__table1.id DESC, __table1.group_key ASC))
                  Sorting (Sorting for ORDER BY)
                    Expression ((Before ORDER BY + Projection))
                      Expression ((WHERE + Change column names to column identifiers))
                        ReadFromMergeTree (default.big)
        """,
    )
    assert distributed == baseline


@EXCHANGE_KINDS
def test_join_with_aggregation(started_cluster, exchange_kind):
    """Combined shuffle: aggregate one side, broadcast the small side, then
    join. Verifies that exchanges around aggregation and join compose."""
    distributed, baseline = _run_both_ways(
        """
        SELECT g.group_key, g.cnt, s.label
        FROM (
            SELECT group_key, count() AS cnt FROM big GROUP BY group_key
        ) AS g
        INNER JOIN small AS s ON g.group_key = s.id
        ORDER BY g.group_key
        """,
        settings_override=_override(
            exchange_kind,
            "distributed_plan_max_rows_to_broadcast = 10000",
            "distributed_plan_force_shuffle_aggregation = 1",
        ),
        expected_plan="""\
            Expression (Project names)
              GatherExchange (sorted by (__table1.group_key ASC))
                Sorting (Sorting for ORDER BY)
                  Expression ((Before ORDER BY + Projection))
                    JoinLogical
                      Expression ((Change column names to column identifiers + (Project names + Projection)))
                        Aggregating
                          ShuffleExchange (by hash([__table2.group_key]))
                            Expression ((Before GROUP BY + Change column names to column identifiers))
                              ReadFromMergeTree (default.big)
                      BroadcastExchange
                        Expression (Change column names to column identifiers)
                          ReadFromMergeTree (default.small)
        """,
    )
    assert distributed == baseline

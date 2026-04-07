"""
Test that worker task failures in distributed plan queries are propagated
to the initiator with the correct error message instead of QUERY_WAS_CANCELLED.

Reproduces https://github.com/ClickHouse/clickhouse-private/issues/40546
(error-propagation part).
"""

import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


DISTRIBUTED_SETTINGS = (
    "make_distributed_plan = 1, "
    "enable_parallel_replicas = 0, "
    "distributed_plan_default_shuffle_join_bucket_count = 2, "
    "distributed_plan_default_reader_bucket_count = 2, "
    "distributed_plan_max_rows_to_broadcast = 0"
)


def test_worker_error_not_masked(started_cluster):
    """When a worker task fails due to a setting not being propagated, the
    initiator should report the worker's error, not QUERY_WAS_CANCELLED.

    sleepEachRow(0.01) on 10000 rows = 100s/block on the worker, exceeding
    the default function_sleep_max_microseconds_per_block (3s). The initiator
    sets a higher limit, but it's NOT propagated to workers — so the worker
    fails with error 160 (FUNCTION_THROW). The initiator should propagate this.
    """
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE IF NOT EXISTS test_err_mask (id UInt64) "
            "ENGINE = MergeTree() ORDER BY id"
        )
        node.query("INSERT INTO test_err_mask SELECT number FROM numbers(10000)")

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query(
            f"""
            SELECT count()
            FROM test_err_mask AS a JOIN test_err_mask AS b ON a.id = b.id
            WHERE sleepEachRow(0.01) = 0
            SETTINGS {DISTRIBUTED_SETTINGS},
                     function_sleep_max_microseconds_per_block = 1000000000
            """,
            timeout=30,
        )

    error_msg = str(exc_info.value)
    logging.info(f"Query failed with: {error_msg}")

    # The error should contain the actual worker failure (sleep time exceeded),
    # not QUERY_WAS_CANCELLED.
    assert "QUERY_WAS_CANCELLED" not in error_msg, (
        f"Initiator masked the worker error with QUERY_WAS_CANCELLED: {error_msg}"
    )
    assert "sleep" in error_msg.lower() or "maximum" in error_msg.lower(), (
        f"Expected sleep-related error from worker, got: {error_msg}"
    )

    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS test_err_mask")

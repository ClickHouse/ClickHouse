# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
)

node_distinct = cluster.add_instance(
    "node_distinct",
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_tmp_data_no_leftovers(start_cluster):
    q = node.get_query_request

    settings = {
        "max_bytes_ratio_before_external_group_by": 0,
        "max_bytes_ratio_before_external_sort": 0,
        "max_bytes_ratio_before_external_distinct": 0,
        "max_bytes_before_external_group_by": "10K",
        "max_bytes_before_external_sort": "10K",
        "max_bytes_before_external_distinct": "10K",
        "join_algorithm": "grace_hash",
        "max_bytes_in_join": "10K",
        "grace_hash_join_initial_buckets": "16",
    }

    # Run some queries in the background to generate temporary data
    q(
        "SELECT ignore(*) FROM numbers(10 * 1024 * 1024) ORDER BY sipHash64(number)",
        settings=settings,
    )
    q("SELECT * FROM system.numbers GROUP BY ALL", settings=settings)
    q("SELECT DISTINCT * FROM system.numbers", settings=settings)
    q(
        "SELECT * FROM system.numbers as t1 JOIN system.numbers as t2 USING (number)",
        settings=settings,
    )

    # Wait a bit to make sure the temporary data is written to disk
    time.sleep(5)

    # Hard restart the node
    node.restart_clickhouse(kill=True)
    path_to_data = "/var/lib/clickhouse/"

    # Check that there are no temporary files left.
    result = node.exec_in_container(["bash", "-c", f"ls -1 {path_to_data}tmp/"])
    assert result == ""


@pytest.mark.parametrize("cancel_stage", ["writing", "extraction"])
def test_distinct_cancellation_releases_temporary_data(start_cluster, cancel_stage):
    metric_query = "SELECT value FROM system.metrics WHERE metric = 'TemporaryFilesForDistinct'"
    baseline_metric = node_distinct.query(metric_query)

    def temporary_files():
        return node_distinct.exec_in_container(["ls", "-1", "/var/lib/clickhouse/tmp/"])

    baseline_files = temporary_files()
    query_id = str(uuid.uuid4())
    query = "SELECT DISTINCT number FROM system.numbers FORMAT Null"
    settings = {
        "max_threads": 1,
        "max_bytes_before_external_distinct": "1M",
        "max_bytes_ratio_before_external_distinct": 0,
        "max_untracked_memory": 0,
    }
    during_extraction = cancel_stage == "extraction"
    failpoint = "external_distinct_suppression_run_prepared_pause"
    if during_extraction:
        # The first block contains over 32 MiB of unique key bytes. The first run targets 16 MiB,
        # leaving the extractor's remaining keys and arena alive alongside the pending file run.
        query = (
            "SELECT DISTINCT concat(toString(number), repeat('x', 512)) AS k "
            "FROM numbers(131072) FORMAT Null"
        )
        settings.update(max_block_size=65536, optimize_distinct_in_order=0)
        node_distinct.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

    try:
        request = node_distinct.get_query_request(query, query_id=query_id, settings=settings)
        if during_extraction:
            # Bound the blocking wait so a failpoint that is never reached fails the test.
            node_distinct.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        else:
            # Wait until temporary data is written before cancelling the query.
            assert_eq_with_retry(
                node_distinct,
                "SELECT ProfileEvents['ExternalDistinctWritePart'] > 0"
                " AND ProfileEvents['ExternalDistinctCompressedBytes'] > 0"
                f" FROM system.processes WHERE query_id = '{query_id}'",
                "1",
                retry_count=100,
                sleep_time=0.1,
            )
    finally:
        try:
            # A paused processor cannot finish until the failpoint is released, so cancel asynchronously.
            kill_mode = "ASYNC" if during_extraction else "SYNC"
            node_distinct.query(f"KILL QUERY WHERE query_id = '{query_id}' {kill_mode}")
        finally:
            if during_extraction:
                node_distinct.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

    error = request.get_error()
    assert "QUERY_WAS_CANCELLED" in error, error
    assert_eq_with_retry(
        node_distinct,
        f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'",
        "0",
    )
    assert_eq_with_retry(node_distinct, metric_query, baseline_metric)
    wait_condition(
        temporary_files,
        lambda files: files == baseline_files,
        max_attempts=100,
        delay=0.1,
    )

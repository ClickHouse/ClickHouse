import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/memory_sampling.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_trace_processing_memory_samples(started_cluster):
    for i in range(10):
        node.query(
            """
            SELECT sum(c) FROM (
                SELECT concat(toString(number), repeat('q', 48)) AS k, count() AS c
                FROM numbers(180000) GROUP BY k
            ) SETTINGS max_threads = 2, memory_profiler_sample_probability = 0 FORMAT Null
            """,
            query_id=f"allocation-sample-workload-{i}",
        )
        node.query("SYSTEM FLUSH LOGS trace_log, text_log")
    node.query("SYSTEM FLUSH LOGS text_log")

    writer = int(
        node.query(
            """
            SELECT if(count() >= 5 AND uniqExact(thread_id) = 1, any(thread_id), 0)
            FROM system.text_log
            WHERE logger_name = 'SystemLog (system.trace_log)'
              AND startsWith(message, 'Flushed system log')
            """
        )
    )
    collector = int(
        node.query(
            """
            SELECT if(count() = 1, any(thread_id), 0)
            FROM system.stack_trace WHERE thread_name = 'TraceCollector'
            """
        )
    )
    assert writer and collector and writer != collector

    allocations, frees, writer_samples, collector_samples = map(
        int,
        node.query(
            f"""
            WITH startsWith(query_id, 'allocation-sample-workload-')
                 AND thread_id NOT IN ({writer}, {collector}) AS workload
            SELECT
                countIf(workload AND size > 0),
                countIf(workload AND size < 0),
                countIf(thread_id = {writer}),
                countIf(thread_id = {collector})
            FROM system.trace_log WHERE trace_type = 'MemorySample'
            """
        ).split(),
    )
    assert allocations > 0 and frees > 0
    assert (writer_samples, collector_samples) == (0, 0)

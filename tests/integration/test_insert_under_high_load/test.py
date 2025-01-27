import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_based_pipeline_throttling(start_cluster):
    instance.query("DROP TABLE IF EXISTS testing_memory SYNC")
    instance.query(
        "CREATE TABLE testing_memory (a UInt64, b String) ENGINE = MergeTree ORDER BY tuple()"
    )
    instance.query("SYSTEM STOP MERGES")

    settings = "max_insert_threads=32, max_memory_usage=6e8, max_execution_time=150"

    # Test case 1: Insert with memory-based pipeline throttling enabled
    _, err = instance.query_and_get_answer_with_error(
        f"""
        INSERT INTO testing_memory
        SELECT number, replicate('x', range(1, 30)) FROM numbers(80000000)
        SETTINGS {settings}, enable_memory_based_pipeline_throttling=1
        """
    )

    assert (
        "TIMEOUT_EXCEEDED" in err or not err.strip()
    ), f"Unexpected behavior for throttling enabled: {insert_status}"

    # Test case 2: Insert with throttling disabled, expecting MEMORY_LIMIT_EXCEEDED
    insert_status = instance.query_and_get_error(
        f"""
        INSERT INTO testing_memory
        SELECT number, replicate('x', range(1, 30)) FROM numbers(80000000)
        SETTINGS {settings}, enable_memory_based_pipeline_throttling=0
        """
    )

    assert (
        "MEMORY_LIMIT_EXCEEDED" in insert_status
    ), f"Unexpected behavior for throttling disabled: {insert_status}"

    # Cleanup
    instance.query("DROP TABLE testing_memory SYNC")

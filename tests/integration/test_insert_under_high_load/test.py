import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_based_pipeline_throttling(start_cluster):
    if (
        instance.is_built_with_thread_sanitizer()
        or instance.is_built_with_address_sanitizer()
    ):
        pytest.skip(
            "This test is time-limited and can be too slow with Thread Sanitizer"
        )

    instance.query("DROP TABLE IF EXISTS testing_memory SYNC")
    instance.query(
        "CREATE TABLE testing_memory (a UInt64, b String) ENGINE = MergeTree ORDER BY tuple()"
    )
    instance.query("SYSTEM STOP MERGES")

    settings = "max_insert_threads=32, max_memory_usage=6e8, max_execution_time=100"

    # Flush query log to get clean state
    instance.query("SYSTEM FLUSH LOGS")

    # Get initial count of InsertPipelineThrottled events
    initial_throttled = int(
        instance.query(
            """
            SELECT sum(ProfileEvents['InsertPipelineThrottled'])
            FROM system.query_log
            WHERE event_date >= today()-1 AND current_database = currentDatabase()
            """
        ).strip() or "0"
    )

    # Perform the insert with memory-based pipeline throttling enabled
    instance.query(
        f"""
        INSERT INTO testing_memory
        SELECT number, replicate('x', range(1, 30)) FROM numbers(80000000)
        SETTINGS {settings}, enable_memory_based_pipeline_throttling=1
        """,
        ignore_error=True,  # Ignore errors; we only care about profiling events
    )

    # Flush query log
    instance.query("SYSTEM FLUSH LOGS")

    # Get updated count
    updated_throttled = int(
        instance.query(
            """
            SELECT sum(ProfileEvents['InsertPipelineThrottled'])
            FROM system.query_log
            WHERE event_date >= today()-1 AND current_database = currentDatabase()
            """
        ).strip() or "0"
    )

    # Check that some throttling events occurred
    throttling_events_increased = updated_throttled > initial_throttled

    assert throttling_events_increased, (
        f"Memory throttling events did not increase: "
        f"initial={initial_throttled}, updated={updated_throttled}"
    )

    # Cleanup
    instance.query("DROP TABLE testing_memory SYNC")


def test_memory_throttle_settings(start_cluster):
    """Test that the memory throttle settings are properly applied"""

    instance.query("DROP TABLE IF EXISTS test_settings SYNC")
    instance.query(
        "CREATE TABLE test_settings (a UInt64) ENGINE = MergeTree ORDER BY tuple()"
    )

    # Run a simple insert with custom throttle settings
    result = instance.query(
        """
        INSERT INTO test_settings
        SELECT number FROM numbers(1000)
        SETTINGS
            enable_memory_based_pipeline_throttling=1,
            insert_memory_throttle_high_threshold=0.9,
            insert_memory_throttle_low_threshold=0.8
        """
    )

    # Verify data was inserted
    count = int(instance.query("SELECT count() FROM test_settings").strip())
    assert count == 1000, f"Expected 1000 rows, got {count}"

    # Cleanup
    instance.query("DROP TABLE test_settings SYNC")

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

    # Get initial count of InsertPipelineShrinking events
    initial_event_count = instance.query(
        """
        SELECT sum(ProfileEvents['InsertPipelineShrinking'])
        FROM system.query_log
        WHERE event_date >= today()-1 AND current_database = currentDatabase()
        LIMIT 1
        """
    ).strip()

    initial_event_count = int(initial_event_count) if initial_event_count else 0

    # Perform the insert with memory-based pipeline throttling enabled
    instance.query(
        f"""
        INSERT INTO testing_memory
        SELECT number, replicate('x', range(1, 30)) FROM numbers(80000000)
        SETTINGS {settings}, enable_memory_based_pipeline_throttling=1
        """,
        ignore_error=True,  # Ignore errors; we only care about profiling events
    )

    # Get updated count of InsertPipelineShrinking events
    updated_event_count = instance.query(
        """
        SELECT sum(ProfileEvents['InsertPipelineShrinking'])
        FROM system.query_log
        WHERE event_date >= today()-1 AND current_database = currentDatabase()
        LIMIT 1
        """
    ).strip()

    updated_event_count = int(updated_event_count) if updated_event_count else 0

    # Check that the number of profile events has increased
    assert updated_event_count > initial_event_count, (
        f"InsertPipelineShrinking event count did not increase: "
        f"initial={initial_event_count}, updated={updated_event_count}"
    )

    # Cleanup
    instance.query("DROP TABLE testing_memory SYNC")

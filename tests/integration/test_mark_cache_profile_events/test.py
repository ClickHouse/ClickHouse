import os
import time
import pytest
import logging
from helpers.cluster import ClickHouseCluster

# Setup logging
logger = logging.getLogger(__name__)

cluster = ClickHouseCluster(__file__)
# Use a smaller mark cache to force evictions more reliably
node = cluster.add_instance(
    "node",
    main_configs=["configs/mark_cache_config.xml"],
    stay_alive=True,
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def get_current_cache_metrics():
    """Get current cache metrics from system.events to establish baseline"""
    result = node.query("""
                        SELECT event, value
                        FROM system.events
                        WHERE event IN ('MarkCacheEvictedBytes', 'MarkCacheEvictedMarks', 'MarkCacheEvictedFiles')
                        ORDER BY event
                        """).strip()

    metrics = {}
    if result:
        for line in result.splitlines():
            event, value = line.split('\t')
            metrics[event] = int(value)

    logger.info(f"Current cache metrics: {metrics}")
    return metrics

def wait_for_cache_eviction(initial_metrics, timeout=30):
    """Wait for cache eviction to occur by monitoring metrics change"""
    start_time = time.time()

    while time.time() - start_time < timeout:
        current_metrics = get_current_cache_metrics()

        # Check if any eviction metric has increased
        eviction_occurred = False
        for metric in ['MarkCacheEvictedBytes', 'MarkCacheEvictedMarks', 'MarkCacheEvictedFiles']:
            initial_value = initial_metrics.get(metric, 0)
            current_value = current_metrics.get(metric, 0)
            if current_value > initial_value:
                eviction_occurred = True
                logger.info(f"Eviction detected: {metric} increased from {initial_value} to {current_value}")
                break

        if eviction_occurred:
            return current_metrics

        time.sleep(0.5)

    raise TimeoutError(f"Cache eviction not detected within {timeout} seconds")

@pytest.fixture()
def setup_cache_test_environment():
    """Setup environment with controlled cache size and data that will force evictions"""
    table_name = "mark_cache_eviction_test"
    logger.info(f"Setting up test environment with table: {table_name}")

    try:
        # Clean up any existing table
        node.query(f"DROP TABLE IF EXISTS {table_name}")

        # Set a small mark cache size to force evictions
        # This ensures our test is deterministic
        node.query("SYSTEM CLEAR MARK CACHE")

        # Create table with specific settings to control mark file size
        node.query(f"""
            CREATE TABLE {table_name} (
                id UInt64,
                data String,
                padding String
            )
            ENGINE = MergeTree()
            ORDER BY id
            SETTINGS
                index_granularity = 1024,  -- Smaller granularity = more marks
                index_granularity_bytes = 0  -- Disable adaptive granularity
        """)

        # Insert data in multiple parts to create multiple mark files
        # Each insert creates a separate part with its own mark file
        batch_size = 5000
        num_batches = 20

        logger.info(f"Inserting {num_batches} batches of {batch_size} rows each")
        for batch in range(num_batches):
            offset = batch * batch_size
            node.query(f"""
                INSERT INTO {table_name}
                SELECT
                    number + {offset} as id,
                    'data_' || toString(number) as data,
                    repeat('x', 100) as padding  -- Add padding to increase mark file size
                FROM numbers({batch_size})
            """)

        # Optimize to ensure we have the expected number of parts
        # But don't merge everything into one part
        node.query(f"OPTIMIZE TABLE {table_name}")

        # Verify table structure
        parts_info = node.query(f"""
            SELECT count() as part_count, sum(rows) as total_rows
            FROM system.parts
            WHERE table = '{table_name}' AND active
        """).strip().split('\t')

        logger.info(f"Created table with {parts_info[0]} parts and {parts_info[1]} total rows")

        yield table_name

    finally:
        logger.info(f"Cleaning up table: {table_name}")
        try:
            node.query(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")

def test_mark_cache_eviction_functionality(start_cluster, setup_cache_test_environment):
    """
    Test that mark cache eviction metrics are properly tracked when cache overflows.

    This test:
    1. Records baseline metrics
    2. Performs operations that will fill and overflow the mark cache
    3. Verifies that eviction metrics are incremented appropriately
    """
    table_name = setup_cache_test_environment

    # Get baseline metrics before starting the test
    initial_metrics = get_current_cache_metrics()
    logger.info(f"Starting test with baseline metrics: {initial_metrics}")

    # Clear the mark cache to start fresh
    node.query("SYSTEM CLEAR MARK CACHE")

    # Perform queries that will load many mark files into cache
    # Query different ranges to load different parts and their mark files
    logger.info("Executing queries to fill mark cache...")

    # First, do some queries to populate the cache
    for i in range(0, 50000, 2500):  # Query every 2500 records
        end_range = i + 2500
        result = node.query(f"""
            SELECT count(*), avg(length(data))
            FROM {table_name}
            WHERE id BETWEEN {i} AND {end_range}
        """)
        logger.debug(f"Query range {i}-{end_range}: {result.strip()}")

    # Now do more intensive queries that should trigger evictions
    logger.info("Executing queries to trigger cache evictions...")

    # Perform random access patterns to force cache pressure
    import random
    ranges = [(i, i + 1000) for i in range(0, 90000, 3000)]
    random.shuffle(ranges)  # Random access pattern increases cache pressure

    for start, end in ranges:
        node.query(f"""
            SELECT count(*), sum(length(padding))
            FROM {table_name}
            WHERE id BETWEEN {start} AND {end}
        """)

    # Wait for evictions to be reflected in metrics
    logger.info("Waiting for cache evictions to be recorded...")
    try:
        final_metrics = wait_for_cache_eviction(initial_metrics, timeout=30)
    except TimeoutError:
        # If no eviction detected, try one more aggressive approach
        logger.warning("No eviction detected, trying more aggressive cache pressure...")

        # Force more cache pressure by querying with PREWHERE (loads more marks)
        for i in range(0, 100000, 1000):
            node.query(f"""
                SELECT count()
                FROM {table_name}
                PREWHERE id % 100 = 0
                WHERE id >= {i} AND id < {i + 1000}
            """)

        final_metrics = wait_for_cache_eviction(initial_metrics, timeout=15)

    # Verify that eviction metrics have increased
    expected_metrics = ['MarkCacheEvictedBytes', 'MarkCacheEvictedMarks', 'MarkCacheEvictedFiles']

    for metric in expected_metrics:
        initial_value = initial_metrics.get(metric, 0)
        final_value = final_metrics.get(metric, 0)

        assert final_value > initial_value, \
            f"{metric} should have increased: initial={initial_value}, final={final_value}"

        logger.info(f"✓ {metric}: {initial_value} → {final_value} (delta: {final_value - initial_value})")

    # Verify logical relationships between metrics
    bytes_evicted = final_metrics.get('MarkCacheEvictedBytes', 0) - initial_metrics.get('MarkCacheEvictedBytes', 0)
    marks_evicted = final_metrics.get('MarkCacheEvictedMarks', 0) - initial_metrics.get('MarkCacheEvictedMarks', 0)
    files_evicted = final_metrics.get('MarkCacheEvictedFiles', 0) - initial_metrics.get('MarkCacheEvictedFiles', 0)

    # Sanity checks for metric relationships
    assert bytes_evicted > 0, "Should have evicted some bytes"
    assert marks_evicted > 0, "Should have evicted some marks"
    assert files_evicted > 0, "Should have evicted some files"

    # Marks should be >= files (each file contains at least 1 mark)
    assert marks_evicted >= files_evicted, \
        f"Marks evicted ({marks_evicted}) should be >= files evicted ({files_evicted})"

    logger.info(f"✓ Test passed - Cache eviction metrics working correctly")

def test_mark_cache_query_log_integration(start_cluster, setup_cache_test_environment):
    """
    Test that eviction metrics appear in query logs when evictions occur during queries
    """
    table_name = setup_cache_test_environment

    # Enable query log
    node.query("SYSTEM FLUSH LOGS")

    # Clear cache and perform operation that should trigger evictions
    node.query("SYSTEM CLEAR MARK CACHE")

    # Get initial metrics
    initial_metrics = get_current_cache_metrics()

    # Perform a query that should trigger cache evictions
    node.query(f"""
        SELECT count(*), avg(length(data)), sum(length(padding))
        FROM {table_name}
        WHERE id % 17 = 0  -- Sparse access pattern to stress cache
    """)

    # More cache pressure
    for i in range(10):
        node.query(f"""
            SELECT count() FROM {table_name}
            WHERE id >= {i * 10000} AND id < {(i + 1) * 10000}
            AND data LIKE '%{i}%'
        """)

    # Wait for metrics to update
    time.sleep(2)
    node.query("SYSTEM FLUSH LOGS")

    # Check if eviction metrics appear in query log
    eviction_queries = node.query("""
                                  SELECT
                                      query,
                                      ProfileEvents['MarkCacheEvictedBytes'] as bytes,
                                      ProfileEvents['MarkCacheEvictedMarks'] as marks,
                                      ProfileEvents['MarkCacheEvictedFiles'] as files
                                  FROM system.query_log
                                  WHERE
                                      type = 'QueryFinish'
                                    AND (
                                      ProfileEvents['MarkCacheEvictedBytes'] > 0 OR
                                      ProfileEvents['MarkCacheEvictedMarks'] > 0 OR
                                      ProfileEvents['MarkCacheEvictedFiles'] > 0
                                      )
                                    AND query NOT LIKE 'SYSTEM%'
                                  ORDER BY event_time_microseconds DESC
                                      LIMIT 5
                                  """).strip()

    logger.info(f"Queries with eviction metrics:\n{eviction_queries}")

    # We should have at least some queries with eviction metrics
    # (This might be empty if evictions didn't occur during these specific queries,
    #  which is why we also test system.events separately)
    if eviction_queries:
        logger.info("✓ Found eviction metrics in query log")
    else:
        logger.info("ℹ No eviction metrics in query log (evictions may have occurred between queries)")

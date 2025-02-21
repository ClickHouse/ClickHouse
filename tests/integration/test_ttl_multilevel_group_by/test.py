import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_minio=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_until(condition_fn, timeout=10, interval=1, fail_message="Condition not met in time"):
    """
    Repeatedly calls condition_fn until it returns True or timeout is reached.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_fn():
            return
        time.sleep(interval)
    assert False, fail_message


def test_two_stage_ttl_group_by(started_cluster):
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_ttl_group_by")

    # Create table with two TTL GROUP BY rules:
    #   1. After 3 seconds, aggregate by user_id, item_id using SUM
    #   2. After 7 seconds, aggregate by user_id using SUM
    node.query("""
    CREATE TABLE test_ttl_group_by (
        event_date DateTime,
        user_id Int32,
        item_id Int32,
        val Float32,
        rolled_by_item_id DateTime DEFAULT event_date,
        rolled_by_user_id DateTime DEFAULT event_date
    ) ENGINE = MergeTree()
        PRIMARY KEY (event_date, user_id, item_id)
        TTL rolled_by_item_id + INTERVAL 4 SECOND
            GROUP BY event_date, user_id, item_id
            SET val = sum(val),
                rolled_by_item_id = max(rolled_by_item_id) + INTERVAL 10 YEAR,
        rolled_by_user_id + INTERVAL 7 SECOND
            GROUP BY event_date, user_id
            SET val = sum(val),
                rolled_by_user_id = max(rolled_by_user_id) + INTERVAL 10 YEAR
        SETTINGS merge_with_ttl_timeout = 0;
    """)

    # Insert 10 rows with current timestamp, evenly distributed across 2 user_ids and 3 items
    node.query("""
        INSERT INTO test_ttl_group_by
        SELECT
            now() AS event_date,  -- Указываем фиксированное время
            number % 2 AS user_id,
            user_id * 10 + (number % 3) AS item_id,
            1 AS val,
            event_date AS rolled_by_item_id,
            event_date AS rolled_by_user_id
        FROM numbers(10);
    """)

    def get_row_count():
        return int(node.query("SELECT count() FROM test_ttl_group_by").strip())

    # Step 1: Ensure all 10 rows were inserted
    wait_until(lambda: get_row_count() == 10, timeout=3, fail_message="Initial insert failed")

    # Step 2: Wait for the first TTL (2 seconds) to apply — should reduce to 6 rows (one per item_id)
    wait_until(
        lambda: get_row_count() == 6,
        timeout=10,
        fail_message="First TTL was not applied"
    )

    row_count_after_first_ttl = get_row_count()
    assert row_count_after_first_ttl == 6, \
        f"Expected = 6 rows after first TTL, got: {row_count_after_first_ttl}"

    # Step 3: Wait for second TTL (7 seconds since insert) to apply — should reduce to 2 rows (one per user_id)
    wait_until(lambda: get_row_count() == 2, timeout=10, fail_message="Second TTL was not applied")

    row_count_after_second_ttl = get_row_count()
    assert row_count_after_second_ttl == 2, \
        f"Expected = 2 rows after second TTL, got: {row_count_after_second_ttl}"

    row_after_second_ttl = node.query("SELECT user_id, val FROM test_ttl_group_by ORDER BY user_id FORMAT TSV")

    rows = row_after_second_ttl.strip().splitlines()
    for line in rows:
        user_id_str, value_str = line.strip().split("\t")
        assert int(value_str) == 5, "Invalid value after TTL aggregation"
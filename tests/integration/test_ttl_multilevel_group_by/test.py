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

def test_two_stage_ttl_group_by(started_cluster):
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_ttl_group_by")

    # Create table with two TTL GROUP BY rules:
    #   1. After 13 seconds, aggregate by user_id, item_id using SUM
    #   2. After 18 seconds, aggregate by user_id using SUM
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
        TTL rolled_by_item_id + INTERVAL 13 SECOND
            GROUP BY event_date, user_id, item_id
            SET val = sum(val),
                rolled_by_item_id = max(rolled_by_item_id) + INTERVAL 10 YEAR,
        rolled_by_user_id + INTERVAL 18 SECOND
            GROUP BY event_date, user_id
            SET val = sum(val),
                rolled_by_user_id = max(rolled_by_user_id) + INTERVAL 10 YEAR
        SETTINGS merge_with_ttl_timeout = 0;
    """)

    # Insert 10 rows with current timestamp, evenly distributed across 2 user_ids and 3 items
    node.query("""
        INSERT INTO test_ttl_group_by
        SELECT
            now() AS event_date,
            number % 2 AS user_id,
            user_id * 10 + (number % 3) AS item_id,
            1 AS val,
            event_date AS rolled_by_item_id,
            event_date AS rolled_by_user_id
        FROM numbers(10);
    """)

    # Step 1: Ensure all 10 rows were inserted
    node.query_with_retry("SELECT count() FROM test_ttl_group_by", check_callback=lambda res: int(res.strip()) == 10,
                          sleep_time=1, retry_count=10)

    # Step 2: Wait for the first TTL (13 seconds) to apply — should reduce to 6 rows (one per item_id)
    node.query_with_retry("SELECT count() FROM test_ttl_group_by", check_callback=lambda res: int(res.strip()) == 6,
                          sleep_time=1, retry_count=20)

    # Step 3: Wait for second TTL (18 seconds since insert) to apply — should reduce to 2 rows (one per user_id)
    node.query_with_retry("SELECT count() FROM test_ttl_group_by", check_callback=lambda res: int(res.strip()) == 2,
                          sleep_time=1, retry_count=10)

    row_after_second_ttl = node.query_with_retry("SELECT user_id, val FROM test_ttl_group_by ORDER BY user_id FORMAT TSV")

    rows = row_after_second_ttl.strip().splitlines()
    for line in rows:
        user_id_str, value_str = line.strip().split("\t")
        assert int(value_str) == 5, "Invalid value after TTL aggregation"
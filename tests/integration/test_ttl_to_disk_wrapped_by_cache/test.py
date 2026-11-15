import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/storage_configuration.xml"],
    with_minio=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Regression test for https://github.com/ClickHouse/ClickHouse/issues/98665
def test_ttl_to_wrapped_by_cache_disk(started_cluster):
    try:
        node.query(
            """
            CREATE TABLE test_ttl_to_wrapped_by_cache_disk
            (
                `id` UInt64,
                `event_time` DateTime
            )
            ENGINE = MergeTree
            PARTITION BY id
            ORDER BY id
            TTL event_time + toIntervalDay(14) TO DISK 's3'
            SETTINGS storage_policy = 's3_cold'
            """
        )

        node.query("SYSTEM STOP MOVES test_ttl_to_wrapped_by_cache_disk")

        node.query("INSERT INTO test_ttl_to_wrapped_by_cache_disk VALUES (1, now() - INTERVAL 100 DAY)")
        assert node.query("SELECT count() FROM test_ttl_to_wrapped_by_cache_disk").strip() == "1"

        # Part should start on default disk.
        assert node.query(
            "SELECT disk_name FROM system.parts "
            "WHERE table = 'test_ttl_to_wrapped_by_cache_disk' AND active"
        ).strip() == "default"

        node.query("SYSTEM START MOVES test_ttl_to_wrapped_by_cache_disk")

        # Wait for TTL move to the cache disk.
        for i in range(100):
            print(f"Waiting until part will be moved: iteration: {i}")

            disk = node.query(
                "SELECT disk_name FROM system.parts "
                "WHERE table = 'test_ttl_to_wrapped_by_cache_disk' AND active"
            ).strip()

            if disk == "s3_cache":
                break

            time.sleep(1)
        else:
            assert False, "TTL move did not happen within timeout"

        # Data should still be intact after the move.
        assert node.query("SELECT count() FROM test_ttl_to_wrapped_by_cache_disk").strip() == "1"

    finally:
        node.query("DROP TABLE test_ttl_to_wrapped_by_cache_disk SYNC")

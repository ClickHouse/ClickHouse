import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_mv,
    create_table,
    generate_random_files,
    generate_random_string,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("enable_hash_ring_filtering", [0, 1])
def test_retry_file_released_by_another_processor(
    started_cluster, enable_hash_ring_filtering
):
    """
    A file whose `processing` node is held by another processor must stay retriable:
    that node is not final, its owner can release the file without committing it.

    Both claim paths consult the file status cache: the plain one for a single file
    (`trySetProcessing`) and the batched one of the hash ring mode
    (`prepareSetProcessingRequests`), so both are checked.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_processing_state_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 5
    ttl_seconds = 3

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "processing_state_cache_ttl_seconds": ttl_seconds,
            "enable_hash_ring_filtering": enable_hash_ring_filtering,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 0,
        },
    )

    # Emulate another processor holding one of the files: create its `processing` node
    # before streaming starts. The node name is the SipHash64 of the file path, which is
    # what `ObjectStorageQueueIFileMetadata::getNodeName` uses.
    held_file = f"{files_path}/test_1.csv"
    held_node = node.query(f"SELECT sipHash64('{held_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.create(f"{keeper_path}/processing/{held_node}", b"another_processor")

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    try:
        create_mv(node, table_name, dst_table_name)

        # Every file except the held one is processed.
        for _ in range(60):
            if get_count() == files_to_generate - 1:
                break
            time.sleep(1)
        assert get_count() == files_to_generate - 1

        # The held file is not processed while its `processing` node exists,
        # even after the cached state expires and keeper is rechecked.
        time.sleep(3 * ttl_seconds)
        assert get_count() == files_to_generate - 1
        # Its `Processing` status is marked as belonging to the other processor,
        # so that the per-attempt columns of this table are not read as its own progress.
        assert (
            node.query(
                f"SELECT status, processing_observed_in_keeper_time IS NOT NULL"
                f" FROM system.s3queue_metadata_cache"
                f" WHERE zookeeper_path = '{keeper_path}' AND file_path = '{held_file}'"
            ).strip()
            == "Processing\t1"
        )
        # A file processed by this table is not marked as such.
        assert (
            node.query(
                f"SELECT count() FROM system.s3queue_metadata_cache"
                f" WHERE zookeeper_path = '{keeper_path}' AND file_path != '{held_file}'"
                f" AND processing_observed_in_keeper_time IS NOT NULL"
            ).strip()
            == "0"
        )

        # The other processor released the file without committing it.
        zk.delete(f"{keeper_path}/processing/{held_node}")

        # It must be retried, and it must happen within the cache TTL, not on a restart.
        deadline = time.monotonic() + 10 * ttl_seconds
        while time.monotonic() < deadline and get_count() != files_to_generate:
            time.sleep(0.5)
        assert get_count() == files_to_generate
        assert files_to_generate == int(
            node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}")
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {dst_table_name}")
        node.query(f"DROP TABLE IF EXISTS {table_name}")


def test_alter_processing_state_cache_ttl(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_alter_processing_state_ttl_{generate_random_string()}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={"processing_state_cache_ttl_seconds": 42},
    )

    def get_setting():
        return node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}'"
            f" AND name = 'processing_state_cache_ttl_seconds'"
        ).strip()

    try:
        assert get_setting() == "42"
        node.query(
            f"ALTER TABLE {table_name} MODIFY SETTING processing_state_cache_ttl_seconds = 0"
        )
        assert get_setting() == "0"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")

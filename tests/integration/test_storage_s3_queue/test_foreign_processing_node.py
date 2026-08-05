import json
import logging
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    create_table,
    create_mv,
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


def run_with_retry(check_result, func, retries=120):
    for _ in range(retries):
        last = func()
        if check_result(last):
            return last
        time.sleep(1)
    raise RuntimeError(f"{last} did not match expectations in {retries} retries")


def test_file_is_retried_after_foreign_processing_node_disappears(started_cluster):
    """A file whose `processing` node belongs to another processor must stay retryable.

    When `trySetProcessing` loses the race for the `processing` node, the local `FileStatus`
    is updated to `Processing`. Unlike `Processed` and `Failed`, that state is not backed by
    a persistent keeper node: the foreign processor can release the file without committing
    it (it can die, or fail the file and reset it). Treating the cached `Processing` state as
    terminal made this table skip the file until the file status was evicted from the cache
    or the server was restarted.

    The foreign processor is emulated by a real `processing` node in keeper (there is no
    difference from the point of view of this table - the node just does not carry our own
    processor info), and its disappearance by removing that node.

    The cached observation of the foreign `processing` node is trusted for
    `s3queue_foreign_processing_node_cache_ttl_seconds` (so that the table does not probe keeper
    for the file on every polling pass), therefore the file is picked up within that timeout
    after the node disappears, not immediately.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_processing_node_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    files_to_generate = 5
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            # Do not trust the observation of the foreign `processing` node for long,
            # so that the file is retried quickly after the node disappears.
            "s3queue_foreign_processing_node_cache_ttl_seconds": 5,
        },
    )

    # The node name is the SipHash64 of the file path, which is what
    # `ObjectStorageQueueIFileMetadata::getNodeName` uses.
    conflict_file = f"{files_path}/test_2.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        # The configured TTL must be visible to operators.
        assert (
            node.query(
                f"SELECT value FROM system.s3_queue_settings "
                f"WHERE table = '{table_name}' AND name = 'foreign_processing_node_cache_ttl_seconds'"
            ).strip()
            == "5"
        )

        create_mv(node, table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        # Every file except the one held by the foreign processor is processed.
        run_with_retry(lambda x: x == files_to_generate - 1, get_count)

        # The file must not be committed while the foreign `processing` node is there.
        assert node.query(f"SELECT count() FROM {dst_table_name} WHERE _path LIKE '%test_2.csv'").strip() == "0"

        # The foreign processor released the file without committing it.
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        # It must now be picked up: the cached `Processing` state is only a hint.
        run_with_retry(lambda x: x == files_to_generate, get_count)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )


def test_cached_state_updated_when_foreign_processor_commits(started_cluster):
    """A file committed by another processor must not stay cached as `Processing`.

    The listing pre-filter (`filterProcessableFiles`) drops a file as soon as its
    `processed` (or `failed`) node appears in keeper. That discovery is written back
    into the local file status cache, so `system.s3queue_metadata_cache` reports the
    terminal state instead of keeping the stale foreign `Processing` observation
    until cache eviction.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_foreign_commit_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    files_to_generate = 3
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            # Always check keeper, so that the terminal node is discovered promptly.
            "s3queue_foreign_processing_node_cache_ttl_seconds": 0,
        },
    )

    conflict_file = f"{files_path}/test_1.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        create_mv(node, table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        run_with_retry(lambda x: x == files_to_generate - 1, get_count)

        def get_cached_status():
            return node.query(
                f"SELECT status FROM system.s3queue_metadata_cache "
                f"WHERE file_path LIKE '%{conflict_file}'"
            ).strip()

        # The cached state of the file is an observation of the foreign `processing` node.
        run_with_retry(lambda x: x == "Processing", get_cached_status)

        # The foreign processor commits the file.
        node_metadata = json.dumps(
            {
                "file_path": conflict_file,
                "last_processed_timestamp": int(time.time()),
                "last_exception": "",
                "retries": 0,
                "processor_id": "0",
            }
        )
        zk.create(f"{keeper_path}/processed/{conflict_node}", node_metadata.encode())
        zk.delete(f"{keeper_path}/processing/{conflict_node}")

        # The cached state follows keeper instead of staying `Processing`.
        run_with_retry(lambda x: x == "Processed", get_cached_status)

        # The file was never ingested by this table.
        assert get_count() == files_to_generate - 1
        assert node.query(f"SELECT count() FROM {dst_table_name} WHERE _path LIKE '%test_1.csv'").strip() == "0"
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {table_name};
        """
        )


def test_foreign_processing_node_cache_ttl_is_per_table(started_cluster):
    """`foreign_processing_node_cache_ttl_seconds` must be honored per table.

    `ObjectStorageQueueMetadataFactory` shares one `ObjectStorageQueueMetadata` between all
    tables with the same `keeper_path`, so a setting kept in that shared object would be
    silently fixed by the table which was created first. This setting belongs to the table:
    both introspection and the actual retry window must use the value from its own DDL.
    """
    node = started_cluster.instances["instance"]

    suffix = generate_random_string()
    first_table_name = f"test_foreign_ttl_first_{suffix}"
    second_table_name = f"test_foreign_ttl_second_{suffix}"
    dst_table_name = f"test_foreign_ttl_dst_{suffix}"
    keeper_path = f"/clickhouse/test_foreign_ttl_{suffix}"
    files_path = f"test_foreign_ttl_{suffix}_data"

    files_to_generate = 3
    generate_random_files(started_cluster, files_path, files_to_generate, start_ind=0, row_num=1)

    # The first table trusts an observation of a foreign `processing` node for an hour.
    create_table(
        started_cluster,
        node,
        first_table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_foreign_processing_node_cache_ttl_seconds": 3600,
        },
    )

    # The second table shares the keeper path, but always checks keeper.
    create_table(
        started_cluster,
        node,
        second_table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "s3queue_loading_retries": 100,
            "s3queue_foreign_processing_node_cache_ttl_seconds": 0,
        },
    )

    conflict_file = f"{files_path}/test_1.csv"
    conflict_node = node.query(f"SELECT sipHash64('{conflict_file}')").strip()
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.ensure_path(f"{keeper_path}/processing")
    zk.create(f"{keeper_path}/processing/{conflict_node}", b"another processor")

    try:
        # Each table reports the value from its own DDL, not the value of the first table.
        def get_setting(table):
            return node.query(
                f"SELECT value FROM system.s3_queue_settings "
                f"WHERE table = '{table}' AND name = 'foreign_processing_node_cache_ttl_seconds'"
            ).strip()

        assert get_setting(first_table_name) == "3600"
        assert get_setting(second_table_name) == "0"

        # Only the second table streams, so it is its own TTL which is in effect.
        create_mv(node, second_table_name, dst_table_name)

        def get_count():
            return int(node.query(f"SELECT count() FROM {dst_table_name}").strip())

        run_with_retry(lambda x: x == files_to_generate - 1, get_count)

        # With the TTL of the first table (an hour) the file would not be retried in time.
        zk.delete(f"{keeper_path}/processing/{conflict_node}")
        run_with_retry(lambda x: x == files_to_generate, get_count, retries=60)
    finally:
        node.query(
            f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {second_table_name};
        DROP TABLE IF EXISTS {first_table_name};
        """
        )

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

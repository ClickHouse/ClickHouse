import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    create_table,
    create_mv,
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


def test_stop_streaming_on_file_iterator_ttl(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_file_iterator_ttl_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 300

    # With TTL = 2 sec streaming must stop once the file iterator is 1 sec old.
    # Commit after every file makes processing slow enough for one
    # streamToViews execution to certainly last longer than that,
    # so streaming must stop and continue with a fresh iterator
    # (and re-acquired bucket locks) several times before all files are done.
    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "buckets": 3,
            "processing_threads_num": 3,
            "persistent_processing_node_ttl_seconds": 2,
            "max_processed_files_before_commit": 1,
            "polling_min_timeout_ms": 100,
        },
    )

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    for _ in range(150):
        if get_count() == files_to_generate:
            break
        time.sleep(1)
    assert get_count() == files_to_generate

    # Streaming must have been stopped by the iterator TTL at least once.
    assert node.contains_in_log("Stopping streaming to views: file iterator age")

    # Every file must be processed exactly once regardless of the stops.
    assert files_to_generate == int(
        node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}")
    )

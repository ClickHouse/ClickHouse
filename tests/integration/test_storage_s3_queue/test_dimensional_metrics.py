import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_mv,
    create_table,
    generate_random_files,
    generate_random_string,
    put_s3_file_content,
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
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
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


def get_dimensional_metric(node, metric, table_name, extra_where=""):
    where = f"labels['table'] = '{table_name}'"
    if extra_where:
        where += f" AND {extra_where}"
    result = node.query(
        f"SELECT sum(value) FROM system.dimensional_metrics "
        f"WHERE metric = '{metric}' AND {where}"
    ).strip()
    return float(result) if result else 0.0


def test_pipeline_lag_metrics(started_cluster):
    """Smoke test for the per-table pipeline-lag dimensional metrics:
    object_storage_queue_newest_seen_object_timestamp_seconds and
    object_storage_queue_newest_committed_object_timestamp_seconds.
    """
    node = started_cluster.instances["instance"]
    table_name = f"dim_metrics_lag_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"

    files_num = 5
    row_num = 3
    test_start_time = time.time()

    total_values = generate_random_files(
        started_cluster, files_path, files_num, row_num=row_num
    )

    create_table(started_cluster, node, table_name, "unordered", files_path)
    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    run_with_retry(lambda x: x == len(total_values), get_count)

    newest_seen = run_with_retry(
        lambda x: x > 0,
        lambda: get_dimensional_metric(
            node, "object_storage_queue_newest_seen_object_timestamp_seconds", table_name
        ),
    )
    newest_committed = run_with_retry(
        lambda x: x > 0,
        lambda: get_dimensional_metric(
            node,
            "object_storage_queue_newest_committed_object_timestamp_seconds",
            table_name,
        ),
    )

    # Both timestamps come from the objects' own last-modified time (not wall-clock "now"
    # at listing/commit time), so once everything generated before the test started has
    # drained, they must land on the exact same value: the last object's last-modified time.
    assert newest_seen == newest_committed
    # Sanity: the objects were just uploaded, so the watermark should be close to "now",
    # not some stale value left over from a previous test using a shared metadata object.
    assert abs(newest_seen - test_start_time) < 300


def test_failure_metrics(started_cluster):
    """Smoke test for the per-table failure dimensional metrics:
    object_storage_queue_failures_total and
    object_storage_queue_permanently_failed_files_total.
    """
    node = started_cluster.instances["instance"]
    table_name = f"dim_metrics_fail_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    file_path = f"{files_path}/bad_row.csv"
    retries_num = 2

    # column1 is UInt32, "not_a_number" fails to parse, deterministically failing every
    # attempt to read this file (as opposed to an insert- or commit-time failure).
    put_s3_file_content(started_cluster, file_path, b"not_a_number,1,1\n")

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "s3queue_loading_retries": retries_num,
            "polling_max_timeout_ms": 5000,
            "polling_backoff_ms": 1000,
        },
    )
    create_mv(node, table_name, dst_table_name)

    permanently_failed = run_with_retry(
        lambda x: x >= 1,
        lambda: get_dimensional_metric(
            node, "object_storage_queue_permanently_failed_files_total", table_name
        ),
    )
    assert permanently_failed == 1

    read_failures = get_dimensional_metric(
        node,
        "object_storage_queue_failures_total",
        table_name,
        extra_where="labels['stage'] = 'read'",
    )
    assert read_failures >= 1

    # The file never parsed successfully, so nothing should have reached the destination.
    assert 0 == int(node.query(f"SELECT count() FROM {dst_table_name}"))

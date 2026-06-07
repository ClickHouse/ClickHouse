"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on an S3Queue table."""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_table,
    create_mv,
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
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


# Each generated file has 10 rows.
ROWS_PER_FILE = 10


def setup_consuming_table(started_cluster, node, table):
    files_path = f"{table}_data"
    create_table(started_cluster, node, table, "unordered", files_path)
    create_mv(node, table, f"{table}_dst")
    return files_path


def wait_dst_count(node, table, expected):
    node.query_with_retry(
        f"SELECT count() FROM {table}_dst",
        check_callback=lambda res: int(res) == expected,
        retry_count=120,
        sleep_time=0.5,
    )


def assert_dst_count_stable(node, table, expected, seconds=10):
    """The streaming task is expected to be stopped, so the row count must not grow."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(node.query(f"SELECT count() FROM {table}_dst")) == expected
        time.sleep(1)


def test_system_stop_start_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_stop_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # STOP halts polling: files added meanwhile are not processed.
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    # START resumes polling and the new files are processed.
    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_pause_start_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_pause_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # PAUSE stops further activity: files added meanwhile are not processed.
    node.query(f"SYSTEM PAUSE {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_cancel_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_cancel_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # CANCEL interrupts only the current poll; polling keeps going afterwards.
    node.query(f"SYSTEM CANCEL {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_refresh_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_refresh_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # REFRESH kicks off a poll out of order; polling continues normally.
    node.query(f"SYSTEM REFRESH {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_stop_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_allbg_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    node.query("SYSTEM STOP ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    # START ALL BACKGROUND resumes every streaming table.
    node.query("SYSTEM START ALL BACKGROUND")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_pause_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_pauseall_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # PAUSE ALL BACKGROUND halts polling for every streaming table.
    node.query("SYSTEM PAUSE ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    node.query("SYSTEM START ALL BACKGROUND")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_cancel_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_cancelall_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # CANCEL ALL BACKGROUND interrupts the current poll but does not halt polling.
    node.query("SYSTEM CANCEL ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_refresh_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_refreshall_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # REFRESH ALL BACKGROUND kicks off a poll for every streaming table; polling continues.
    node.query("SYSTEM REFRESH ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_stop_requires_grant(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_grant_{generate_random_string()}"
    setup_consuming_table(started_cluster, node, table)
    user = f"user_{table}"
    node.query(f"DROP USER IF EXISTS {user}; CREATE USER {user}")

    # A user without the required SYSTEM privilege cannot control the table.
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        assert "ACCESS_DENIED" in node.query_and_get_error(
            f"SYSTEM {verb} {table}", user=user
        )

    # Once granted, every verb succeeds.
    node.query(f"GRANT SYSTEM ON *.* TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        node.query(f"SYSTEM {verb} {table}", user=user)

    node.query(f"DROP USER {user}")

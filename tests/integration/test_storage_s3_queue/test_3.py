import io
import json
import logging
import random
import string
import time
import uuid
from multiprocessing.dummy import Pool

import pytest
from kazoo.exceptions import NoNodeError

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_queue_common import (
    run_query,
    random_str,
    generate_random_files,
    put_s3_file_content,
    put_azure_file_content,
    create_table,
    create_mv,
    generate_random_string,
)

AVAILABLE_MODES = ["unordered", "ordered"]


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance_2 = started_cluster.instances["instance2"]

    instance.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")
    instance_2.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")

    minio = started_cluster.minio_client
    objects = list(minio.list_objects(started_cluster.minio_bucket, recursive=True))
    for obj in objects:
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)

    container_client = started_cluster.blob_service_client.get_container_client(
        started_cluster.azurite_container
    )

    if container_client.exists():
        blob_names = [b.name for b in container_client.list_blobs()]
        logging.debug(f"Deleting blobs: {blob_names}")
        for b in blob_names:
            container_client.delete_blob(b)

    yield  # run test


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance2",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance_23.12",
            with_zookeeper=True,
            image="clickhouse/clickhouse-server",
            tag="23.12",
            stay_alive=True,
            with_installed_binary=True,
            use_old_analyzer=True,
        )
        cluster.add_instance(
            "instance_too_many_parts",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/merge_tree.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance_24.5",
            with_zookeeper=True,
            image="clickhouse/clickhouse-server",
            tag="24.5",
            stay_alive=True,
            user_configs=[
                "configs/users.xml",
            ],
            with_installed_binary=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_settings_check(started_cluster):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    table_name = f"test_settings_check"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    mode = "ordered"

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 5,
            "s3queue_buckets": 2,
        },
    )

    assert (
        "Existing table metadata in ZooKeeper differs in buckets setting. Stored in ZooKeeper: 2, local: 3"
        in create_table(
            started_cluster,
            node_2,
            table_name,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_processing_threads_num": 5,
                "s3queue_buckets": 3,
            },
            expect_error=True,
        )
    )

    node.query(f"DROP TABLE {table_name} SYNC")


@pytest.mark.parametrize("processing_threads", [1, 5])
def test_processed_file_setting(started_cluster, processing_threads):
    node = started_cluster.instances["instance"]
    table_name = f"test_processed_file_setting_{processing_threads}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = (
        f"/clickhouse/test_{table_name}_{processing_threads}_{generate_random_string()}"
    )
    files_path = f"{table_name}_data"
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": processing_threads,
            "s3queue_last_processed_path": f"{files_path}/test_5.csv",
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    expected_rows = 4
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()

    node.restart_clickhouse()
    time.sleep(10)

    expected_rows = 4
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()


@pytest.mark.parametrize("processing_threads", [1, 5])
def test_processed_file_setting_distributed(started_cluster, processing_threads):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    table_name = f"test_processed_file_setting_distributed_{processing_threads}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = (
        f"/clickhouse/test_{table_name}_{processing_threads}_{generate_random_string()}"
    )
    files_path = f"{table_name}_data"
    files_to_generate = 10

    for instance in [node, node_2]:
        create_table(
            started_cluster,
            instance,
            table_name,
            "ordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_processing_threads_num": processing_threads,
                "s3queue_last_processed_path": f"{files_path}/test_5.csv",
                "s3queue_buckets": 2,
                "polling_max_timeout_ms": 2000,
                "polling_backoff_ms": 1000,
            },
        )

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    for instance in [node, node_2]:
        create_mv(instance, table_name, dst_table_name)

    def get_count():
        query = f"SELECT count() FROM {dst_table_name}"
        return int(node.query(query)) + int(node_2.query(query))

    expected_rows = 4
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()

    for instance in [node, node_2]:
        instance.restart_clickhouse()

    time.sleep(10)
    expected_rows = 4
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()


def test_upgrade(started_cluster):
    node = started_cluster.instances["instance_23.12"]
    if "23.12" not in node.query("select version()").strip():
        node.restart_with_original_version()

    table_name = f"test_upgrade"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        version="23.12",
        additional_settings={
            "keeper_path": keeper_path,
            "after_processing": "keep",
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    expected_rows = 10
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()

    node.restart_with_latest_version()

    assert expected_rows == get_count()


def test_exception_during_insert(started_cluster):
    node = started_cluster.instances["instance_too_many_parts"]

    # A unique table name is necessary for repeatable tests
    table_name = f"test_exception_during_insert_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 100,
            "polling_backoff_ms": 0,
        },
    )
    node.rotate_logs()

    node.query("system stop merges")
    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    def wait_for_rows(expected_rows):
        for _ in range(20):
            if expected_rows == get_count():
                break
            time.sleep(1)
        assert expected_rows == get_count()

    expected_rows = [0]

    def generate(check_inserted):
        files_to_generate = 1
        row_num = 1
        time.sleep(10)
        total_values = generate_random_files(
            started_cluster,
            files_path,
            files_to_generate,
            start_ind=0,
            row_num=row_num,
            use_random_names=1,
        )
        expected_rows[0] += files_to_generate * row_num
        if check_inserted:
            wait_for_rows(expected_rows[0])

    generate(True)
    generate(True)
    generate(True)
    generate(False)

    node.wait_for_log_line(
        "Merges are processing significantly slower than inserts: while pushing to view default.test_exception_during_insert_"
    )

    time.sleep(2)
    exception = node.query(
        f"SELECT exception FROM system.s3queue WHERE zookeeper_path ilike '%{table_name}%' and notEmpty(exception)"
    )
    assert "Too many parts" in exception

    node.query("system start merges")
    node.query(f"optimize table {dst_table_name} final")

    wait_for_rows(expected_rows[0])


@pytest.mark.parametrize("processing_threads", [1, 16])
def test_commit_on_limit(started_cluster, processing_threads):
    node = started_cluster.instances["instance"]

    # A unique table name is necessary for repeatable tests
    table_name = f"test_commit_on_limit_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    dst_table_name = f"{table_name}_dst"
    files_to_generate = 40

    failed_files_event_before = int(
        node.query(
            "SELECT value FROM system.events WHERE name = 'ObjectStorageQueueFailedFiles' SETTINGS system_events_show_zero_values=1"
        )
    )
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": processing_threads,
            "s3queue_loading_retries": 0,
            "s3queue_max_processed_files_before_commit": 10,
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    incorrect_values = [
        ["failed", 1, 1],
    ]
    incorrect_values_csv = (
        "\n".join((",".join(map(str, row)) for row in incorrect_values)) + "\n"
    ).encode()

    correct_values = [
        [1, 1, 1],
    ]
    correct_values_csv = (
        "\n".join((",".join(map(str, row)) for row in correct_values)) + "\n"
    ).encode()

    put_s3_file_content(
        started_cluster, f"{files_path}/test_99.csv", correct_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_999.csv", correct_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_9999.csv", incorrect_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_99999.csv", correct_values_csv
    )
    put_s3_file_content(
        started_cluster, f"{files_path}/test_999999.csv", correct_values_csv
    )

    create_mv(node, table_name, dst_table_name)

    expected_files = files_to_generate + 4
    for _ in range(100):
        if expected_files == int(node.query(f"select count() from {dst_table_name}")):
            break
        time.sleep(1)
    assert expected_files == int(node.query(f"select count() from {dst_table_name}"))

    def get_processed_files():
        return (
            node.query(
                f"SELECT file_name FROM system.s3queue WHERE zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 "
            )
            .strip()
            .split("\n")
        )

    def get_failed_files():
        return (
            node.query(
                f"SELECT file_name FROM system.s3queue WHERE zookeeper_path ilike '%{table_name}%' and status = 'Failed'"
            )
            .strip()
            .split("\n")
        )

    for _ in range(30):
        if "test_999999.csv" in get_processed_files():
            break
        time.sleep(1)

    assert "test_999999.csv" in get_processed_files()

    assert 1 == int(
        node.count_in_log(f"Setting file {files_path}/test_9999.csv as failed")
    )
    assert 1 == int(
        node.count_in_log(
            f"File {files_path}/test_9999.csv failed to process and will not be retried"
        )
    )

    assert failed_files_event_before + 1 == int(
        node.query(
            "SELECT value FROM system.events WHERE name = 'ObjectStorageQueueFailedFiles' SETTINGS system_events_show_zero_values=1"
        )
    )

    expected_processed = ["test_" + str(i) + ".csv" for i in range(files_to_generate)]
    processed = get_processed_files()
    for value in expected_processed:
        assert value in processed

    expected_failed = ["test_9999.csv"]
    failed = get_failed_files()
    for value in expected_failed:
        assert value not in processed
        assert value in failed

    node.query("system flush logs")
    count = node.query(
        f"SELECT count() FROM system.text_log WHERE message ILIKE '%successful files: 10)%' and logger_name ILIKE '%{table_name}%'"
    )
    count_2 = node.query(
        f"SELECT count() FROM system.text_log WHERE message ILIKE '%successful files: 4)%' and logger_name ILIKE '%{table_name}%'"
    )
    assert int(count) + int(count_2) == int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE message ILIKE '%successful files: %' and logger_name ILIKE '%{table_name}%'"
        )
    )


def test_upgrade_2(started_cluster):
    node = started_cluster.instances["instance_24.5"]
    if "24.5" not in node.query("select version()").strip():
        node.restart_with_original_version()
    assert "24.5" in node.query("select version()").strip()

    table_name = f"test_upgrade_2_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        version="24.5",
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_current_shard_num": 0,
            "s3queue_processing_threads_num": 2,
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    expected_rows = 10
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()

    # Parallel ordered mode used before 24.6 is not supported.
    # Users must do ALTER TABLE MODIFY SETTING buckets=N.
    node.query(f"DROP TABLE {table_name}_mv")

    node.restart_with_latest_version()
    assert table_name in node.query("SHOW TABLES")

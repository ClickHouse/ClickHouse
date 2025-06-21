import logging
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    put_s3_file_content,
    put_azure_file_content,
    create_table,
    create_mv,
    generate_random_string,
)


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")

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


def put_file_content(started_cluster, engine_name, file_path, values_csv):
    if engine_name == "S3Queue":
        put_s3_file_content(started_cluster, file_path, values_csv)
    else:
        put_azure_file_content(started_cluster, file_path, values_csv)


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
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("processing_threads_num", [1, 16])
@pytest.mark.parametrize("buckets", [1, 4])
@pytest.mark.parametrize("engine_name", ["S3Queue",
                                         "AzureQueue",
                                         ])
def test_ordered_mode_with_hive(started_cluster, engine_name, processing_threads_num, buckets):
    node = started_cluster.instances["instance"]
    table_name = (
        f"test_prefix_mode_{engine_name}_{generate_random_string()}"
    )
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    if engine_name == "S3Queue":
        storage = "s3"
    else:
        storage = "azure"

    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file1.csv", b"1,1,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file3.csv", b"1,1,3\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file1.csv", b"1,3,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file3.csv", b"1,3,3\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file1.csv", b"3,1,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file3.csv", b"3,1,3\n")

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "s3queue_loading_retries": 3,
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 5000,
            "polling_backoff_ms": 1000,
            "processing_threads_num": processing_threads_num,
            "buckets": buckets,
        },
        engine_name=engine_name,
        hive_partitioning="date=*/city=*/",
    )
    create_mv(node, table_name, dst_table_name, virtual_columns="date Date, city String")

    def compare_data(data, expected_data, buckets):
        data = data.strip().split("\n")
        if buckets == 1:
            assert data == expected_data, f"Expected: {expected_data}, got: {data}"
        else:
            for expected_line in expected_data:
                assert expected_line in data, f"Expected: {expected_data} as subset, got: {data}"

    expected_count = 6
    for _ in range(100):
        count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
        print(f"{count}/{expected_count}")
        if count >= expected_count:
            break
        time.sleep(1)

    data = node.query(f"SELECT column1, column2, column3, date, city FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV").strip()
    expected_data = [
        '1,1,1,"2025-01-01","Amsterdam"',
        '1,1,3,"2025-01-01","Amsterdam"',
        '1,3,1,"2025-01-01","Copenhagen"', 
        '1,3,3,"2025-01-01","Copenhagen"',
        '3,1,1,"2025-01-03","Amsterdam"',
        '3,1,3,"2025-01-03","Amsterdam"',
    ]
    compare_data(data, expected_data, buckets)

    # Add new files to same partitions
    # One in the middle and one in the end
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file2.csv", b"1,1,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file4.csv", b"1,1,4\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file2.csv", b"1,3,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file4.csv", b"1,3,4\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file2.csv", b"3,1,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file4.csv", b"3,1,4\n")

    # only new files on the end (file4.csv) should be visible
    expected_count = 9
    for _ in range(100):
        count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
        print(f"{count}/{expected_count}")
        if count >= expected_count:
            break
        time.sleep(1)

    # With buckets we can get some files from the middle, if those files are last in bucket, but not global last.
    # It depends of hashes of file paths.
    # This sleep is for additional time for processing.
    if buckets > 1:
        time.sleep(10)

    data = node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV").strip()
    expected_data = [
        "1,1,1",
        "1,1,3",
        "1,1,4",
        "1,3,1",
        "1,3,3",
        "1,3,4",
        "3,1,1",
        "3,1,3",
        "3,1,4",
    ]
    compare_data(data, expected_data, buckets)

    # Add new city and new date
    # All should be visible
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file2.csv", b"1,2,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file4.csv", b"1,2,4\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-02/city=Amsterdam/file2.csv", b"2,1,2\n")

    expected_count = 12
    for _ in range(100):
        count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
        print(f"{count}/{expected_count}")
        if count >= expected_count:
            break
        time.sleep(1)

    if buckets > 1:
        time.sleep(10)

    data = node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV").strip()
    expected_data = [
        "1,1,1",
        "1,1,3",
        "1,1,4",
        "1,2,2",
        "1,2,4",
        "1,3,1",
        "1,3,3",
        "1,3,4",
        "2,1,2",
        "3,1,1",
        "3,1,3",
        "3,1,4",
    ]
    compare_data(data, expected_data, buckets)

    node.restart_clickhouse()
    data = node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV").strip()
    compare_data(data, expected_data, buckets)

    # Add some records
    # Only few should be visible
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-02/city=Amsterdam/file1.csv", b"2,1,1\n") # Skipped
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-02/city=Amsterdam/file3.csv", b"2,1,3\n") # Added
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file1.csv", b"1,2,1\n") # Skipped
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file3.csv", b"1,2,3\n") # Skipped
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file5.csv", b"1,2,5\n") # Added

    expected_count = 14
    for _ in range(100):
        count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
        print(f"{count}/{expected_count}")
        if count >= expected_count:
            break
        time.sleep(1)

    if buckets > 1:
        time.sleep(10)

    data = node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV").strip()
    expected_data = [
        "1,1,1",
        "1,1,3",
        "1,1,4",
        "1,2,2",
        "1,2,4",
        "1,2,5",
        "1,3,1",
        "1,3,3",
        "1,3,4",
        "2,1,2",
        "2,1,3",
        "3,1,1",
        "3,1,3",
        "3,1,4",
    ]
    compare_data(data, expected_data, buckets)

    zk = started_cluster.get_kazoo_client("zoo1")
    processed_nodes = []
    if (buckets == 1):
        processed_nodes = zk.get_children(f"{keeper_path}/processed")
    else:
        for i in range(buckets):
            bucket_nodes = zk.get_children(f"{keeper_path}/buckets/{i}/processed")
            for node in bucket_nodes:
                if node not in processed_nodes:
                    processed_nodes.append(node)
    processed_nodes.sort()
    expected_nodes = [
        "date=2025-01-01_city=Amsterdam",
        "date=2025-01-01_city=Berlin",
        "date=2025-01-01_city=Copenhagen",
        "date=2025-01-02_city=Amsterdam",
        "date=2025-01-03_city=Amsterdam",
    ]
    assert processed_nodes == expected_nodes

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
            user_configs=[
                "configs/users.xml",
                "configs/enable_keeper_fault_injection.xml"
            ],
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
            user_configs=[
                "configs/users.xml",
                "configs/enable_keeper_fault_injection.xml"
            ],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("hosts", [1, 2])
@pytest.mark.parametrize("processing_threads_num", [1, 16])
@pytest.mark.parametrize("buckets", [1, 4])
@pytest.mark.parametrize("engine_name", ["S3Queue",
                                         "AzureQueue",
                                         ])
def test_ordered_mode_with_hive(started_cluster, engine_name, processing_threads_num, buckets, hosts):
    instances = [started_cluster.instances["instance"]]
    if hosts == 2:
        instances.append(started_cluster.instances["instance2"])

    table_name = (
        f"test_prefix_mode_{engine_name}_{generate_random_string()}"
    )
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"

    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file1.csv", b"1,1,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file3.csv", b"1,1,3\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file1.csv", b"1,3,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file3.csv", b"1,3,3\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file1.csv", b"3,1,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file3.csv", b"3,1,3\n")

    for node in instances:
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
            hive_partitioning_path="date=*/city=*/",
            hive_partitioning_columns="date Date, city String",
        )
        create_mv(node, table_name, dst_table_name, virtual_columns="date Date, city String")
        time.sleep(5)

    def compare_data(data, expected_data, buckets):
        data = data.strip().split("\n")
        data.sort()
        if buckets == 1:
            assert data == expected_data, f"Expected: {expected_data}, got: {data}"
        else:
            for expected_line in expected_data:
                assert expected_line in data, f"Expected: {expected_data} as subset, got: {data}"

    def wait_for_data(dst_table_name, expected_count):
        for i in range(10):
            count = 0
            for node in instances:
                count += int(node.query(f"SELECT count() FROM {dst_table_name}"))
            print(f"{count}/{expected_count}")
            if count >= expected_count:
                break
            time.sleep(1)

    expected_data = [
        '1,1,1,"2025-01-01","Amsterdam"',
        '1,1,3,"2025-01-01","Amsterdam"',
        '1,3,1,"2025-01-01","Copenhagen"', 
        '1,3,3,"2025-01-01","Copenhagen"',
        '3,1,1,"2025-01-03","Amsterdam"',
        '3,1,3,"2025-01-03","Amsterdam"',
    ]
    wait_for_data(dst_table_name, len(expected_data))

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3, date, city FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data, buckets)

    # Add new files to same partitions
    # One in the middle and one in the end
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file2.csv", b"1,1,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Amsterdam/file4.csv", b"1,1,4\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file2.csv", b"1,3,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Copenhagen/file4.csv", b"1,3,4\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file2.csv", b"3,1,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-03/city=Amsterdam/file4.csv", b"3,1,4\n")

    # Only new files on the end (file4.csv) should be visible
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
    wait_for_data(dst_table_name, len(expected_data))

    # With buckets we can get some files from the middle, if those files are last in bucket, but not global last.
    # It depends of hashes of file paths.
    # This sleep is for additional time for processing.
    if buckets > 1:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data, buckets)

    # Add new city and new date
    # All should be visible
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file2.csv", b"1,2,2\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file4.csv", b"1,2,4\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-02/city=Amsterdam/file2.csv", b"2,1,2\n")

    expected_data += [
        "1,2,2",
        "1,2,4",
        "2,1,2",
    ]
    expected_data.sort()
    wait_for_data(dst_table_name, len(expected_data))

    if buckets > 1:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data, buckets)

    instances[0].restart_clickhouse()

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data, buckets)

    # Add some records
    # Only few should be visible
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-02/city=Amsterdam/file1.csv", b"2,1,1\n") # Skipped
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-02/city=Amsterdam/file3.csv", b"2,1,3\n") # Added
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file1.csv", b"1,2,1\n") # Skipped
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file3.csv", b"1,2,3\n") # Skipped
    put_file_content(started_cluster, engine_name, f"{files_path}/date=2025-01-01/city=Berlin/file5.csv", b"1,2,5\n") # Added

    expected_data += [
        "1,2,5",
        "2,1,3",
    ]
    expected_data.sort()
    wait_for_data(dst_table_name, len(expected_data))

    if buckets > 1:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data, buckets)

    zk = started_cluster.get_kazoo_client("zoo1")
    processed_nodes = []
    if (buckets == 1):
        processed_nodes = zk.get_children(f"{keeper_path}/processed")
    else:
        for i in range(buckets):
            # Files are linked to buckets by hash of file path.
            # Path contains random table name, so distributing files to buckets is not predictable.
            # In rare case bucket can have zero processed files.
            if not zk.exists(f"{keeper_path}/buckets/{i}/processed"):
                continue
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

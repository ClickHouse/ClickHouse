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
from helpers.test_tools import wait_condition


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
@pytest.mark.parametrize("buckets", [0, 4])
@pytest.mark.parametrize("engine_name", ["S3Queue",
                                         "AzureQueue",
                                         ])
def test_ordered_mode_with_hive(started_cluster, engine_name, processing_threads_num, buckets, hosts):

    is_single_thread = (buckets==0 and processing_threads_num==1)
    if is_single_thread and hosts > 1:
        pytest.skip("Single thread with multiple hosts is not supported")

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

    # Wait for tables to be created and queryable on all instances
    # Check MV existence - if MV exists, then S3Queue and dst tables must also exist
    for node in instances:
        for _ in range(10):
            try:
                node.query(f"EXISTS TABLE {table_name}_mv")
                break
            except:
                time.sleep(0.5)

    def compare_data(data, expected_data):
        data = data.strip().split("\n")
        data.sort()
        if is_single_thread == 1:
            assert data == expected_data, f"Expected: {expected_data}, got: {data}"
        else:
            for expected_line in expected_data:
                assert data.count(expected_line) == 1, f"Expected exacly one element '{expected_line}', got: {data}"

    def wait_for_data(dst_table_name, expected_data, columns="column1, column2, column3"):
        for i in range(60):
            data = ""
            for node in instances:
                data += node.query(f"SELECT {columns} FROM {dst_table_name} ORDER BY {columns} FORMAT CSV")
            data_lines = data.strip().split("\n")
            missing = [row for row in expected_data if row not in data_lines]
            if not missing:
                break
            print(f"{len(expected_data) - len(missing)}/{len(expected_data)}")
            time.sleep(1)

    expected_data = [
        '1,1,1,"2025-01-01","Amsterdam"',
        '1,1,3,"2025-01-01","Amsterdam"',
        '1,3,1,"2025-01-01","Copenhagen"',
        '1,3,3,"2025-01-01","Copenhagen"',
        '3,1,1,"2025-01-03","Amsterdam"',
        '3,1,3,"2025-01-03","Amsterdam"',
    ]
    wait_for_data(dst_table_name, expected_data, "column1, column2, column3, date, city")

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3, date, city FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

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
    wait_for_data(dst_table_name, expected_data)

    # With buckets we can get some files from the middle, if those files are last in bucket, but not global last.
    # It depends of hashes of file paths.
    # This sleep is for additional time for processing.
    if not is_single_thread:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

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
    wait_for_data(dst_table_name, expected_data)
    if not is_single_thread:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    instances[0].restart_clickhouse()

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

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
    wait_for_data(dst_table_name, expected_data)

    if not is_single_thread:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    zk = started_cluster.get_kazoo_client("zoo1")
    processed_nodes = []
    if is_single_thread:
        processed_nodes = zk.get_children(f"{keeper_path}/processed")
    else:
        for i in range(buckets if buckets > 0 else processing_threads_num):
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


@pytest.mark.parametrize("hosts", [1, 2])
@pytest.mark.parametrize("processing_threads_num", [1, 16])
@pytest.mark.parametrize("buckets", [0, 4])
@pytest.mark.parametrize("engine_name", ["S3Queue", "AzureQueue"])
def test_ordered_mode_with_regex_partitioning(started_cluster, engine_name, processing_threads_num, buckets, hosts):
    """
    Test regex-based partitioning with hostname extraction from filenames.
    For example, filename format: {hostname}_{timestamp}_{sequence}.csv
    """
    is_single_thread = (buckets == 0 and processing_threads_num == 1)
    if is_single_thread and hosts > 1:
        pytest.skip("Single thread with multiple hosts is not supported")

    instances = [started_cluster.instances["instance"]]
    if hosts == 2:
        instances.append(started_cluster.instances["instance2"])

    table_name = f"test_regex_partition_{engine_name}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"

    # Regex patterns for hostname-based partitioning
    # Extract all named groups from filename, use 'hostname' as partition key
    partition_regex = r'(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)'
    partition_component = 'hostname'

    # Initial files - 6 files across 3 hostnames
    put_file_content(started_cluster, engine_name, f"{files_path}/server-1_20251217T100000.000000Z_0001.csv", b"1,1,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/server-1_20251217T100000.000000Z_0003.csv", b"1,1,3\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/server-2_20251217T100000.000000Z_0001.csv", b"1,3,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/server-2_20251217T100000.000000Z_0003.csv", b"1,3,3\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T100000.000000Z_0001.csv", b"3,1,1\n")
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T100000.000000Z_0003.csv", b"3,1,3\n")

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
            partitioning_mode="regex",
            partition_regex=partition_regex,
            partition_component=partition_component,
        )
        create_mv(node, table_name, dst_table_name)

    # Wait for tables to be created and queryable on all instances
    # Check MV existence - if MV exists, then S3Queue and dst tables must also exist
    for node in instances:
        for _ in range(10):
            try:
                node.query(f"EXISTS TABLE {table_name}_mv")
                break
            except:
                time.sleep(0.5)

    def compare_data(data, expected_data):
        data = data.strip().split("\n")
        data.sort()
        if is_single_thread:
            assert data == expected_data, f"Expected: {expected_data}, got: {data}"
        else:
            for expected_line in expected_data:
                assert data.count(expected_line) == 1, f"Expected exactly one element '{expected_line}', got: {data}"

    def wait_for_data(dst_table_name, expected_data, columns="column1, column2, column3"):
        for i in range(60):
            data = ""
            for node in instances:
                data += node.query(f"SELECT {columns} FROM {dst_table_name} ORDER BY {columns} FORMAT CSV")
            data_lines = data.strip().split("\n")
            missing = [row for row in expected_data if row not in data_lines]
            if not missing:
                break
            print(f"{len(expected_data) - len(missing)}/{len(expected_data)}")
            time.sleep(1)

    # Step 1: Verify initial 6 rows
    expected_data = [
        "1,1,1",
        "1,1,3",
        "1,3,1",
        "1,3,3",
        "3,1,1",
        "3,1,3",
    ]
    wait_for_data(dst_table_name, expected_data)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    # Step 2: Add files in middle and end of existing partitions
    # Middle files should be skipped (lexicographically before max processed)
    # End files should be processed
    put_file_content(started_cluster, engine_name, f"{files_path}/server-1_20251217T100000.000000Z_0002.csv", b"1,1,2\n")  # SKIPPED
    put_file_content(started_cluster, engine_name, f"{files_path}/server-1_20251217T100000.000000Z_0004.csv", b"1,1,4\n")  # PROCESSED
    put_file_content(started_cluster, engine_name, f"{files_path}/server-2_20251217T100000.000000Z_0002.csv", b"1,3,2\n")  # SKIPPED
    put_file_content(started_cluster, engine_name, f"{files_path}/server-2_20251217T100000.000000Z_0004.csv", b"1,3,4\n")  # PROCESSED
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T100000.000000Z_0002.csv", b"3,1,2\n")  # SKIPPED
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T100000.000000Z_0004.csv", b"3,1,4\n")  # PROCESSED

    # Only *_0004.csv files should be processed
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
    wait_for_data(dst_table_name, expected_data)

    if not is_single_thread:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    # Step 3: Add files for new hostname and newer timestamp
    put_file_content(started_cluster, engine_name, f"{files_path}/server-4_20251217T100000.000000Z_0002.csv", b"1,2,2\n")  # New partition
    put_file_content(started_cluster, engine_name, f"{files_path}/server-4_20251217T100000.000000Z_0004.csv", b"1,2,4\n")  # New partition
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T110000.000000Z_0002.csv", b"2,1,2\n")  # Newer timestamp

    expected_data += [
        "1,2,2",
        "1,2,4",
        "2,1,2",
    ]
    expected_data.sort()
    wait_for_data(dst_table_name, expected_data)

    if not is_single_thread:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    # Step 4: Restart ClickHouse node (test persistence)
    instances[0].restart_clickhouse()

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    # Step 5: Add mixed files (some before, some after max processed)
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T110000.000000Z_0001.csv", b"2,1,1\n")  # SKIPPED (before max)
    put_file_content(started_cluster, engine_name, f"{files_path}/server-3_20251217T110000.000000Z_0003.csv", b"2,1,3\n")  # PROCESSED (after max)
    put_file_content(started_cluster, engine_name, f"{files_path}/server-4_20251217T100000.000000Z_0001.csv", b"1,2,1\n")  # SKIPPED (before max)
    put_file_content(started_cluster, engine_name, f"{files_path}/server-4_20251217T100000.000000Z_0003.csv", b"1,2,3\n")  # SKIPPED (before max)
    put_file_content(started_cluster, engine_name, f"{files_path}/server-4_20251217T100000.000000Z_0005.csv", b"1,2,5\n")  # PROCESSED (after max)

    expected_data += [
        "1,2,5",
        "2,1,3",
    ]
    expected_data.sort()
    wait_for_data(dst_table_name, expected_data)

    if not is_single_thread:
        time.sleep(10)

    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3 FORMAT CSV")
    compare_data(data, expected_data)

    # Step 6: Verify ZooKeeper structure - check partition keys
    zk = started_cluster.get_kazoo_client("zoo1")
    processed_nodes = []
    if is_single_thread:
        processed_nodes = zk.get_children(f"{keeper_path}/processed")
    else:
        for i in range(buckets if buckets > 0 else processing_threads_num):
            # Files are linked to buckets by hash of file path.
            # In rare case bucket can have zero processed files.
            if not zk.exists(f"{keeper_path}/buckets/{i}/processed"):
                continue
            bucket_nodes = zk.get_children(f"{keeper_path}/buckets/{i}/processed")
            for node in bucket_nodes:
                if node not in processed_nodes:
                    processed_nodes.append(node)

    processed_nodes.sort()
    # Expected partition keys are hostnames
    expected_nodes = ["server-1", "server-2", "server-3", "server-4"]
    assert processed_nodes == expected_nodes, f"Expected nodes: {expected_nodes}, got: {processed_nodes}"


@pytest.mark.parametrize("hosts", [2])
@pytest.mark.parametrize("processing_threads_num", [16])
@pytest.mark.parametrize("buckets", [8])
@pytest.mark.parametrize("engine_name", ["S3Queue"])
def test_ordered_mode_with_regex_partitioning_large_num_files(started_cluster, engine_name, processing_threads_num, buckets, hosts):
    """
    Test regex-based partitioning with large number of files across multiple hosts.
    This test ensures concurrent processing works correctly at scale.
    """
    instances = [started_cluster.instances["instance"], started_cluster.instances["instance2"]]

    table_name = f"test_regex_large_{engine_name}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"

    # Regex patterns for hostname-based partitioning
    partition_regex = r'(?P<hostname>server-\d+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)'
    partition_component = 'hostname'

    # Generate large number of files (500) across multiple partitions (hostnames)
    num_hosts = 10
    files_per_host = 50
    total_files = num_hosts * files_per_host

    print(f"Creating {total_files} files across {num_hosts} partitions...")

    expected_sum = 0
    for host_id in range(1, num_hosts + 1):
        for seq in range(1, files_per_host + 1):
            # Generate unique data: host_id * 1000 + seq
            value = host_id * 1000 + seq
            expected_sum += value

            filename = f"{files_path}/server-{host_id}_20251217T100000.000000Z_{seq:04d}.csv"
            content = f"{value},{host_id},{seq}\n".encode()
            put_file_content(started_cluster, engine_name, filename, content)

    print(f"Files created. Expected sum: {expected_sum}")

    # Create tables on all instances
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
            partitioning_mode="regex",
            partition_regex=partition_regex,
            partition_component=partition_component,
        )
        create_mv(node, table_name, dst_table_name)

    # Wait for tables to be created
    for node in instances:
        for _ in range(10):
            try:
                node.query(f"EXISTS TABLE {table_name}_mv")
                break
            except:
                time.sleep(0.5)

    # Wait for all files to be processed (allow up to 120 seconds)
    print("Waiting for all files to be processed...")
    for i in range(120):
        total_count = 0
        for node in instances:
            total_count += int(node.query(f"SELECT count() FROM {dst_table_name}"))
        print(f"Progress: {total_count}/{total_files}")
        if total_count >= total_files:
            break
        time.sleep(1)

    # Verify all files were processed exactly once
    total_count = 0
    for node in instances:
        total_count += int(node.query(f"SELECT count() FROM {dst_table_name}"))

    assert total_count == total_files, f"Expected {total_files} rows, got {total_count}"

    # Verify data integrity - sum should match expected
    actual_sum = 0
    for node in instances:
        actual_sum += int(node.query(f"SELECT sum(column1) FROM {dst_table_name}"))

    assert actual_sum == expected_sum, f"Expected sum {expected_sum}, got {actual_sum}"

    # Verify all partitions were created in ZooKeeper.
    # Note: ZooKeeper metadata is committed after data insertion,
    # so we need to retry to allow the last batch's ZK commit to complete.
    zk = started_cluster.get_kazoo_client("zoo1")
    expected_partition_keys = sorted([f"server-{i}" for i in range(1, num_hosts + 1)])

    for attempt in range(30):
        processed_nodes = set()
        for i in range(buckets):
            if not zk.exists(f"{keeper_path}/buckets/{i}/processed"):
                continue
            bucket_nodes = zk.get_children(f"{keeper_path}/buckets/{i}/processed")
            processed_nodes.update(bucket_nodes)

        if sorted(processed_nodes) == expected_partition_keys:
            break
        time.sleep(1)

    assert sorted(processed_nodes) == expected_partition_keys, f"Expected {num_hosts} partitions, got {len(processed_nodes)}: {sorted(processed_nodes)}"


@pytest.mark.parametrize("bucketing_mode", ["path", "partition"])
@pytest.mark.parametrize("engine_name", ["S3Queue"])
def test_bucketing_mode_with_regex_partitioning(started_cluster, engine_name, bucketing_mode):
    """
    Test bucketing_mode setting with regex-based partitioning.

    - bucketing_mode='path': Files distributed across buckets based on hash of full path (default)
    - bucketing_mode='partition': Files from same partition always go to same bucket

    This test verifies that when bucketing_mode='partition':
    1. Validation: Fails without partitioning_mode, succeeds with partitioning_mode
    2. All files from the same hostname end up in the same bucket
    3. Files are only in specific buckets, not distributed across all buckets
    4. Each partition maps to exactly ONE bucket
    """
    instance = started_cluster.instances["instance"]
    instances = [instance]

    # Test validation: bucketing_mode='partition' requires partitioning_mode
    if bucketing_mode == "partition":
        validation_table = f"test_validation_{engine_name}_{generate_random_string()}"
        validation_path = f"{validation_table}_data"
        validation_keeper = f"/clickhouse/test_{validation_table}_{generate_random_string()}"

        # Should fail without partitioning_mode
        with pytest.raises(Exception) as exc_info:
            create_table(
                started_cluster,
                instance,
                validation_table,
                "ordered",
                validation_path,
                additional_settings={
                    "keeper_path": validation_keeper,
                    "processing_threads_num": 4,
                    "buckets": 4,
                    "bucketing_mode": "partition",
                    # partitioning_mode defaults to 'none'
                },
                engine_name=engine_name,
            )

        error_message = str(exc_info.value)
        assert "bucketing_mode='partition' requires partitioning_mode" in error_message, \
            f"Expected validation error about bucketing_mode, got: {error_message}"

    table_name = f"test_bucketing_{bucketing_mode}_{engine_name}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"

    # Regex patterns for hostname-based partitioning
    partition_regex = r'(?P<hostname>server-\d+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)'
    partition_component = 'hostname'

    processing_threads_num = 4
    buckets = 4

    # Create files across 3 hostnames (partitions)
    # With bucketing_mode='partition', all files from same hostname should go to same bucket
    # With bucketing_mode='path', files from same hostname may be distributed across buckets

    num_hostnames = 3
    files_per_hostname = 6

    print(f"Testing bucketing_mode='{bucketing_mode}' with {num_hostnames} partitions and {buckets} buckets")

    expected_data = []
    for host_id in range(1, num_hostnames + 1):
        for seq in range(1, files_per_hostname + 1):
            value = host_id * 100 + seq
            filename = f"{files_path}/server-{host_id}_20251217T100000.000000Z_{seq:04d}.csv"
            content = f"{value},{host_id},{seq}\n".encode()
            put_file_content(started_cluster, engine_name, filename, content)
            expected_data.append(f"{value},{host_id},{seq}")

    # Create table with specified bucketing_mode
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
                "bucketing_mode": bucketing_mode,
            },
            engine_name=engine_name,
            partitioning_mode="regex",
            partition_regex=partition_regex,
            partition_component=partition_component,
        )
        create_mv(node, table_name, dst_table_name)

    # Wait for tables to be created
    for node in instances:
        for _ in range(10):
            try:
                node.query(f"EXISTS TABLE {table_name}_mv")
                break
            except:
                time.sleep(0.5)

    # Wait for all files to be processed
    def wait_for_all_data(expected_count):
        for i in range(30):
            count = 0
            for node in instances:
                count += int(node.query(f"SELECT count() FROM {dst_table_name}"))
            print(f"Progress: {count}/{expected_count}")
            if count >= expected_count:
                break
            time.sleep(1)

    expected_count = num_hostnames * files_per_hostname
    wait_for_all_data(expected_count)

    # Verify all files were processed
    total_count = 0
    for node in instances:
        total_count += int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert total_count == expected_count, f"Expected {expected_count} rows, got {total_count}"

    # Verify data correctness
    data = ""
    for node in instances:
        data += node.query(f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1 FORMAT CSV")

    data_lines = data.strip().split("\n")
    data_lines.sort()
    expected_data.sort()
    assert data_lines == expected_data, f"Data mismatch"

    # Check bucket distribution in ZooKeeper
    zk = started_cluster.get_kazoo_client("zoo1")

    # Wait for ZooKeeper nodes to be populated
    # There can be a delay between data being processed and ZK metadata being written
    def get_partition_to_buckets():
        partition_to_buckets = {}
        buckets_used = set()

        for bucket_id in range(buckets):
            bucket_path = f"{keeper_path}/buckets/{bucket_id}/processed"
            if not zk.exists(bucket_path):
                continue

            buckets_used.add(bucket_id)
            partition_keys = zk.get_children(bucket_path)
            for partition_key in partition_keys:
                if partition_key not in partition_to_buckets:
                    partition_to_buckets[partition_key] = []
                partition_to_buckets[partition_key].append(bucket_id)

        return partition_to_buckets, buckets_used

    # Wait for all partitions to appear in ZooKeeper
    wait_condition(
        get_partition_to_buckets,
        lambda result: len(result[0]) == num_hostnames,
        max_attempts=20,
        delay=0.5,
    )

    partition_to_buckets, buckets_used = get_partition_to_buckets()

    if bucketing_mode == "partition":
        # All 3 hostnames should be present as partition keys
        assert len(partition_to_buckets) == num_hostnames, \
            f"Expected {num_hostnames} partitions, got {len(partition_to_buckets)}"

        # With bucketing_mode='partition', each partition should map to exactly ONE bucket
        for partition_key, bucket_ids in partition_to_buckets.items():
            assert len(bucket_ids) == 1, \
                f"With bucketing_mode='partition', partition '{partition_key}' should be in exactly 1 bucket, " \
                f"but found in buckets: {bucket_ids}"

        # With 3 partitions and 4 buckets, we should use EXACTLY 3 buckets (one per partition)
        assert len(buckets_used) == num_hostnames, \
            f"Expected EXACTLY {num_hostnames} buckets to be used (one per partition), but {len(buckets_used)} were used: {buckets_used}"

import logging
import time
from datetime import datetime

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    put_s3_file_content,
    put_azure_file_content,
    create_table,
    create_mv,
    generate_random_string,
)

AVAILABLE_MODES = ["unordered", "ordered"]
AUXILIARY_ZOOKEEPER_NAME = "zookeeper2"


def wait_for_keeper_commit(node, where):
    # The keeper commit (writing the processed pointer and removing the
    # processing node) runs *after* the inserted rows become visible in the
    # destination table, so a row count is not enough to know keeper is settled.
    # The commit is atomic, so an empty `processing` folder means every file
    # that finished has also had its processed pointer written.
    query = (
        f"SELECT processing_nodes_count FROM system.s3_queue_metadata WHERE {where}"
    )
    for _ in range(60):
        if node.query(query).strip() == "0":
            return
        time.sleep(1)
    assert node.query(query).strip() == "0"


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
            user_configs=[
                "configs/users.xml",
                "configs/enable_keeper_fault_injection.xml",
                "configs/keeper_retries.xml",
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
                "configs/enable_keeper_fault_injection.xml",
                "configs/keeper_retries.xml",
            ],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
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


def test_settings_check(started_cluster):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    table_name = "test_settings_check"
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
    generate_random_files(
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

    generate_random_files(
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
    generate_random_files(
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

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                f"SELECT file_name FROM system.s3queue_metadata_cache WHERE zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 "
            )
            .strip()
            .split("\n")
        )

    def get_failed_files():
        return (
            node.query(
                f"SELECT file_name FROM system.s3queue_metadata_cache WHERE zookeeper_path ilike '%{table_name}%' and status = 'Failed'"
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

    finish_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    node.query("system flush logs")
    commit_id = node.query(
        f"SELECT commit_id FROM system.s3queue_log WHERE file_name = '{files_path}/test_999999.csv'"
    ).strip()
    assert len(commit_id) > 0
    commit_id_count = int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE commit_id = {commit_id}"
        ).strip()
    )
    assert files_to_generate + 5 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE transaction_start_time >= toDateTime('{start_time}') and transaction_start_time <= toDateTime('{finish_time}')"
        ).strip()
    )
    # 11 and not 10, because failed file is not accounted in
    # current_processed_files which is compared to max_processed_files.
    assert commit_id_count <= 11
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


# `S3Queue` and `AzureQueue` register separate system tables
# (`system.s3_queue_metadata` and `system.azure_queue_metadata`), so exercise
# both engines to cover both registrations.
@pytest.mark.parametrize("engine_name", ["S3Queue", "AzureQueue"])
def test_system_queue_metadata(started_cluster, engine_name):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 10
    if engine_name == "S3Queue":
        storage = "s3"
        system_table = "system.s3_queue_metadata"
    else:
        storage = "azure"
        system_table = "system.azure_queue_metadata"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,
        },
        engine_name=engine_name,
    )
    create_mv(node, table_name, dst_table_name)

    generate_random_files(
        started_cluster, files_path, files_to_generate, storage=storage, row_num=1
    )
    # A malformed file which must end up in the `failed` folder.
    incorrect_values_csv = b"not_a_number,1,1\n"
    if storage == "s3":
        put_s3_file_content(
            started_cluster, f"{files_path}/bad.csv", incorrect_values_csv
        )
    else:
        put_azure_file_content(
            started_cluster, f"{files_path}/bad.csv", incorrect_values_csv
        )

    for _ in range(60):
        if files_to_generate == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert files_to_generate == int(node.query(f"SELECT count() FROM {dst_table_name}"))

    def get_counts():
        return list(
            map(
                int,
                node.query(
                    f"""
                    SELECT processed_nodes_count, processing_nodes_count, failed_nodes_count
                    FROM {system_table}
                    WHERE zookeeper_path ilike '%{keeper_path}%'
                    """
                )
                .strip()
                .split("\t"),
            )
        )

    for _ in range(60):
        processed, processing, failed = get_counts()
        if processed == files_to_generate and failed == 1 and processing == 0:
            break
        time.sleep(1)

    assert processed == files_to_generate
    assert processing == 0
    assert failed == 1

    # The contents columns must hold exactly as many nodes as the counts.
    processed_len, processing_len, failed_len = list(
        map(
            int,
            node.query(
                f"""
                SELECT length(processed_nodes), length(processing_nodes), length(failed_nodes)
                FROM {system_table}
                WHERE zookeeper_path ilike '%{keeper_path}%'
                """
            )
            .strip()
            .split("\t"),
        )
    )
    assert processed_len == files_to_generate
    assert processing_len == 0
    assert failed_len == 1

    # The `failed` node content stores the metadata of the malformed file,
    # which includes its path.
    failed_value = node.query(
        f"""
        SELECT arrayJoin(mapValues(failed_nodes))
        FROM {system_table}
        WHERE zookeeper_path ilike '%{keeper_path}%'
        """
    )
    assert "bad.csv" in failed_value

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_ordered(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_ordered_{generate_random_string()}"
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
        additional_settings={
            "keeper_path": keeper_path,
            # A single processed pointer (no buckets) keeps the test deterministic.
            "s3queue_buckets": 1,
            "s3queue_processing_threads_num": 1,
        },
    )
    create_mv(node, table_name, dst_table_name)

    generate_random_files(started_cluster, files_path, files_to_generate, row_num=1)

    for _ in range(60):
        if files_to_generate == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert files_to_generate == int(node.query(f"SELECT count() FROM {dst_table_name}"))
    wait_for_keeper_commit(node, f"zookeeper_path ilike '%{keeper_path}%'")

    # In ordered mode there are no per-file processed nodes, so
    # processed_nodes_count is NULL and the last processed pointer is exposed
    # via processed_path.
    assert (
        "1"
        == node.query(
            f"""
            SELECT processed_nodes_count IS NULL
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        ).strip()
    )

    # processing/failed counts are still meaningful in ordered mode.
    processing, failed = list(
        map(
            int,
            node.query(
                f"""
                SELECT processing_nodes_count, failed_nodes_count
                FROM system.s3_queue_metadata
                WHERE zookeeper_path ilike '%{keeper_path}%'
                """
            )
            .strip()
            .split("\t"),
        )
    )
    assert processing == 0
    assert failed == 0

    # The single processed pointer holds the last processed file path.
    processed_path_len = int(
        node.query(
            f"""
            SELECT length(processed_path)
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        ).strip()
    )
    assert processed_path_len == 1

    processed_path_value = node.query(
        f"""
        SELECT arrayJoin(mapValues(processed_path))
        FROM system.s3_queue_metadata
        WHERE zookeeper_path ilike '%{keeper_path}%'
        """
    )
    assert files_path in processed_path_value

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_ordered_buckets(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_buckets_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 100
    buckets = 4

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_buckets": buckets,
            "s3queue_processing_threads_num": buckets,
        },
    )
    create_mv(node, table_name, dst_table_name)

    generate_random_files(started_cluster, files_path, files_to_generate, row_num=1)

    for _ in range(60):
        if files_to_generate == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert files_to_generate == int(node.query(f"SELECT count() FROM {dst_table_name}"))
    wait_for_keeper_commit(node, f"zookeeper_path ilike '%{keeper_path}%'")

    # processed_nodes_count is NULL in ordered mode.
    assert (
        "1"
        == node.query(
            f"""
            SELECT processed_nodes_count IS NULL
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        ).strip()
    )

    # With buckets, each bucket that processed a file keeps its own pointer,
    # so processed_path is keyed by `buckets/<n>/processed`.
    keys = (
        node.query(
            f"""
            SELECT arrayJoin(mapKeys(processed_path))
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            ORDER BY 1
            """
        )
        .strip()
        .split("\n")
    )
    assert 2 <= len(keys) <= buckets
    for key in keys:
        assert key.startswith("buckets/") and key.endswith("/processed"), key

    processed_path_values = node.query(
        f"""
        SELECT arrayJoin(mapValues(processed_path))
        FROM system.s3_queue_metadata
        WHERE zookeeper_path ilike '%{keeper_path}%'
        """
    )
    for value in processed_path_values.strip().split("\n"):
        assert files_path in value

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_ordered_partitioned(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_partitioned_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    partition_regex = r"(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)"
    partition_component = "hostname"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            # No buckets, so the pointers live directly under `processed/<partition>`.
            "s3queue_buckets": 1,
            "s3queue_processing_threads_num": 1,
        },
        partitioning_mode="regex",
        partition_regex=partition_regex,
        partition_component=partition_component,
    )
    create_mv(node, table_name, dst_table_name)

    hostnames = ["server-1", "server-2", "server-3"]
    for hostname in hostnames:
        put_s3_file_content(
            started_cluster,
            f"{files_path}/{hostname}_20251217T100000.000000Z_0001.csv",
            b"1,1,1\n",
        )

    for _ in range(60):
        if len(hostnames) == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert len(hostnames) == int(node.query(f"SELECT count() FROM {dst_table_name}"))
    wait_for_keeper_commit(node, f"zookeeper_path ilike '%{keeper_path}%'")

    # processed_nodes_count is NULL in ordered mode.
    assert (
        "1"
        == node.query(
            f"""
            SELECT processed_nodes_count IS NULL
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        ).strip()
    )

    # With partitioning there is the root `processed` pointer plus one
    # pointer per partition, keyed by `processed/<partition>`.
    keys = (
        node.query(
            f"""
            SELECT arrayJoin(mapKeys(processed_path))
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            ORDER BY 1
            """
        )
        .strip()
        .split("\n")
    )
    assert len(keys) == len(hostnames) + 1
    assert keys[0] == "processed"
    for key in keys[1:]:
        assert key.startswith("processed/"), key

    # Every pointer, including the root one parsed from `NodeMetadata`,
    # must hold a processed file path.
    values = (
        node.query(
            f"""
            SELECT arrayJoin(mapValues(processed_path))
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        )
        .strip()
        .split("\n")
    )
    assert len(values) == len(hostnames) + 1
    for value in values:
        assert files_path in value, value

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_ordered_partitioned_buckets(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_part_buckets_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    partition_regex = r"(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)"
    partition_component = "hostname"
    buckets = 4

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            # buckets > 1 together with partitioning exercises the combined
            # `buckets/<n>/processed/<partition>` layout in readProcessedPointers.
            "s3queue_buckets": buckets,
            "s3queue_processing_threads_num": buckets,
        },
        partitioning_mode="regex",
        partition_regex=partition_regex,
        partition_component=partition_component,
    )
    create_mv(node, table_name, dst_table_name)

    # Many distinct partitions (hostnames) so partition pointers land under
    # several buckets. A file is linked to a bucket by the hash of its path,
    # so the exact bucket distribution is not deterministic, but every
    # partition must end up under some `buckets/<n>/processed/<hostname>`.
    hostnames = [f"server-{i}" for i in range(1, 9)]
    for hostname in hostnames:
        put_s3_file_content(
            started_cluster,
            f"{files_path}/{hostname}_20251217T100000.000000Z_0001.csv",
            b"1,1,1\n",
        )

    for _ in range(60):
        if len(hostnames) == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert len(hostnames) == int(node.query(f"SELECT count() FROM {dst_table_name}"))
    wait_for_keeper_commit(node, f"zookeeper_path ilike '%{keeper_path}%'")

    # processed_nodes_count is NULL in ordered mode.
    assert (
        "1"
        == node.query(
            f"""
            SELECT processed_nodes_count IS NULL
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        ).strip()
    )

    # Fetch the `processed_path` map as (key, value) pairs so each pointer's
    # value can be checked against its key.
    pairs = (
        node.query(
            f"""
            SELECT e.1, e.2
            FROM (
                SELECT arrayJoin(processed_path) AS e
                FROM system.s3_queue_metadata
                WHERE zookeeper_path ilike '%{keeper_path}%'
            )
            ORDER BY 1
            """
        )
        .strip()
        .split("\n")
    )
    assert pairs and pairs != [""], "processed_path must not be empty"

    # Files are linked to buckets by the hash of their (randomized) path, so the
    # exact spread of partitions across buckets varies between runs. Assert only
    # the distribution-independent invariants.
    root_buckets = set()
    child_buckets = set()
    partitions_seen = set()
    for pair in pairs:
        key, _, value = pair.partition("\t")
        parts = key.split("/")
        assert parts[0] == "buckets" and parts[2] == "processed", key
        if len(parts) == 3:
            # A bucket root. It points at a real file once that bucket has
            # processed one; buckets that processed nothing keep the empty
            # startup pointer, so only check the value when it is set.
            root_buckets.add(parts[1])
            if value:
                assert files_path in value, (key, value)
        else:
            # A per-partition child, which always holds the last processed path.
            assert len(parts) == 4, key
            child_buckets.add(parts[1])
            partitions_seen.add(parts[3])
            assert files_path in value, (key, value)

    # The reader must return both shapes: every bucket that processed a file
    # exposes its root pointer as well as its per-partition children.
    assert child_buckets, "expected at least one bucket with partition children"
    assert child_buckets <= root_buckets, (child_buckets, root_buckets)
    assert root_buckets <= {str(i) for i in range(buckets)}, root_buckets
    # Every processed partition is exposed as a child pointer.
    assert partitions_seen == set(hostnames), (partitions_seen, hostnames)

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_ordered_skips_empty_bucket_roots(started_cluster):
    # All `buckets/<n>/processed` roots are created at table startup, but a
    # bucket that never processed a file keeps an empty pointer. With far more
    # buckets than files, most roots stay empty, so this deterministically
    # exercises the reader's skip of empty pointers: `processed_path` must expose
    # only the buckets that actually processed a file, never the empty ones.
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_empty_buckets_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    partition_regex = r"(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)"
    buckets = 8

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_buckets": buckets,
            "s3queue_processing_threads_num": buckets,
        },
        partitioning_mode="regex",
        partition_regex=partition_regex,
        partition_component="hostname",
    )
    create_mv(node, table_name, dst_table_name)

    # Far fewer files than buckets guarantees several empty bucket roots.
    hostnames = ["server-1", "server-2"]
    for hostname in hostnames:
        put_s3_file_content(
            started_cluster,
            f"{files_path}/{hostname}_20251217T100000.000000Z_0001.csv",
            b"1,1,1\n",
        )

    for _ in range(60):
        if len(hostnames) == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert len(hostnames) == int(node.query(f"SELECT count() FROM {dst_table_name}"))
    wait_for_keeper_commit(node, f"zookeeper_path ilike '%{keeper_path}%'")

    pairs = (
        node.query(
            f"""
            SELECT e.1, e.2
            FROM (
                SELECT arrayJoin(processed_path) AS e
                FROM system.s3_queue_metadata
                WHERE zookeeper_path ilike '%{keeper_path}%'
            )
            ORDER BY 1
            """
        )
        .strip()
        .split("\n")
    )
    assert pairs and pairs != [""], "processed_path must not be empty"

    root_buckets = set()
    child_buckets = set()
    partitions_seen = set()
    for pair in pairs:
        key, _, value = pair.partition("\t")
        parts = key.split("/")
        assert parts[0] == "buckets" and parts[2] == "processed", key
        # Only processed buckets are returned, so every value is a real path.
        assert files_path in value, (key, value)
        if len(parts) == 3:
            root_buckets.add(parts[1])
        else:
            assert len(parts) == 4, key
            child_buckets.add(parts[1])
            partitions_seen.add(parts[3])

    # Empty bucket roots must be skipped: the returned roots are exactly the
    # buckets that processed a file, and there are fewer of them than buckets.
    assert root_buckets == child_buckets, (root_buckets, child_buckets)
    assert len(root_buckets) < buckets, root_buckets
    assert partitions_seen == set(hostnames), (partitions_seen, hostnames)

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_ordered_partitioned_last_processed(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_part_last_{generate_random_string()}"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    partition_regex = r"(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)"
    last_processed_path = f"{files_path}/server-1_20251217T100000.000000Z_0001.csv"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_buckets": 1,
            "s3queue_processing_threads_num": 1,
            "s3queue_last_processed_path": last_processed_path,
        },
        partitioning_mode="regex",
        partition_regex=partition_regex,
        partition_component="hostname",
    )

    # Before any file is processed, the root `processed` node already holds
    # the `last_processed_path` pointer and no partition children exist, so
    # `processed_path` must expose the root pointer alone.
    processed_path = node.query(
        f"""
        SELECT processed_path
        FROM system.s3_queue_metadata
        WHERE zookeeper_path ilike '%{keeper_path}%'
        """
    ).strip()
    assert processed_path == f"{{'processed':'{last_processed_path}'}}"

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_auxiliary_keeper(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_aux_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_suffix = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    # The factory key for an auxiliary keeper is "<keeper>:<path>", but the keeper
    # reads themselves must use the raw path against the auxiliary keeper client.
    keeper_path = f"{AUXILIARY_ZOOKEEPER_NAME}:{keeper_suffix}"
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
            # A single processed pointer (no buckets) keeps the test deterministic.
            "s3queue_buckets": 1,
            "s3queue_processing_threads_num": 1,
        },
    )
    create_mv(node, table_name, dst_table_name)

    generate_random_files(started_cluster, files_path, files_to_generate, row_num=1)

    for _ in range(60):
        if files_to_generate == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert files_to_generate == int(node.query(f"SELECT count() FROM {dst_table_name}"))
    wait_for_keeper_commit(node, f"zookeeper_path ilike '%{keeper_suffix}%'")

    # The display column keeps the auxiliary-keeper-prefixed factory key.
    zookeeper_path = node.query(
        f"""
        SELECT zookeeper_path
        FROM system.s3_queue_metadata
        WHERE zookeeper_path ilike '%{keeper_suffix}%'
        """
    ).strip()
    assert zookeeper_path == keeper_path

    # The processed pointer must be read from the auxiliary keeper using the raw
    # path; without that, the read would target a nonexistent path and the
    # metadata would be empty.
    processed_path_value = node.query(
        f"""
        SELECT arrayJoin(mapValues(processed_path))
        FROM system.s3_queue_metadata
        WHERE zookeeper_path ilike '%{keeper_suffix}%'
        """
    )
    assert files_path in processed_path_value

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_reads_only_selected_columns(started_cluster):
    # The map columns are documented as "fetched only when this column is
    # selected". Verify that against the keeper request counters: selecting only
    # `zookeeper_path` issues no folder reads, selecting only the `*_count`
    # columns issues a cheap `exists` stat (no listing, no data reads), and
    # selecting a map column lists the folder's children.
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_reads_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 5

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )
    create_mv(node, table_name, dst_table_name)

    generate_random_files(started_cluster, files_path, files_to_generate, row_num=1)
    for _ in range(60):
        if files_to_generate == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert files_to_generate == int(node.query(f"SELECT count() FROM {dst_table_name}"))

    where = f"WHERE zookeeper_path ilike '%{keeper_path}%'"

    def keeper_ops(columns):
        # Run the query with a unique id and read the ZooKeeper request counters
        # it accumulated from query_log. `list` = getChildren (listing a folder),
        # `read` = data reads (single or batched get), `exists` = stat only.
        query_id = f"{table_name}_{generate_random_string()}"
        node.query(
            f"SELECT {columns} FROM system.s3_queue_metadata {where}",
            query_id=query_id,
        )
        node.query("SYSTEM FLUSH LOGS")
        row = (
            node.query(
                f"""
                SELECT
                    ProfileEvents['ZooKeeperList'],
                    ProfileEvents['ZooKeeperMultiRead'] + ProfileEvents['ZooKeeperGet'],
                    ProfileEvents['ZooKeeperExists']
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'QueryFinish'
                ORDER BY event_time_microseconds DESC
                LIMIT 1
                """
            )
            .strip()
            .split("\t")
        )
        return dict(zip(("list", "read", "exists"), map(int, row)))

    # Only `zookeeper_path` (served from the in-memory factory): no folder reads.
    ops = keeper_ops("zookeeper_path")
    assert ops["list"] == 0, ops
    assert ops["read"] == 0, ops
    assert ops["exists"] == 0, ops

    # Only the `*_count` columns: a cheap `exists` per folder, but no child
    # listing and no data reads.
    ops = keeper_ops("processed_nodes_count, processing_nodes_count, failed_nodes_count")
    assert ops["exists"] >= 1, ops
    assert ops["list"] == 0, ops
    assert ops["read"] == 0, ops

    # A map column must list the folder's children (and read their data).
    ops = keeper_ops("processed_nodes")
    assert ops["list"] >= 1, ops

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_filter_pushdown(started_cluster):
    # `zookeeper_path` filtering is pushed down before any keeper reads, so a
    # targeted query never probes (and cannot fail on) unrelated queues. A query
    # whose filter matches no registered path must therefore do no folder reads
    # at all - without pushdown it would still list every registered queue.
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_pushdown_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 5

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )
    create_mv(node, table_name, dst_table_name)

    generate_random_files(started_cluster, files_path, files_to_generate, row_num=1)
    for _ in range(60):
        if files_to_generate == int(
            node.query(f"SELECT count() FROM {dst_table_name}")
        ):
            break
        time.sleep(1)
    assert files_to_generate == int(node.query(f"SELECT count() FROM {dst_table_name}"))

    # The exact factory key for this table, to filter on directly.
    zk_path = node.query(
        f"""
        SELECT zookeeper_path
        FROM system.s3_queue_metadata
        WHERE zookeeper_path ilike '%{keeper_path}%'
        """
    ).strip()
    assert zk_path

    def list_ops(where):
        query_id = f"{table_name}_{generate_random_string()}"
        node.query(
            f"SELECT processed_nodes FROM system.s3_queue_metadata WHERE {where}",
            query_id=query_id,
        )
        node.query("SYSTEM FLUSH LOGS")
        return int(
            node.query(
                f"""
                SELECT ProfileEvents['ZooKeeperList']
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'QueryFinish'
                ORDER BY event_time_microseconds DESC
                LIMIT 1
                """
            ).strip()
        )

    # A matching filter still lists the table's folder.
    assert list_ops(f"zookeeper_path = '{zk_path}'") >= 1
    # A filter that matches no registered path is pushed down before the keeper
    # reads, so nothing is probed.
    assert list_ops("zookeeper_path = '/no/such/queue/path'") == 0

    node.query(f"DROP TABLE {table_name} SYNC")


def test_system_queue_metadata_broken_mandatory_folder(started_cluster):
    # `processed` (unordered mode), `processing` and `failed` are created up
    # front together with the queue metadata, so their absence means the layout
    # in keeper is broken and the queue will start failing its own updates.
    # The table must surface that as an error rather than report an empty
    # folder, which would make a broken queue look healthy.
    node = started_cluster.instances["instance"]
    table_name = f"test_system_queue_metadata_broken_{generate_random_string()}"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    # No materialized view: nothing streams, so the folders stay as created.
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )

    def select(columns):
        return node.query_and_get_error(
            f"""
            SELECT {columns}
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        )

    # While the layout is intact, both the count and the contents are readable.
    assert (
        node.query(
            f"""
            SELECT failed_nodes_count, length(failed_nodes)
            FROM system.s3_queue_metadata
            WHERE zookeeper_path ilike '%{keeper_path}%'
            """
        ).strip()
        == "0\t0"
    )

    zk = started_cluster.get_kazoo_client("zoo1")
    zk.delete(f"{keeper_path}/failed")

    # Both the count-only path (stat) and the contents path (list) must fail.
    for columns in ["failed_nodes_count", "failed_nodes"]:
        error = select(columns)
        assert "No node" in error, error
        assert f"{keeper_path}/failed" in error, error

    # Restore the layout so the table can be dropped normally.
    zk.create(f"{keeper_path}/failed")
    node.query(f"DROP TABLE {table_name} SYNC")


def test_selected_rows_not_double_counted(started_cluster):
    # The queue source reads the file's format through the inner pipeline of
    # StorageObjectStorageSource::createReader and reports the same rows again through
    # ISource auto-progress, so `SelectedRows` and `SelectedBytes` are twice the query's own
    # `read_rows`/`read_bytes` unless that inner pipeline has profile event updates disabled.
    # See #116301.
    node = started_cluster.instances["instance"]
    table_name = f"test_selected_rows_{generate_random_string()}"
    files_path = f"{table_name}_data"
    row_num = 10

    generate_random_files(started_cluster, files_path, 1, row_num=row_num)
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": f"/clickhouse/test_{table_name}_{generate_random_string()}"
        },
    )

    # A direct select runs the queue source in the foreground, so its counters land in the
    # query's own `query_log` row. The streaming path has no row to read them from: its insert
    # is executed straight from `InterpreterInsertQuery`, never through `executeQuery`.
    query_id = f"{table_name}_direct"
    node.query(
        f"SELECT * FROM {table_name} FORMAT Null",
        query_id=query_id,
        settings={"stream_like_engine_allow_direct_select": 1},
    )
    node.query("SYSTEM FLUSH LOGS query_log")

    read_rows, read_bytes, selected_rows, selected_bytes = (
        node.query(
            f"""
            SELECT read_rows, read_bytes,
                   ProfileEvents['SelectedRows'], ProfileEvents['SelectedBytes']
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            ORDER BY event_time_microseconds DESC LIMIT 1
            """
        )
        .strip()
        .split("\t")
    )

    # The read amounts are pinned as well, so a select that stopped reading the file cannot
    # satisfy the equalities with both sides at zero.
    assert read_rows == str(row_num), (read_rows, read_bytes)
    assert read_bytes != "0", (read_rows, read_bytes)
    assert selected_rows == read_rows, (selected_rows, read_rows)
    assert selected_bytes == read_bytes, (selected_bytes, read_bytes)

    node.query(f"DROP TABLE {table_name} SYNC")

import io
import json
import logging
import random
import string
import time
import uuid
from multiprocessing.dummy import Pool
from datetime import datetime

import pytest
from kazoo.exceptions import NoNodeError

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
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
from helpers.config_cluster import minio_secret_key

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
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
                "configs/disable_streaming.xml",
            ],
            user_configs=[
                "configs/users.xml",
                "configs/enable_keeper_fault_injection.xml",
                "configs/keeper_retries.xml",
                "configs/insert_deduplication.xml",
            ],
            stay_alive=True,
            cpu_limit=8,
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
                "configs/insert_deduplication.xml",
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
                "configs/compatibility.xml",
            ],
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers_245.xml",
                "configs/insert_deduplication.xml",
            ],
            with_installed_binary=True,
        )
        cluster.add_instance(
            "instance2_24.5",
            with_zookeeper=True,
            keeper_required_feature_flags=["create_if_not_exists"],
            image="clickhouse/clickhouse-server",
            tag="24.5",
            stay_alive=True,
            user_configs=[
                "configs/users.xml",
                "configs/compatibility.xml",
            ],
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers_245.xml",
            ],
            with_installed_binary=True,
        )
        cluster.add_instance(
            "instance_without_keeper_fault_injection",
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
                "configs/disable_streaming.xml",
            ],
            user_configs=[
                "configs/users.xml",
                "configs/keeper_retries.xml",
                "configs/insert_deduplication.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "readonly_instance",
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
                "configs/disable_streaming.xml",
                "configs/read_only_mode.xml",
            ],
            user_configs=[
                "configs/users.xml",
                "configs/enable_keeper_fault_injection.xml",
                "configs/keeper_retries.xml",
                "configs/insert_deduplication.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_upgrade_3(started_cluster):
    node = started_cluster.instances["instance_24.5"]
    if "24.5" not in node.query("select version()").strip():
        node.restart_with_original_version(clear_data_dir=True)
    assert "24.5" in node.query("select version()").strip()

    table_name = f"test_upgrade_3_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        no_settings=True,
        version="24.5",
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

    assert table_name in node.query("SHOW TABLES")

    node.query(
        f"""
        ALTER TABLE {table_name} MODIFY SETTING polling_min_timeout_ms=111
    """
    )
    assert 111 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_min_timeout_ms'"
        )
    )

    node.query(
        f"""
        ALTER TABLE {table_name} MODIFY SETTING polling_min_timeout_ms=222, polling_max_timeout_ms=333
    """
    )
    assert 222 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_min_timeout_ms'"
        )
    )
    assert 333 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_max_timeout_ms'"
        )
    )

    assert "polling_max_timeout_ms = 333" in node.query(
        f"SHOW CREATE TABLE {table_name}"
    )

    node.restart_clickhouse()

    assert "polling_max_timeout_ms = 333" in node.query(
        f"SHOW CREATE TABLE {table_name}"
    )

    assert 333 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_max_timeout_ms'"
        )
    )
    node.query(f"DROP TABLE {table_name} SYNC")


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
def test_filtering_files(started_cluster, mode):
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_replicated_{mode}_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 100

    node1.query("DROP DATABASE IF EXISTS r")
    node2.query("DROP DATABASE IF EXISTS r")

    node1.query(
        f"CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node1')"
    )
    node2.query(
        f"CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node2')"
    )

    create_table(
        started_cluster,
        node1,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 100,
            "polling_backoff_ms": 0,
        },
        database_name="r",
    )

    files = [(f"{files_path}/test_{i}.csv", i) for i in range(0, files_to_generate)]
    total_values = generate_random_files(
        started_cluster,
        files_path,
        files_to_generate,
        start_ind=0,
        row_num=1,
        files=files,
    )
    incorrect_values = [
        ["failed", 1, 1],
    ]
    incorrect_values_csv = (
        "\n".join((",".join(map(str, row)) for row in incorrect_values)) + "\n"
    ).encode()

    failed_file = f"{files_path}/testz_fff.csv"
    put_s3_file_content(started_cluster, failed_file, incorrect_values_csv)

    create_mv(node1, f"r.{table_name}", dst_table_name)

    def get_count():
        return int(node1.query(f"SELECT count() FROM default.{dst_table_name}"))

    expected_rows = files_to_generate
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()

    create_mv(node2, f"r.{table_name}", dst_table_name)
    for _ in range(20):
        if node2.contains_in_log(f"StorageS3Queue (r.{table_name}): Processed rows: 0"):
            break
        time.sleep(1)
    assert node2.contains_in_log(f"StorageS3Queue (r.{table_name}): Processed rows: 0")

    found_1_global = False
    found_2_global = False
    if mode == "unordered":
        is_unordered = True
    else:
        is_unordered = False

    for file in files:
        found_1 = node2.contains_in_log(
            f"StorageS3Queue (r.{table_name}): Skipping file {file[0]}: Processed"
        )
        found_1_global = found_1_global or found_1

        if is_unordered:
            found_2 = node2.contains_in_log(
                f"Will skip file {file[0]}: it should be processed by"
            )
            found_2_global = found_2_global or found_2
        else:
            found_2 = False

        assert found_1 or found_2, "Failed with file " + file[0]

    assert found_1_global
    if is_unordered:
        assert found_2_global

    assert node2.contains_in_log(
        f"StorageS3Queue (r.{table_name}): Skipping file {failed_file}: Failed"
    ) or node1.contains_in_log(
        f"StorageS3Queue (r.{table_name}): Skipping file {failed_file}: Failed"
    )


def test_failed_commit(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_failed_commit_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=2
    )

    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")

    create_mv(node, table_name, dst_table_name)

    def check_failpoint():
        return node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 999. Coordination::Exception: Failed to commit processed files"
        )

    for _ in range(100):
        if check_failpoint():
            break
        time.sleep(1)

    assert check_failpoint()

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    count_failed = int(
        node.count_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 999. Coordination::Exception: Failed to commit processed files"
        )
    )
    count = get_count()
    expected_rows = 2 * count_failed
    expected_rows_upper = 2 * (
        count_failed + 2
    )  # Could get more in between getting 'count_failed' and getting 'count'

    assert expected_rows <= count and count <= expected_rows_upper

    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    processed = False
    for _ in range(100):
        node.query("SYSTEM FLUSH LOGS")
        processed = int(
            node.query(
                f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
            )
        )
        if processed == 1:
            break
        time.sleep(1)

    assert processed == 1
    assert 2 == int(
        node.query(
            f"SELECT rows_processed FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )


def test_failure_in_the_middle(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_failure_in_the_middle_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 1

    format = "column1 String, column2 String"
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 10000,
            "polling_max_timeout_ms": 100,
        },
    )
    values = []
    num_rows = 1000000
    for _ in range(num_rows):
        values.append(
            ["".join("a" for i in range(1000)), "".join("a" for i in range(1000))]
        )
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()

    file_name = f"{table_name}_file.csv"
    put_s3_file_content(started_cluster, f"{files_path}/{file_name}", values_csv)

    node.query(
        f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_in_the_middle_of_file"
    )

    try:
        create_mv(node, table_name, dst_table_name, format=format)

        def check_failpoint():
            return node.contains_in_log(
                f"StorageS3Queue (default.{table_name}): Got an error while pulling chunk: Code: 1002. DB::Exception: Failed to read file. Processed rows:"
            )

        for _ in range(120):
            if check_failpoint():
                break
            time.sleep(1)

        assert check_failpoint()

        node.query("SYSTEM FLUSH LOGS")
        assert 0 == int(
            node.query(
                f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
            )
        )

        for _ in range(20):
            if 0 < int(
                node.query(
                    f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Failed' and exception ilike '%Failed to read file. Processed rows%'"
                )
            ):
                break
            time.sleep(1)

        assert 0 < int(
            node.query(
                f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Failed' and exception ilike '%Failed to read file. Processed rows%'"
            )
        )
    finally:
        node.query(
            f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_in_the_middle_of_file"
        )

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    assert 0 == get_count()

    processed = False
    for _ in range(50):
        node.query("SYSTEM FLUSH LOGS")
        processed = int(
            node.query(
                f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
            )
        )
        if processed == 1:
            break
        time.sleep(1)

    assert processed == 1
    assert num_rows == int(
        node.query(
            f"SELECT rows_processed FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )
    node.query(f"DROP TABLE {dst_table_name} SYNC")


def test_macros_support(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_macros_{uuid.uuid4().hex[:8]}"
    files_path = f"{table_name}_data"

    node.query(
        f"""
        DROP DATABASE IF EXISTS a;
        DROP DATABASE IF EXISTS r;
        CREATE DATABASE a ENGINE=Atomic;
        CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node1');
        """
    )

    res = create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": "{table}/{uuid}",
        },
        database_name="a",
        expect_error=True,
    )

    assert "Macro 'uuid' in engine arguments is only supported" in res

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": "{table}/{uuid}",
        },
        database_name="r",
    )

    table_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database = 'r' AND name = '{table_name}'"
    ).strip()
    keeper_path = f"/clickhouse/s3queue/{table_name}/{table_uuid}/"

    assert (
        node.query(
            f"SELECT count() > 0 FROM system.zookeeper WHERE path = '{keeper_path}'"
        )
        == "1\n"
    )
    assert f"keeper_path = \\'{table_name}/{{uuid}}\\'" in node.query(
        f"SHOW CREATE TABLE r.{table_name}"
    )


def test_disable_streaming(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_disable_streaming_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    assert (
        "false"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/disable_streaming.xml",
        "0",
        "1",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert (
        "true"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "processing_threads_num": 1,
            "keeper_path": keeper_path,
        },
    )

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Streaming is disabled, rescheduling next check in 5000 ms"
    )
    assert 0 == get_count()

    assert (
        "true"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/disable_streaming.xml",
        "1",
        "0",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert (
        "false"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    expected_rows = files_to_generate
    for _ in range(100):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()


def test_disable_insertion_and_mutation(started_cluster):
    node = started_cluster.instances["readonly_instance"]

    table_name = f"test_disable_insertion_and_mutation_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    assert (
        "true"
        == node.query(
            "SELECT getServerSetting('disable_insertion_and_mutation')"
        ).strip()
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "processing_threads_num": 1,
            "keeper_path": keeper_path,
        },
    )

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Streaming is disabled, rescheduling next check in 5000 ms"
    )
    assert 0 == get_count()


def test_shutdown_logs(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_shutdown_logs"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 5,
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=100
    )
    create_mv(node, table_name, dst_table_name)
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    node.restart_clickhouse()

    def check_in_text_log(message, logger_name):
        return int(
            node.query(
                f"SELECT count() FROM system.text_log WHERE logger_name ilike '%{logger_name}%' and message ilike '%{message}%' and event_time >= toDateTime('{start_time}')"
            )
        )

    assert 1 == check_in_text_log("Shutting down storages", "Application")
    assert 1 == check_in_text_log(
        "Waiting for streaming to finish...", f"StorageS3Queue (default.{table_name})"
    )
    assert 1 == check_in_text_log(
        "Shut down storage", f"StorageS3Queue (default.{table_name})"
    )
    assert 1 == check_in_text_log("Shutting down system logs", "DatabaseCatalog")
    assert 0 == check_in_text_log("Shutting down system databases", "DatabaseCatalog")
    node.query(f"DROP TABLE {dst_table_name} SYNC")


def test_shutdown_order(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_shutdown_order_{generate_random_string()}"
    dst_table_name = f"a_{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    format = "column1 Int32, column2 String"
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 5,
            "polling_max_timeout_ms": 100,
            "polling_min_timeout_ms": 100,
        },
    )

    def insert():
        files_to_generate = 10
        table_name_suffix = f"{uuid.uuid4()}"
        for i in range(files_to_generate):
            file_name = f"file_{table_name}_{table_name_suffix}_{i}.csv"
            s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
            node.query(
                f"INSERT INTO FUNCTION {s3_function} select number, randomString(100) FROM numbers(50000)"
            )

    insert()

    mv_table_name = f"{table_name}_mv"
    create_mv(
        node,
        table_name,
        dst_table_name,
        mv_name=mv_table_name,
        format=format,
        dst_table_engine=f"ReplicatedMergeTree('/clickhouse/tables/{table_name}', 'node')",
    )

    node.restart_clickhouse()

    node.query(f"SYSTEM FLUSH LOGS system.text_log")

    def check_in_text_log(message, logger_name):
        return int(
            node.query(
                f"SELECT count() FROM system.text_log WHERE logger_name ilike '%{logger_name}%' and message ilike '%{message}%'"
            )
        )

    assert 0 == check_in_text_log(
        "Failed to process data", f"StorageS3Queue(default.{table_name})"
    )

    node.query(f"SYSTEM FLUSH LOGS system.s3queue_log")

    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Failed'"
        )
    )
    new_table_name = f"{table_name}_new"
    new_mv_table_name = f"{new_table_name}_mv"
    node.query(f"DROP TABLE {mv_table_name} SYNC")
    node.query(f"RENAME TABLE {table_name} to {new_table_name}")

    create_mv(
        node,
        new_table_name,
        dst_table_name,
        mv_name=new_mv_table_name,
        format=format,
        dst_table_exists=True,
    )
    insert()
    time.sleep(0.1)

    node.restart_clickhouse()
    assert 0 == check_in_text_log(
        "Failed to process data", f"StorageS3Queue(default.{table_name})"
    )
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Failed'"
        )
    )

    node.query(f"DROP TABLE {new_table_name} SYNC")
    node.query(f"DROP TABLE {dst_table_name} SYNC")


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
@pytest.mark.parametrize("limit", [1, 9999999999])
def test_mv_settings(started_cluster, mode, limit):
    node = started_cluster.instances["instance"]
    table_name = f"test_mv_settings_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    if limit == 9999999999:
        expected_parts_num = 1
    else:
        expected_parts_num = 1

    format = "column1 String"
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "polling_max_timeout_ms": 0,
            "polling_min_timeout_ms": 0,
            "min_insert_block_size_rows_for_materialized_views": limit,
            "min_insert_block_size_bytes_for_materialized_views": limit,
        },
    )

    num_rows = 10

    def insert():
        files_to_generate = 5
        table_name_suffix = f"{uuid.uuid4()}"
        for i in range(files_to_generate):
            file_name = f"file_{table_name}_{table_name_suffix}_{i}.csv"
            s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
            node.query(
                f"INSERT INTO FUNCTION {s3_function} select randomString(10) FROM numbers({num_rows})"
            )

    insert()

    mv_table_name = f"{table_name}_mv"
    create_mv(
        node,
        table_name,
        dst_table_name,
        mv_name=mv_table_name,
        format=format,
    )
    node.query(f"SYSTEM STOP MERGES {dst_table_name}")

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    expected_rows = num_rows
    for _ in range(100):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_parts_num == int(
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{dst_table_name}' AND level = 0"
        )
    )


def test_detach_attach_table(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_detach_attach_table_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    mv_table_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    format = "column1 Int32, column2 String"
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "polling_max_timeout_ms": 0,
            "polling_min_timeout_ms": 0,
        },
    )

    node.query(f"DETACH TABLE {table_name}")
    node.query(f"ATTACH TABLE {table_name}")

    num_rows = 10000
    file_count = [0]

    def insert():
        table_name_suffix = f"{uuid.uuid4()}"
        file_count[0] += 1
        file_name = f"file_{table_name}_{table_name_suffix}_{file_count[0]}.csv"
        s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
        node.query(
            f"INSERT INTO FUNCTION {s3_function} select number, randomString(100) FROM numbers({num_rows})"
        )

    insert()

    create_mv(
        node,
        table_name,
        dst_table_name,
        mv_name=mv_table_name,
        format=format,
    )

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    for _ in range(20):
        if num_rows == get_count():
            break
        time.sleep(1)

    assert num_rows == get_count()


def test_failed_startup(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_failed_startup_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    mv_table_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    format = "column1 Int32, column2 String"
    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_startup")
    assert "Failed to startup" in create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        expect_error=True,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "polling_max_timeout_ms": 0,
            "polling_min_timeout_ms": 0,
        },
    )
    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_startup")

    zk = started_cluster.get_kazoo_client("zoo1")

    # Wait for table data to be removed.
    uuid = node.query(
        f"select uuid from system.tables where name = '{table_name}'"
    ).strip()
    wait_message = f"StorageObjectStorageQueue({keeper_path}): Table '{uuid}' has been removed from the registry"
    wait_message_2 = (
        f"StorageObjectStorageQueue({keeper_path}): Table is unregistered after retry"
    )
    for _ in range(50):
        if node.contains_in_log(wait_message) or node.contains_in_log(wait_message_2):
            break
        time.sleep(1)
    assert node.contains_in_log(wait_message) or node.contains_in_log(wait_message_2)

    try:
        zk.get(f"{keeper_path}")
        assert False
    except NoNodeError:
        pass

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
            "polling_max_timeout_ms": 0,
            "polling_min_timeout_ms": 0,
        },
    )

    assert len(zk.get(f"{keeper_path}")) > 0


def test_create_or_replace_table(started_cluster):
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_rename_table_2_{uuid.uuid4().hex[:8]}"
    db_name = f"db_{table_name}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    node1.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node1')"
    )
    node2.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node2')"
    )

    create_table(
        started_cluster,
        node1,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 100,
            "polling_backoff_ms": 0,
        },
        database_name=db_name,
    )
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")

    create_table(
        started_cluster,
        node1,
        table_name,
        "unordered",
        files_path,
        replace=True,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 200,
            "polling_backoff_ms": 0,
        },
        database_name=db_name,
    )

    create_mv(node1, f"{db_name}.{table_name}", dst_table_name, mv_name=mv_name)
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")


def test_persistent_processing_nodes_cleanup(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"max_persistent_processing_nodes_cleanup_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 1000,
            "use_persistent_processing_nodes": 1,
            "persistent_processing_node_ttl_seconds": 10,
            "cleanup_interval_min_ms": 0,
            "cleanup_interval_max_ms": 0,
        },
    )

    zk = started_cluster.get_kazoo_client("zoo1")
    zk.create(f"{keeper_path}/processing/test", b"somedata")
    assert b"somedata" == zk.get(f"{keeper_path}/processing/test")[0]

    bucket_lock_path = f"{keeper_path}/buckets/0/lock"
    zk.create(bucket_lock_path, b"somedata")

    time.sleep(5)
    assert b"somedata" == zk.get(f"{keeper_path}/processing/test")[0]
    assert b"somedata" == zk.get(bucket_lock_path)[0]
    time.sleep(10)
    try:
        zk.get(f"{keeper_path}/processing/test")[0]
        assert False
    except NoNodeError:
        pass
    try:
        zk.get(bucket_lock_path)
        assert False
    except NoNodeError:
        pass


def test_persistent_processing(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"max_persistent_processing_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    format = "a Int32, b String"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 1000,
            "use_persistent_processing_nodes": 1,
            "persistent_processing_node_ttl_seconds": 10,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 500,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 100,
        },
    )
    file_name = f"file_{table_name}.csv"
    s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
    node.query(
        f"INSERT INTO FUNCTION {s3_function} select number, randomString(100) FROM numbers(10)"
    )

    zk = started_cluster.get_kazoo_client("zoo1")
    nodes = zk.get_children(f"{keeper_path}/processing")
    assert len(nodes) == 0

    node.query(
        f"""
        CREATE TABLE {dst_table_name} ({format})
        ENGINE = MergeTree
        ORDER BY a;
    """
    )
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT * FROM {table_name} WHERE NOT sleepEachRow(0.5);
        """
    )

    found = False
    for _ in range(20):
        nodes = zk.get_children(f"{keeper_path}/processing")
        if len(nodes) > 0:
            found = True
            break
        time.sleep(1)
    assert found

    time.sleep(10)

    nodes = zk.get_children(f"{keeper_path}/processing")
    assert len(nodes) == 0


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
def test_persistent_processing_failed_commit_retries(started_cluster, mode):
    node = started_cluster.instances["instance_without_keeper_fault_injection"]
    table_name = (
        f"max_persistent_processing_failed_commit_retries_{generate_random_string()}"
    )
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    format = "a Int32, b String"

    processing_threads = 16
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 1000,
            "use_persistent_processing_nodes": 1,
            "persistent_processing_node_ttl_seconds": 60,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 500,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 100,
            "processing_threads_num": processing_threads,
        },
    )
    i = [0]

    def insert():
        i[0] += 1
        file_name = f"file_{table_name}_{i[0]}.csv"
        s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
        node.query(
            f"INSERT INTO FUNCTION {s3_function} select number, randomString(100) FROM numbers(10)"
        )

    insert()

    zk = started_cluster.get_kazoo_client("zoo1")
    nodes = zk.get_children(f"{keeper_path}/processing")
    assert len(nodes) == 0
    is_ordered = mode == "ordered"
    if is_ordered:
        nodes = zk.get_children(f"{keeper_path}/buckets")
        assert len(nodes) == processing_threads
        for id in range(processing_threads):
            try:
                zk.get(f"{keeper_path}/buckets/{id}/lock")
                assert False
            except NoNodeError:
                pass

    node.query(
        f"""
        CREATE TABLE {dst_table_name} ({format})
        ENGINE = MergeTree
        ORDER BY a;
    """
    )

    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit_once")
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT * FROM {table_name} WHERE NOT sleepEachRow(0.5);
        """
    )

    found = False
    for _ in range(100):
        nodes = zk.get_children(f"{keeper_path}/processing")
        if len(nodes) > 0:
            found = True
            break
        time.sleep(0.1)
    assert found
    if is_ordered:
        locked_buckets = 0
        for id in range(processing_threads):
            try:
                zk.get(f"{keeper_path}/buckets/{id}/lock")
                locked_buckets += 1
            except NoNodeError:
                pass
        assert locked_buckets > 0

    found = False
    for _ in range(10):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Successfully committed"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit_once")

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Failed to commit processed files at try 1"
    )
    assert not node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Failed to commit processed files at try 5"
    )

    nodes = zk.get_children(f"{keeper_path}/processing")
    assert len(nodes) == 0

    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    insert()

    found = False
    for _ in range(20):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 999. Coordination::Exception: Failed to commit processed files"
        ):
            found = True
            break
        time.sleep(1)

    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")
    assert found

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Failed to commit processed files at try 10"
    )

    found = False
    for _ in range(30):
        nodes = zk.get_children(f"{keeper_path}/processing")
        if len(nodes) == 0:
            found = True
            break
        time.sleep(1)
    nodes = zk.get_children(f"{keeper_path}/processing")
    assert found, f"Nodes: {nodes}"

    node.query(f"DROP TABLE default.{table_name} SYNC")
    if is_ordered:
        for id in range(processing_threads):
            try:
                zk.get(f"{keeper_path}/buckets/{id}/lock")
                assert False
            except NoNodeError:
                pass


def test_metadata_cache_exact_size_tracking(started_cluster):
    node = started_cluster.instances["instance"]
    mode = "unordered"
    # Clear the cache, there is no drop cache command for now
    node.restart_clickhouse()

    table_name = f"test_cache_exact_{mode}_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "metadata_cache_size_bytes": 100000,
            "metadata_cache_size_elements": 100,
            "processing_threads_num": 1,
        },
    )

    # Process 1 file first to measure exact sizeof(FileStatus)
    generate_random_files(
        started_cluster, files_path, 1, start_ind=0, row_num=5
    )
    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    for _ in range(60):
        if 5 == get_count():
            break
        time.sleep(1)

    assert 5 == get_count()

    cache_size_bytes_str = node.query(
        f"SELECT value FROM system.metrics WHERE metric = 'ObjectStorageQueueMetadataCacheSizeBytes'"
    ).strip()

    cache_size_elements_str = node.query(
        f"SELECT value FROM system.metrics WHERE metric = 'ObjectStorageQueueMetadataCacheSizeElements'"
    ).strip()

    if not cache_size_bytes_str or not cache_size_elements_str:
        logging.warning("Cache metrics not available, skipping size verification")
        return

    cache_size_bytes = int(cache_size_bytes_str)
    cache_size_elements = int(cache_size_elements_str)

    assert cache_size_elements == 1, f"Expected 1 cache element, got {cache_size_elements}"

    # Measure exact sizeof(FileStatus) for this platform
    sizeof_file_status = cache_size_bytes
    logging.info(f"sizeof(FileStatus) = {sizeof_file_status} bytes")

    # Sanity check: FileStatus has 2 mutexes + 6 atomics + 1 string + additional cache tracking fields
    if is_arm():
        assert 200 <= sizeof_file_status <= 300, f"Unexpected sizeof(FileStatus) = {sizeof_file_status} on ARM"
    else:
        assert 250 <= sizeof_file_status <= 300, f"Unexpected sizeof(FileStatus) = {sizeof_file_status} on x64"

    # Process 19 more files
    files_to_generate = 19
    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=1, row_num=5
    )

    expected_rows = 100  # 20 files * 5 rows
    for _ in range(60):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count(), f"Expected {expected_rows} rows, got {get_count()}"

    cache_size_bytes = int(
        node.query(
            f"SELECT value FROM system.metrics WHERE metric = 'ObjectStorageQueueMetadataCacheSizeBytes'"
        ).strip()
    )
    cache_size_elements = int(
        node.query(
            f"SELECT value FROM system.metrics WHERE metric = 'ObjectStorageQueueMetadataCacheSizeElements'"
        ).strip()
    )

    logging.info(
        f"Cache metrics: {cache_size_elements} elements, {cache_size_bytes} bytes"
    )

    assert cache_size_elements == 20, f"Expected 20 cache elements, got {cache_size_elements}"

    # Verify exact total size = sizeof(FileStatus) * number_of_files
    expected_total_size = sizeof_file_status * 20
    assert cache_size_bytes == expected_total_size, (
        f"Cache size {cache_size_bytes} doesn't match expected {expected_total_size} (sizeof(FileStatus) * 20)"
    )

    # Test that ALTER immediately evicts cache entries
    size_for_10_files = sizeof_file_status * 10
    node.query(
        f"""
        ALTER TABLE {table_name}
        MODIFY SETTING
            metadata_cache_size_bytes = {size_for_10_files},
            metadata_cache_size_elements = 10
        """
    )

    new_cache_size_bytes = int(
        node.query(
            f"SELECT value FROM system.metrics WHERE metric = 'ObjectStorageQueueMetadataCacheSizeBytes'"
        ).strip()
    )
    new_cache_size_elements = int(
        node.query(
            f"SELECT value FROM system.metrics WHERE metric = 'ObjectStorageQueueMetadataCacheSizeElements'"
        ).strip()
    )

    logging.info(
        f"After eviction - Cache metrics: {new_cache_size_elements} elements, {new_cache_size_bytes} bytes"
    )

    assert new_cache_size_elements <= 10, (
        f"Cache elements {new_cache_size_elements} should be evicted to <= 10"
    )
    assert new_cache_size_bytes <= size_for_10_files, (
        f"Cache size {new_cache_size_bytes} should be evicted to <= {size_for_10_files}"
    )


@pytest.mark.parametrize("mode", ["unordered"])
def test_deduplication(started_cluster, mode):
    node = started_cluster.instances["instance_without_keeper_fault_injection"]
    table_name = f"test_deduplication_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    format = "a Int32, b String"

    processing_threads = 16
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 1000,
            "use_persistent_processing_nodes": 1,
            "persistent_processing_node_ttl_seconds": 60,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 500,
            "polling_max_timeout_ms": 200,
            "polling_backoff_ms": 100,
            "processing_threads_num": processing_threads,
            # Set tracked files limit to 1, to make sure we try to read
            # those files again and deduplicate
            "s3queue_tracked_file_ttl_sec": 1,
            "after_processing": "delete",
            "deduplication_v2": 1,
        },
    )
    i = [0]

    num_rows = 5

    def insert(file_i=None):
        i[0] += 1
        if file_i is None:
            file_name = f"file_{table_name}_{i[0]}.csv"
        else:
            file_name = f"file_{table_name}_{file_i}.csv"

        s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
        node.query(
            f"INSERT INTO FUNCTION {s3_function} select number, toString({i[0]}) FROM numbers({num_rows})"
        )

    files_num = 10
    for _ in range(files_num):
        insert()

    node.query(
        f"""
        CREATE TABLE {dst_table_name} ({format}, _path String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table_name}', 'node')
        ORDER BY a SETTINGS replicated_deduplication_window_seconds_for_async_inserts = 1000;
    """
    )

    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_after_insert")
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT *, _path FROM {table_name};
        """
    )

    found = False
    for _ in range(50):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 710. DB::Exception: Failed after insert"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    expected_rows = files_num * num_rows
    assert expected_rows == int(node.query(f"SELECT count() FROM {dst_table_name}"))

    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_after_insert")

    found = False
    for _ in range(50):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Successfully committed 10 files"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Failed to process data:"
    )
    assert expected_rows == int(node.query(f"SELECT count() FROM {dst_table_name}"))

    node.query(
        f"""
        ALTER TABLE {table_name} MODIFY SETTING after_processing='keep'
    """
    )

    insert(1)

    found = False
    for _ in range(50):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Successfully committed 1 files"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    # etag changed, we should not deduplicate the file
    files_num += 1
    expected_rows = files_num * num_rows
    # We processed the file above, but it was deduplicated, so nothing in destination table.
    assert expected_rows == int(node.query(f"SELECT count() FROM {dst_table_name}"))

    # wait for node ttl to expire and we will process the file again,
    # but now that after_processing=keep, we will deduplicate it,
    # as etag did not change.
    found = False
    for _ in range(50):
        node.query("SYSTEM FLUSH LOGS")
        if 1 < int(
            node.query(
                f"SELECT count() FROM system.text_log WHERE message ilike '%Successfully committed 1 files%' and logger_name ilike '%{table_name}%'"
            )
        ):
            found = True
            break
        time.sleep(1)
    assert found

    assert files_num * num_rows == int(
        node.query(f"SELECT count() FROM {dst_table_name}")
    )


@pytest.mark.parametrize("mode", ["unordered"])
def test_deduplication_with_multiple_chunks(started_cluster, mode):
    """
    Test deduplication when files are processed in multiple chunks.
    Each chunk will have a deduplication token = etag + file offset.
    """
    node = started_cluster.instances["instance_without_keeper_fault_injection"]
    table_name = f"test_deduplication_chunks_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    format = "a Int32, b String"

    processing_threads = 16
    num_rows = 500000
    files_num = 5

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        format=format,
        file_format="parquet",
        additional_settings={
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 1000,
            "polling_backoff_ms": 1000,
            "use_persistent_processing_nodes": 1,
            "persistent_processing_node_ttl_seconds": 60,
            "cleanup_interval_min_ms": 100,
            "cleanup_interval_max_ms": 200,
            "polling_max_timeout_ms": 200,
            "polling_backoff_ms": 100,
            "processing_threads_num": processing_threads,
            # Set tracked files limit to 1, to make sure we try to read
            # those files again and deduplicate
            "s3queue_tracked_file_ttl_sec": 1,
            "after_processing": "delete",
            "deduplication_v2": 1,
        },
    )
    i = [0]

    def insert(file_i=None):
        i[0] += 1
        if file_i is None:
            file_name = f"file_{table_name}_{i[0]}.parquet"
        else:
            file_name = f"file_{table_name}_{file_i}.parquet"

        s3_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/{file_name}', 'minio', '{minio_secret_key}')"
        node.query(
            f"INSERT INTO FUNCTION {s3_function} select number, randomString(100) FROM numbers({num_rows})"
        )

    for _ in range(files_num):
        insert()

    node.query(
        f"""
        CREATE TABLE {dst_table_name} ({format}, _path String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table_name}', 'node')
        ORDER BY a SETTINGS replicated_deduplication_window_seconds_for_async_inserts = 1000;
    """
    )

    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_after_insert")
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT *, _path FROM {table_name};
        """
    )

    # Wait for commit failures due to failpoint
    found = False
    for _ in range(100):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 710. DB::Exception: Failed after insert"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    # We must have successfully inserted all the data
    assert files_num * num_rows == int(
        node.query(f"SELECT count() FROM {dst_table_name}")
    )

    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_after_insert")

    # Wait for successful commit after disabling failpoint
    found = False
    for _ in range(50):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Successfully committed {files_num} files"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Failed to process data:"
    )

    assert files_num * num_rows == int(
        node.query(f"SELECT count() FROM {dst_table_name}")
    )

    for ii in range(files_num):
        file_name = f"{files_path}/file_{table_name}_{ii + 1}.parquet"
        step = 65409

        # We read at least 3 chunks per each file, each chunk will have size of 65409
        assert node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Read {step} rows from file {file_name} (file offset: 0"
        )
        assert node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Read {step} rows from file {file_name} (file offset: {step}"
        )
        assert node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Read {step} rows from file {file_name} (file offset: {step * 2}"
        )

    node.query(
        f"""
        ALTER TABLE {table_name} MODIFY SETTING after_processing='keep'
    """
    )

    insert(1)

    found = False
    for _ in range(50):
        if node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Successfully committed 1 files"
        ):
            found = True
            break
        time.sleep(1)
    assert found

    # We inserted 2 files with the same name, but etag is now different,
    # so it is treated as separate files.
    files_num += 1
    assert files_num * num_rows == int(
        node.query(f"SELECT count() FROM {dst_table_name}")
    )

    # wait for node ttl to expire and we will process the file again,
    # but now that after_processing=keep, we will deduplicate it,
    # as etag did not change.
    found = False
    for _ in range(50):
        node.query("SYSTEM FLUSH LOGS")
        if 1 < int(
            node.query(
                f"SELECT count() FROM system.text_log WHERE message ilike '%Successfully committed 1 files%' and logger_name ilike '%{table_name}%'"
            )
        ):
            found = True
            break
        time.sleep(1)
    assert found

    assert files_num * num_rows == int(
        node.query(f"SELECT count() FROM {dst_table_name}")
    )

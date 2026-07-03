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
from helpers.config_cluster import minio_secret_key
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
DEFAULT_AUTH = ["'minio'", f"'{minio_secret_key}'"]
NO_AUTH = ["NOSIGN"]


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


def prepare_public_s3_bucket(started_cluster):
    def create_bucket(client, bucket_name, policy):
        if client.bucket_exists(bucket_name):
            client.remove_bucket(bucket_name)

        client.make_bucket(bucket_name)

        client.set_bucket_policy(bucket_name, json.dumps(policy))

    def get_policy_with_public_access(bucket_name):
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                    ],
                    "Resource": f"arn:aws:s3:::{bucket_name}",
                },
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                    ],
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                },
            ],
        }

    minio_client = started_cluster.minio_client

    started_cluster.minio_public_bucket = f"{started_cluster.minio_bucket}-public"
    create_bucket(
        minio_client,
        started_cluster.minio_public_bucket,
        get_policy_with_public_access(started_cluster.minio_public_bucket),
    )


# TODO: Update the modes for this test to include "ordered" once PR #55795 is finished.
@pytest.mark.parametrize("mode", ["unordered"])
def test_multiple_tables_streaming_sync(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"multiple_tables_streaming_sync_{mode}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 300

    for i in range(3):
        table = f"{table_name}_{i + 1}"
        dst_table = f"{dst_table_name}_{i + 1}"
        create_table(
            started_cluster,
            node,
            table,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
            },
        )
        create_mv(node, table, dst_table)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=1
    )

    def get_count(table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(100):
        if (
            get_count(f"{dst_table_name}_1")
            + get_count(f"{dst_table_name}_2")
            + get_count(f"{dst_table_name}_3")
        ) == files_to_generate:
            break
        time.sleep(1)

    if (
        get_count(f"{dst_table_name}_1")
        + get_count(f"{dst_table_name}_2")
        + get_count(f"{dst_table_name}_3")
    ) != files_to_generate:
        info = node.query(
            f"SELECT * FROM system.s3queue WHERE zookeeper_path like '%{table_name}' ORDER BY file_name FORMAT Vertical"
        )
        logging.debug(info)
        assert False

    res1 = [
        list(map(int, l.split()))
        for l in node.query(
            f"SELECT column1, column2, column3 FROM {dst_table_name}_1"
        ).splitlines()
    ]
    res2 = [
        list(map(int, l.split()))
        for l in node.query(
            f"SELECT column1, column2, column3 FROM {dst_table_name}_2"
        ).splitlines()
    ]
    res3 = [
        list(map(int, l.split()))
        for l in node.query(
            f"SELECT column1, column2, column3 FROM {dst_table_name}_3"
        ).splitlines()
    ]
    assert {tuple(v) for v in res1 + res2 + res3} == set(
        [tuple(i) for i in total_values]
    )

    # Checking that all files were processed only once
    time.sleep(10)
    assert (
        get_count(f"{dst_table_name}_1")
        + get_count(f"{dst_table_name}_2")
        + get_count(f"{dst_table_name}_3")
    ) == files_to_generate


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_multiple_tables_streaming_sync_distributed(started_cluster, mode):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    # A unique table name is necessary for repeatable tests
    table_name = (
        f"multiple_tables_streaming_sync_distributed_{mode}_{generate_random_string()}"
    )
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1000
    row_num = 10
    total_rows = row_num * files_to_generate

    for instance in [node, node_2]:
        create_table(
            started_cluster,
            instance,
            table_name,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_buckets": 2,
                "polling_max_timeout_ms": 2000,
                "polling_backoff_ms": 1000,
                **({"s3queue_processing_threads_num": 1} if mode == "ordered" else {}),
            },
        )

    for instance in [node, node_2]:
        create_mv(instance, table_name, dst_table_name)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=row_num
    )

    def get_count(node, table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(150):
        if (
            get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        ) == total_rows:
            break
        time.sleep(1)

    if (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) != total_rows:
        info = node.query(
            f"SELECT * FROM system.s3queue WHERE zookeeper_path like '%{table_name}' ORDER BY file_name FORMAT Vertical"
        )
        logging.debug(info)
        assert False

    get_query = f"SELECT column1, column2, column3 FROM {dst_table_name}"
    res1 = [list(map(int, l.split())) for l in run_query(node, get_query).splitlines()]
    res2 = [
        list(map(int, l.split())) for l in run_query(node_2, get_query).splitlines()
    ]

    logging.debug(
        f"res1 size: {len(res1)}, res2 size: {len(res2)}, total_rows: {total_rows}"
    )

    assert len(res1) + len(res2) == total_rows

    # Checking that all engines have made progress
    assert len(res1) > 0
    assert len(res2) > 0

    assert {tuple(v) for v in res1 + res2} == set([tuple(i) for i in total_values])

    # Checking that all files were processed only once
    time.sleep(10)
    assert (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) == total_rows


def test_max_set_age(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = "max_set_age"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    max_age = 20
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "tracked_file_ttl_sec": max_age,
            "cleanup_interval_min_ms": max_age / 3,
            "cleanup_interval_max_ms": max_age / 3,
            "polling_max_timeout_ms": 5000,
            "polling_backoff_ms": 1000,
            "processing_threads_num": 1,
        },
    )
    create_mv(node, table_name, dst_table_name)

    _ = generate_random_files(started_cluster, files_path, files_to_generate, row_num=1)

    expected_rows = files_to_generate

    node.wait_for_log_line("Checking node limits")
    node.wait_for_log_line("Node limits check finished")

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    def wait_for_condition(check_function, max_wait_time=1.5 * max_age):
        before = time.time()
        while time.time() - before < max_wait_time:
            if check_function():
                return
            time.sleep(0.25)
        assert False

    wait_for_condition(lambda: get_count() == expected_rows)
    assert files_to_generate == int(
        node.query(f"SELECT uniq(_path) from {dst_table_name}")
    )

    expected_rows *= 2
    wait_for_condition(lambda: get_count() == expected_rows)
    assert files_to_generate == int(
        node.query(f"SELECT uniq(_path) from {dst_table_name}")
    )

    paths_count = [
        int(x)
        for x in node.query(
            f"SELECT count() from {dst_table_name} GROUP BY _path"
        ).splitlines()
    ]
    assert files_to_generate == len(paths_count)
    for path_count in paths_count:
        assert 2 == path_count

    def get_object_storage_failures():
        return int(
            node.query(
                "SELECT value FROM system.events WHERE name = 'ObjectStorageQueueFailedFiles' SETTINGS system_events_show_zero_values=1"
            )
        )

    failed_count = get_object_storage_failures()

    values = [
        ["failed", 1, 1],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()

    # use a different filename for each test to allow running a bunch of them sequentially with --count
    file_with_error = f"max_set_age_fail_{uuid.uuid4().hex[:8]}.csv"
    put_s3_file_content(started_cluster, f"{files_path}/{file_with_error}", values_csv)

    wait_for_condition(lambda: failed_count + 1 == get_object_storage_failures())

    node.query("SYSTEM FLUSH LOGS")
    assert "Cannot parse input" in node.query(
        f"SELECT exception FROM system.s3queue WHERE file_name ilike '%{file_with_error}'"
    )
    assert "Cannot parse input" in node.query(
        f"SELECT exception FROM system.s3queue_log WHERE file_name ilike '%{file_with_error}' ORDER BY processing_end_time DESC LIMIT 1"
    )

    assert 1 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE file_name ilike '%{file_with_error}' AND notEmpty(exception)"
        )
    )

    wait_for_condition(lambda: failed_count + 2 == get_object_storage_failures())

    node.query("SYSTEM FLUSH LOGS")
    assert "Cannot parse input" in node.query(
        f"SELECT exception FROM system.s3queue WHERE file_name ilike '%{file_with_error}' ORDER BY processing_end_time DESC LIMIT 1"
    )
    assert 1 < int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE file_name ilike '%{file_with_error}' AND notEmpty(exception)"
        )
    )

    node.restart_clickhouse()

    expected_rows *= 2
    wait_for_condition(lambda: get_count() == expected_rows)
    assert files_to_generate == int(
        node.query(f"SELECT uniq(_path) from {dst_table_name}")
    )


def test_max_set_size(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"max_set_size"
    # A unique path is necessary for repeatable tests
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
            "s3queue_tracked_files_limit": 9,
            "s3queue_cleanup_interval_min_ms": 0,
            "s3queue_cleanup_interval_max_ms": 0,
            "s3queue_processing_threads_num": 1,
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    get_query = f"SELECT * FROM {table_name} ORDER BY column1, column2, column3"
    res1 = [list(map(int, l.split())) for l in run_query(node, get_query).splitlines()]
    assert res1 == sorted(total_values, key=lambda x: (x[0], x[1], x[2]))
    print(total_values)

    time.sleep(10)

    zk = started_cluster.get_kazoo_client("zoo1")
    processed_nodes = zk.get_children(f"{keeper_path}/processed/")
    assert len(processed_nodes) == 9

    res1 = [list(map(int, l.split())) for l in run_query(node, get_query).splitlines()]
    assert res1 == [total_values[0]]

    time.sleep(10)
    res1 = [list(map(int, l.split())) for l in run_query(node, get_query).splitlines()]
    assert res1 == [total_values[1]]


def test_drop_table(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_drop"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 300

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
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=100000
    )
    create_mv(node, table_name, dst_table_name)
    node.wait_for_log_line(f"rows from file: test_drop_data")
    node.query(f"DROP TABLE {table_name} SYNC")
    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Table is being dropped"
    ) or node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Shutdown was called"
    )


def test_s3_client_reused(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test_s3_client_reused"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    row_num = 10

    def get_created_s3_clients_count():
        value = node.query(
            f"SELECT value FROM system.events WHERE event='S3Clients'"
        ).strip()
        return int(value) if value != "" else 0

    def wait_all_processed(files_num):
        expected_count = files_num * row_num
        for _ in range(100):
            count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
            print(f"{count}/{expected_count}")
            if count == expected_count:
                break
            time.sleep(1)
        assert (
            int(node.query(f"SELECT count() FROM {dst_table_name}")) == expected_count
        )

    prepare_public_s3_bucket(started_cluster)

    s3_clients_before = get_created_s3_clients_count()

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "after_processing": "delete",
            "s3queue_processing_threads_num": 1,
            "keeper_path": keeper_path,
            "polling_backoff_ms": 0,
        },
        auth=NO_AUTH,
        bucket=started_cluster.minio_public_bucket,
    )

    s3_clients_after = get_created_s3_clients_count()
    assert s3_clients_before + 1 == s3_clients_after

    create_mv(node, table_name, dst_table_name)

    for i in range(0, 10):
        s3_clients_before = get_created_s3_clients_count()

        generate_random_files(
            started_cluster,
            files_path,
            count=1,
            start_ind=i,
            row_num=row_num,
            bucket=started_cluster.minio_public_bucket,
        )

        wait_all_processed(i + 1)

        s3_clients_after = get_created_s3_clients_count()

        assert s3_clients_before == s3_clients_after

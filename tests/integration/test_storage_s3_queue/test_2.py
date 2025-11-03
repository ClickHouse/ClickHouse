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
            user_configs=[
                "configs/users.xml",
                "configs/enable_keeper_fault_injection.xml",
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


def get_processed_files(node, table_name):
    return (
        node.query(
            f"""
select splitByChar('/', file_name)[-1] as file
from system.s3queue where zookeeper_path ilike '%{table_name}%' and status = 'Processed' order by file
        """
        )
        .strip()
        .split("\n")
    )


def get_unprocessed_files(node, table_name):
    return node.query(
        f"""
        select concat('test_',  toString(number), '.csv') as file from numbers(300)
        where file not
        in (select splitByChar('/', file_name)[-1] from system.s3queue where zookeeper_path ilike '%{table_name}%' and status = 'Processed')
        """
    )


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
def test_processing_threads(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"processing_threads_{mode}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 300
    processing_threads = 32

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": processing_threads,
        },
    )
    create_mv(node, table_name, dst_table_name)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=1
    )

    def get_count(table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(50):
        if (get_count(f"{dst_table_name}")) == files_to_generate:
            break
        time.sleep(1)

    if get_count(dst_table_name) != files_to_generate:
        processed_files = get_processed_files(node, table_name)
        unprocessed_files = get_unprocessed_files(node, table_name)
        logging.debug(
            f"Processed files: {len(processed_files)}/{files_to_generate}, unprocessed files: {unprocessed_files}, count: {get_count(dst_table_name)}"
        )
        assert False

    res = [
        list(map(int, l.split()))
        for l in node.query(
            f"SELECT column1, column2, column3 FROM {dst_table_name}"
        ).splitlines()
    ]
    assert {tuple(v) for v in res} == set([tuple(i) for i in total_values])

    if mode == "ordered":
        zk = started_cluster.get_kazoo_client("zoo1")
        nodes = zk.get_children(f"{keeper_path}")
        print(f"Metadata nodes: {nodes}")
        processed_nodes = zk.get_children(f"{keeper_path}/buckets/")
        assert len(processed_nodes) == processing_threads


@pytest.mark.parametrize(
    "mode, processing_threads",
    [
        pytest.param("unordered", 1),
        pytest.param("unordered", 8),
        pytest.param("ordered", 1),
        pytest.param("ordered", 8),
    ],
)
def test_shards(started_cluster, mode, processing_threads):
    node = started_cluster.instances["instance"]
    table_name = f"test_shards_{mode}_{processing_threads}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 300
    shards_num = 3

    for i in range(shards_num):
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
                "s3queue_processing_threads_num": processing_threads,
                "s3queue_buckets": shards_num,
            },
        )
        create_mv(node, table, dst_table)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=1
    )

    def get_count(table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(30):
        count = (
            get_count(f"{dst_table_name}_1")
            + get_count(f"{dst_table_name}_2")
            + get_count(f"{dst_table_name}_3")
        )
        if count == files_to_generate:
            break
        print(f"Current {count}/{files_to_generate}")
        time.sleep(1)

    if (
        get_count(f"{dst_table_name}_1")
        + get_count(f"{dst_table_name}_2")
        + get_count(f"{dst_table_name}_3")
    ) != files_to_generate:
        processed_files = (
            node.query(
                f"""
select splitByChar('/', file_name)[-1] as file from system.s3queue
where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 order by file
                """
            )
            .strip()
            .split("\n")
        )
        logging.debug(
            f"Processed files: {len(processed_files)}/{files_to_generate}: {processed_files}"
        )

        count = (
            get_count(f"{dst_table_name}_1")
            + get_count(f"{dst_table_name}_2")
            + get_count(f"{dst_table_name}_3")
        )
        logging.debug(f"Processed rows: {count}/{files_to_generate}")

        info = node.query(
            f"""
            select concat('test_',  toString(number), '.csv') as file from numbers(300)
            where file not in (select splitByChar('/', file_name)[-1] from system.s3queue
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0)
            """
        )
        logging.debug(f"Unprocessed files: {info}")

        files1 = (
            node.query(f"select distinct(_path) from {dst_table_name}_1")
            .strip()
            .split("\n")
        )
        files2 = (
            node.query(f"select distinct(_path) from {dst_table_name}_2")
            .strip()
            .split("\n")
        )
        files3 = (
            node.query(f"select distinct(_path) from {dst_table_name}_3")
            .strip()
            .split("\n")
        )

        def intersection(list_a, list_b):
            return [e for e in list_a if e in list_b]

        logging.debug(f"Intersecting files 1: {intersection(files1, files2)}")
        logging.debug(f"Intersecting files 2: {intersection(files1, files3)}")
        logging.debug(f"Intersecting files 3: {intersection(files2, files3)}")

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

    if mode == "ordered":
        zk = started_cluster.get_kazoo_client("zoo1")
        processed_nodes = zk.get_children(f"{keeper_path}/buckets/")
        assert len(processed_nodes) == shards_num


@pytest.mark.parametrize(
    "mode, processing_threads",
    [
        pytest.param("unordered", 1),
        pytest.param("unordered", 8),
        pytest.param("ordered", 1),
        pytest.param("ordered", 2),
    ],
)
def test_shards_distributed(started_cluster, mode, processing_threads):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    table_name = f"test_shards_distributed_{mode}_{processing_threads}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 600
    row_num = 1000
    total_rows = row_num * files_to_generate
    shards_num = 2

    i = 0
    for instance in [node, node_2]:
        create_table(
            started_cluster,
            instance,
            table_name,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_processing_threads_num": processing_threads,
                "s3queue_buckets": shards_num,
                "polling_max_timeout_ms": 1000,
                "polling_backoff_ms": 0,
            },
        )
        i += 1

    for instance in [node, node_2]:
        create_mv(instance, table_name, dst_table_name)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=row_num
    )

    def get_count(node, table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    def print_debug_info():
        processed_files = (
            node.query(
                f"""
select splitByChar('/', file_name)[-1] as file from system.s3queue where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 order by file
            """
            )
            .strip()
            .split("\n")
        )
        logging.debug(
            f"Processed files by node 1: {len(processed_files)}/{files_to_generate}"
        )
        processed_files = (
            node_2.query(
                f"""
select splitByChar('/', file_name)[-1] as file from system.s3queue where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 order by file
            """
            )
            .strip()
            .split("\n")
        )
        logging.debug(
            f"Processed files by node 2: {len(processed_files)}/{files_to_generate}"
        )

        count = get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        logging.debug(f"Processed rows: {count}/{total_rows}")

        info = node.query(
            f"""
            select concat('test_',  toString(number), '.csv') as file from numbers(300)
            where file not in (select splitByChar('/', file_name)[-1] from clusterAllReplicas(cluster, system.s3queue)
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0)
            """
        )
        logging.debug(f"Unprocessed files: {info}")

        files1 = (
            node.query(
                f"""
            select splitByChar('/', file_name)[-1] from system.s3queue
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0
            """
            )
            .strip()
            .split("\n")
        )
        files2 = (
            node_2.query(
                f"""
            select splitByChar('/', file_name)[-1] from system.s3queue
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0
            """
            )
            .strip()
            .split("\n")
        )

        def intersection(list_a, list_b):
            return [e for e in list_a if e in list_b]

        logging.debug(f"Intersecting files: {intersection(files1, files2)}")

    for _ in range(30):
        if (
            get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        ) == total_rows:
            break
        time.sleep(1)

    if (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) != total_rows:
        print_debug_info()
        assert False

    get_query = f"SELECT column1, column2, column3 FROM {dst_table_name}"
    res1 = [list(map(int, l.split())) for l in run_query(node, get_query).splitlines()]
    res2 = [
        list(map(int, l.split())) for l in run_query(node_2, get_query).splitlines()
    ]

    if len(res1) + len(res2) != total_rows or len(res1) <= 0 or len(res2) <= 0 or True:
        logging.debug(
            f"res1 size: {len(res1)}, res2 size: {len(res2)}, total_rows: {total_rows}"
        )
        print_debug_info()

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

    if mode == "ordered":
        zk = started_cluster.get_kazoo_client("zoo1")
        processed_nodes = zk.get_children(f"{keeper_path}/buckets/")
        assert len(processed_nodes) == shards_num

    node.restart_clickhouse()
    time.sleep(10)
    assert (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) == total_rows

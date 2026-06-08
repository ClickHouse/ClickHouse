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
from system.s3queue_metadata_cache where zookeeper_path ilike '%{table_name}%' and status = 'Processed' order by file
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
        in (select splitByChar('/', file_name)[-1] from system.s3queue_metadata_cache where zookeeper_path ilike '%{table_name}%' and status = 'Processed')
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
    table_name = f"test_shards_{mode}_{processing_threads}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}"
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

    for _ in range(100):
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
select splitByChar('/', file_name)[-1] as file from system.s3queue_metadata_cache
where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 order by file
                """
            )
            .strip()
            .split("\n")
        )
        print(
            f"Processed files: {len(processed_files)}/{files_to_generate}: {processed_files}"
        )

        count = (
            get_count(f"{dst_table_name}_1")
            + get_count(f"{dst_table_name}_2")
            + get_count(f"{dst_table_name}_3")
        )
        print(f"Processed rows: {count}/{files_to_generate}")

        info = node.query(
            f"""
            select concat('test_',  toString(number), '.csv') as file from numbers(300)
            where file not in (select splitByChar('/', file_name)[-1] from system.s3queue_metadata_cache
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0)
            """
        )
        print(f"Unprocessed files: {info}")

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

        print(f"Intersecting files 1: {intersection(files1, files2)}")
        print(f"Intersecting files 2: {intersection(files1, files3)}")
        print(f"Intersecting files 3: {intersection(files2, files3)}")

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
    table_name = f"test_shards_distributed_{mode}_{processing_threads}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1000
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

    time.sleep(2)
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=row_num
    )

    def get_count(node, table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    def print_debug_info():
        processed_files = (
            node.query(
                f"""
select splitByChar('/', file_name)[-1] as file from system.s3queue_metadata_cache where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 order by file
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
select splitByChar('/', file_name)[-1] as file from system.s3queue_metadata_cache where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0 order by file
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
            where file not in (select splitByChar('/', file_name)[-1] from clusterAllReplicas(cluster, system.s3queue_metadata_cache)
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0)
            """
        )
        logging.debug(f"Unprocessed files: {info}")

        files1 = (
            node.query(
                f"""
            select splitByChar('/', file_name)[-1] from system.s3queue_metadata_cache
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0
            """
            )
            .strip()
            .split("\n")
        )
        files2 = (
            node_2.query(
                f"""
            select splitByChar('/', file_name)[-1] from system.s3queue_metadata_cache
            where zookeeper_path ilike '%{table_name}%' and status = 'Processed' and rows_processed > 0
            """
            )
            .strip()
            .split("\n")
        )

        def intersection(list_a, list_b):
            return [e for e in list_a if e in list_b]

        logging.debug(f"Intersecting files: {intersection(files1, files2)}")

    for _ in range(120):
        if (
            get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        ) == total_rows:
            break
        time.sleep(1)

    count1 = get_count(node, dst_table_name)
    count2 = get_count(node_2, dst_table_name)
    if (count1 + count2) != total_rows:
        expected_files = [f"{files_path}/test_{x}.csv" for x in range(files_to_generate)]
        node.query("SYSTEM FLUSH LOGS")
        node_2.query("SYSTEM FLUSH LOGS")
        processed_files = (
            node.query(
                f"SELECT distinct(_path) FROM clusterAllReplicas(cluster, default.{dst_table_name})"
            )
            .strip()
            .split("\n")
        )
        processed_files.sort()
        logging.debug(f"Processed files: {processed_files}")
        missing_files = [file for file in expected_files if file not in processed_files]
        missing_files.sort()

        assert (
            False
        ), f"Expected {total_rows} in total, got {count1} and {count2} ({count1 + count2}, having {len(missing_files)} missing files: ({missing_files})"


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


def _wait_for_lock_stat(zk, lock_path, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stat = zk.exists(lock_path)
        if stat is not None:
            return stat
        time.sleep(0.05)
    return None


def _setup_slow_ordered_bucket_lock_test(started_cluster, table_name, extra_settings):
    """Ordered + single bucket on two replicas, with a deliberately slow MV so the single
    bucket lock is held long enough for the cleanup (or the test) to remove it mid-flight.
    Returns (node1, node2, keeper_path, dst_table_name)."""
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    dst_table_name = f"{table_name}_dst"

    settings = {
        "keeper_path": keeper_path,
        "s3queue_buckets": 1,
        "s3queue_processing_threads_num": 1,
        # One commit per batch, so the lock is held for the whole (slow) batch with no heartbeat.
        "s3queue_max_processed_files_before_commit": 10000,
        "polling_min_timeout_ms": 100,
        "polling_max_timeout_ms": 100,
    }
    settings.update(extra_settings)

    for node in (node1, node2):
        create_table(
            started_cluster, node, table_name, "ordered", files_path,
            additional_settings=settings,
        )
        node.query(f"DROP TABLE IF EXISTS {dst_table_name}")
        node.query(f"DROP TABLE IF EXISTS {table_name}_mv")
        node.query(
            f"CREATE TABLE {dst_table_name} "
            f"(column1 UInt32, column2 UInt32, column3 UInt32) ENGINE = MergeTree ORDER BY column1"
        )
        # `sleepEachRow` keeps the bucket lock held while the batch is being processed.
        node.query(
            f"CREATE MATERIALIZED VIEW {table_name}_mv TO {dst_table_name} AS "
            f"SELECT column1, column2, column3 FROM {table_name} WHERE NOT sleepEachRow(0.1)"
        )
    return node1, node2, keeper_path, dst_table_name


def _assert_recovered_without_already_processed_error(
    started_cluster, table_name, node1, node2, dst_table_name, total_values
):
    expected = set(tuple(v) for v in total_values)

    def union_distinct():
        rows = set()
        for node in (node1, node2):
            for line in node.query(
                f"SELECT column1, column2, column3 FROM {dst_table_name}"
            ).splitlines():
                rows.add(tuple(map(int, line.split())))
        return rows

    for _ in range(120):
        if union_distinct() == expected:
            break
        time.sleep(1)

    # No data was lost: every file's rows ended up processed.
    assert union_distinct() == expected

    for node in (node1, node2):
        # The race must not produce the incident's `LOGICAL_ERROR`...
        assert not node.contains_in_log(
            "is already processed, while expected it not to be"
        )
        # ... no file ended up Failed ...
        assert (
            node.query(
                f"SELECT count() FROM system.s3queue_metadata_cache "
                f"WHERE zookeeper_path ILIKE '%{table_name}%' AND status = 'Failed'"
            ).strip()
            == "0"
        )
        # ... and the server is still responsive.
        assert node.query("SELECT 1").strip() == "1"


def test_ordered_bucket_lock_deleted_during_commit(started_cluster):
    # Option A: deterministically simulate the TTL cleanup removing a *live* bucket lock by
    # deleting it from Keeper while a slow consumer still holds it, so the other replica takes the
    # bucket over. The commit-time `czxid` ownership check must turn this into a clean retry
    # instead of the "File is already processed" LOGICAL_ERROR, with no data loss.
    table_name = f"test_lock_deleted_{generate_random_string()}"
    node1, node2, keeper_path, dst_table_name = _setup_slow_ordered_bucket_lock_test(
        started_cluster,
        table_name,
        # Large TTL: here the lock is removed by the test, not by the cleanup.
        {"s3queue_persistent_processing_node_ttl_seconds": 3600},
    )

    total_values = generate_random_files(
        started_cluster, f"{table_name}_data", 30, row_num=1
    )

    zk = started_cluster.get_kazoo_client("zoo1")
    lock_path = f"{keeper_path}/buckets/0/lock"

    stat_before = _wait_for_lock_stat(zk, lock_path)
    assert stat_before is not None, "bucket lock was never acquired"
    zk.delete(lock_path)

    # The race actually fired only if another replica re-acquired the bucket (new lock node).
    deadline = time.monotonic() + 60
    stat_after = None
    while time.monotonic() < deadline:
        stat_after = zk.exists(lock_path)
        if stat_after is not None and stat_after.czxid != stat_before.czxid:
            break
        time.sleep(0.05)
    assert (
        stat_after is not None and stat_after.czxid != stat_before.czxid
    ), "bucket was not taken over after its lock was deleted"

    _assert_recovered_without_already_processed_error(
        started_cluster, table_name, node1, node2, dst_table_name, total_values
    )


def test_ordered_bucket_lock_cleaned_by_ttl(started_cluster):
    # Option B: end-to-end — a small `persistent_processing_node_ttl_seconds` plus a slow consumer
    # lets the cleanup task remove a *live* bucket lock (the batch never commits before the TTL, so
    # there is no heartbeat), and the other replica takes over. Same guarantee as option A, but via
    # the real TTL cleanup path.
    table_name = f"test_lock_ttl_{generate_random_string()}"
    node1, node2, keeper_path, dst_table_name = _setup_slow_ordered_bucket_lock_test(
        started_cluster,
        table_name,
        {
            "s3queue_persistent_processing_node_ttl_seconds": 2,
            "s3queue_cleanup_interval_min_ms": 100,
            "s3queue_cleanup_interval_max_ms": 200,
        },
    )

    total_values = generate_random_files(
        started_cluster, f"{table_name}_data", 40, row_num=1
    )

    _assert_recovered_without_already_processed_error(
        started_cluster, table_name, node1, node2, dst_table_name, total_values
    )

    # The cleanup must actually have removed a stale node for this test to be meaningful.
    assert node1.contains_in_log("Removing stale processing node") or node2.contains_in_log(
        "Removing stale processing node"
    )

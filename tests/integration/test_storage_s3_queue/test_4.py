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
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_cloud_mode",
            with_zookeeper=True,
            stay_alive=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            user_configs=["configs/cloud_mode.xml"],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_replicated(started_cluster):
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_replicated_{uuid.uuid4().hex[:8]}"
    mv_name = f"{table_name}_mv"
    db_name = f"r"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1000

    node1.query(f"DROP DATABASE IF EXISTS {db_name}")
    node2.query(f"DROP DATABASE IF EXISTS {db_name}")

    node1.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/clickhouse/databases/replicateddb', 'shard1', 'node1')"
    )
    node2.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/clickhouse/databases/replicateddb', 'shard1', 'node2')"
    )

    create_table(
        started_cluster,
        node1,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "processing_threads_num": 16,
            "keeper_path": keeper_path,
        },
        database_name="r",
    )

    assert '"processing_threads_num":16' in node1.query(
        f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
    )

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(
        node1,
        f"{db_name}.{table_name}",
        f"{db_name}.{dst_table_name}",
        mv_name=f"{db_name}.{mv_name}",
    )

    def get_count():
        return int(
            node1.query(
                f"SELECT count() FROM clusterAllReplicas(cluster, {db_name}.{dst_table_name})"
            )
        )

    expected_rows = files_to_generate
    for _ in range(100):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()


def test_bad_settings(started_cluster):
    node = started_cluster.instances["node_cloud_mode"]

    table_name = f"test_bad_settings_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    try:
        create_table(
            started_cluster,
            node,
            table_name,
            "ordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "processing_threads_num": 1,
                "buckets": 0,
            },
        )
        assert False
    except Exception as e:
        assert "Ordered mode in cloud without either" in str(e)


def test_processing_threads(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_processing_threads_{uuid.uuid4().hex[:8]}"
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
        additional_settings={
            "processing_threads_num": 16,
            "keeper_path": keeper_path,
        },
    )

    assert '"processing_threads_num":16' in node.query(
        f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
    )

    assert 16 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'processing_threads_num'"
        )
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

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Using 16 processing threads"
    )


def test_alter_settings(started_cluster):
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_alter_settings_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1000

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
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 10,
            "s3queue_loading_retries": 20,
            "s3queue_tracked_files_limit": 2000,
            "s3queue_polling_max_timeout_ms": 1000,
        },
        database_name="r",
    )

    assert '"processing_threads_num":10' in node1.query(
        f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
    )

    assert '"loading_retries":20' in node1.query(
        f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
    )

    assert '"after_processing":"keep"' in node1.query(
        f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
    )

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node1, f"r.{table_name}", f"r.{dst_table_name}", mv_name=f"r.{mv_name}")

    def get_count():
        return int(
            node1.query(
                f"SELECT count() FROM clusterAllReplicas(cluster, r.{dst_table_name})"
            )
        )

    expected_rows = files_to_generate
    for _ in range(100):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()

    assert (
        "true"
        == node1.query(
            f"SELECT value FROM system.s3_queue_settings WHERE name = 'enable_hash_ring_filtering' and table = '{table_name}'"
        ).strip()
    )

    node1.query(
        f"""
        ALTER TABLE r.{table_name}
        MODIFY SETTING processing_threads_num=5,
        loading_retries=44,
        after_processing='delete',
        tracked_files_limit=50,
        tracked_file_ttl_sec=10000,
        polling_min_timeout_ms=222,
        s3queue_polling_max_timeout_ms=333,
        polling_backoff_ms=111,
        max_processed_files_before_commit=444,
        s3queue_max_processed_rows_before_commit=555,
        max_processed_bytes_before_commit=666,
        max_processing_time_sec_before_commit=777,
        enable_hash_ring_filtering=false,
        list_objects_batch_size=1234
    """
    )

    int_settings = {
        "processing_threads_num": 5,
        "loading_retries": 44,
        "tracked_files_ttl_sec": 10000,
        "tracked_files_limit": 50,
        "polling_min_timeout_ms": 222,
        "polling_max_timeout_ms": 333,
        "polling_backoff_ms": 111,
        "max_processed_files_before_commit": 444,
        "max_processed_rows_before_commit": 555,
        "max_processed_bytes_before_commit": 666,
        "max_processing_time_sec_before_commit": 777,
        "enable_hash_ring_filtering": "false",
        "list_objects_batch_size": 1234,
    }
    string_settings = {"after_processing": "delete"}

    def check_alterable(setting):
        if setting.startswith("s3queue_"):
            name = setting[len("s3queue_"):]
        else:
            name = setting
        if name == "tracked_files_ttl_sec":
            name = "tracked_file_ttl_sec" # sadly
        assert 1 == int(node1.query(f"select alterable from system.s3_queue_settings where name = '{name}'"))

    for setting, _ in int_settings.items():
        check_alterable(setting)

    for setting, _ in string_settings.items():
        check_alterable(setting)

    assert 0 == int(node1.query(f"select alterable from system.s3_queue_settings where name = 'mode'"))

    def with_keeper(setting):
        return setting in {
            "after_processing",
            "loading_retries",
            "processing_threads_num",
            "tracked_files_limit",
            "tracked_files_ttl_sec",
        }

    def check_int_settings(node, settings):
        for setting, value in settings.items():
            if with_keeper(setting):
                assert f'"{setting}":{value}' in node.query(
                    f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
                )
            if setting == "tracked_files_ttl_sec":
                setting = "tracked_file_ttl_sec"
            assert (
                str(value)
                == node.query(
                    f"SELECT value FROM system.s3_queue_settings WHERE name = '{setting}' and table = '{table_name}'"
                ).strip()
            )

    def check_string_settings(node, settings):
        for setting, value in settings.items():
            if with_keeper(setting):
                assert f'"{setting}":"{value}"' in node.query(
                    f"SELECT * FROM system.zookeeper WHERE path = '{keeper_path}'"
                )
            assert (
                str(value)
                == node.query(
                    f"SELECT value FROM system.s3_queue_settings WHERE name = '{setting}' and table = '{table_name}'"
                ).strip()
            )

    for node in [node1, node2]:
        check_int_settings(node, int_settings)
        check_string_settings(node, string_settings)

        node.restart_clickhouse()

        check_int_settings(node, int_settings)
        check_string_settings(node, string_settings)

    node1.query(
        f"""
        ALTER TABLE r.{table_name} RESET SETTING after_processing, tracked_file_ttl_sec, loading_retries, s3queue_tracked_files_limit
    """
    )

    int_settings = {
        "processing_threads_num": 5,
        "loading_retries": 10,
        "tracked_files_ttl_sec": 0,
        "tracked_files_limit": 1000,
    }
    string_settings = {"after_processing": "keep"}

    for node in [node1, node2]:
        check_int_settings(node, int_settings)
        check_string_settings(node, string_settings)

        node.restart_clickhouse()

        check_int_settings(node, int_settings)
        check_string_settings(node, string_settings)


def test_list_and_delete_race(started_cluster):
    node = started_cluster.instances["instance"]
    if node.is_built_with_sanitizer():
        # Issue does not reproduce under sanitizer
        return
    node_2 = started_cluster.instances["instance2"]
    table_name = f"list_and_delete_race_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1000
    row_num = 1

    for instance in [node, node_2]:
        create_table(
            started_cluster,
            instance,
            table_name,
            "unordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "tracked_files_limit": 1,
                "polling_max_timeout_ms": 0,
                "processing_threads_num": 1,
                "polling_min_timeout_ms": 200,
                "cleanup_interval_min_ms": 0,
                "cleanup_interval_max_ms": 0,
                "polling_backoff_ms": 100,
                "after_processing": "delete",
            },
        )

    threads = 10
    total_rows = row_num * files_to_generate * (threads + 1)

    busy_pool = Pool(threads)

    def generate(_):
        generate_random_files(
            started_cluster,
            files_path,
            files_to_generate,
            row_num=row_num,
            use_random_names=True,
        )

    generate(0)

    p = busy_pool.map_async(generate, range(threads))

    create_mv(node, table_name, dst_table_name)
    time.sleep(2)
    create_mv(node_2, table_name, dst_table_name)

    p.wait()

    def get_count(node, table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(150):
        if (
            get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        ) == total_rows:
            break
        time.sleep(1)

    assert (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        == total_rows
    )

    get_query = f"SELECT column1, column2, column3 FROM {dst_table_name}"
    res1 = [list(map(int, l.split())) for l in run_query(node, get_query).splitlines()]
    res2 = [
        list(map(int, l.split())) for l in run_query(node_2, get_query).splitlines()
    ]

    logging.debug(
        f"res1 size: {len(res1)}, res2 size: {len(res2)}, total_rows: {total_rows}"
    )

    assert len(res1) + len(res2) == total_rows
    # (kssenii) I will fix this assert a tiny bit later
    # assert (
    #    node.contains_in_log("because of the race with list & delete")
    #    or node_2.contains_in_log("because of the race with list & delete")
    #    or node.contains_in_log(
    #        f"StorageS3Queue (default.{table_name}): Skipping file"  # Unfortunately this optimization makes the race less easy to catch.
    #    )
    #    or node_2.contains_in_log(
    #        f"StorageS3Queue (default.{table_name}): Skipping file"
    #    )
    # )


def test_registry(started_cluster):
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_registry_{uuid.uuid4().hex[:8]}"
    db_name = f"db_{table_name}"
    mv_name = f"{table_name}_mv"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1000

    node1.query(f"DROP DATABASE IF EXISTS {db_name}")
    node2.query(f"DROP DATABASE IF EXISTS {db_name}")

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
        "ordered",
        files_path,
        additional_settings={"keeper_path": keeper_path, "buckets": 3},
        database_name=db_name,
    )

    zk = started_cluster.get_kazoo_client("zoo1")
    registry, stat = zk.get(f"{keeper_path}/registry/")

    uuid1 = node1.query(
        f"SELECT uuid FROM system.tables WHERE database = '{db_name}' and table = '{table_name}'"
    ).strip()
    assert uuid1 in str(registry)

    expected = [f"0\\ninstance\\n{uuid1}\\n", f"0\\ninstance2\\n{uuid1}\\n"]

    for elem in expected:
        assert elem in str(registry)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(
        node1,
        f"{db_name}.{table_name}",
        f"{db_name}.{dst_table_name}",
        mv_name=f"{db_name}.{mv_name}",
    )

    def get_count():
        return int(
            node1.query(
                f"SELECT count() FROM clusterAllReplicas(cluster, {db_name}.{dst_table_name})"
            )
        )

    expected_rows = files_to_generate
    for _ in range(100):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()

    table_name_2 = f"test_registry_{uuid.uuid4().hex[:8]}_2"
    create_table(
        started_cluster,
        node1,
        table_name_2,
        "ordered",
        files_path,
        additional_settings={"keeper_path": keeper_path, "buckets": 3},
        database_name=db_name,
    )

    registry, stat = zk.get(f"{keeper_path}/registry/")

    uuid2 = node1.query(
        f"SELECT uuid FROM system.tables WHERE database = '{db_name}' and table = '{table_name_2}'"
    ).strip()

    assert uuid1 in str(registry)
    assert uuid2 in str(registry)

    expected = [
        f"0\\ninstance\\n{uuid1}\\n",
        f"0\\ninstance2\\n{uuid1}\\n",
        f"0\\ninstance\\n{uuid2}\\n",
        f"0\\ninstance2\\n{uuid2}\\n",
    ]

    for elem in expected:
        assert elem in str(registry)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    registry, stat = zk.get(f"{keeper_path}/registry/")

    assert uuid1 in str(registry)
    assert uuid2 in str(registry)

    node1.query(f"DROP TABLE {db_name}.{table_name_2} SYNC")

    assert zk.exists(keeper_path) is not None
    registry, stat = zk.get(f"{keeper_path}/registry/")

    assert uuid1 in str(registry)
    assert uuid2 not in str(registry)

    expected = [
        f"0\\ninstance\\n{uuid1}\\n",
        f"0\\ninstance2\\n{uuid1}\\n",
    ]

    for elem in expected:
        assert elem in str(registry)

    node1.query(f"DROP TABLE {db_name}.{table_name} SYNC")

    assert zk.exists(keeper_path) is None

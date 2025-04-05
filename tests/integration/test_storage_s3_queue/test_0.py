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


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
@pytest.mark.parametrize("engine_name", ["S3Queue", "AzureQueue"])
def test_delete_after_processing(started_cluster, mode, engine_name):
    node = started_cluster.instances["instance"]
    table_name = (
        f"delete_after_processing_{mode}_{engine_name}_{generate_random_string()}"
    )
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    files_num = 5
    row_num = 10
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    if engine_name == "S3Queue":
        storage = "s3"
    else:
        storage = "azure"

    total_values = generate_random_files(
        started_cluster, files_path, files_num, row_num=row_num, storage=storage
    )
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={"after_processing": "delete", "keeper_path": keeper_path},
        engine_name=engine_name,
    )
    create_mv(node, table_name, dst_table_name)

    expected_count = files_num * row_num
    for _ in range(100):
        count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
        print(f"{count}/{expected_count}")
        if count == expected_count:
            break
        time.sleep(1)

    assert int(node.query(f"SELECT count() FROM {dst_table_name}")) == expected_count
    assert int(node.query(f"SELECT uniq(_path) FROM {dst_table_name}")) == files_num
    assert [
        list(map(int, l.split()))
        for l in node.query(
            f"SELECT column1, column2, column3 FROM {dst_table_name} ORDER BY column1, column2, column3"
        ).splitlines()
    ] == sorted(total_values, key=lambda x: (x[0], x[1], x[2]))

    node.query("system flush logs")

    if engine_name == "S3Queue":
        system_tables = ["s3queue_log", "s3queue"]
    else:
        system_tables = ["azure_queue_log", "azure_queue"]

    for table in system_tables:
        if table.endswith("_log"):
            assert (
                int(
                    node.query(
                        f"SELECT sum(rows_processed) FROM system.{table} WHERE table = '{table_name}'"
                    )
                )
                == files_num * row_num
            )
        else:
            assert (
                int(
                    node.query(
                        f"SELECT sum(rows_processed) FROM system.{table} WHERE zookeeper_path = '{keeper_path}'"
                    )
                )
                == files_num * row_num
            )

    if engine_name == "S3Queue":
        minio = started_cluster.minio_client
        objects = list(minio.list_objects(started_cluster.minio_bucket, recursive=True))
        assert len(objects) == 0
    else:
        client = started_cluster.blob_service_client.get_container_client(
            started_cluster.azurite_container
        )
        objects_iterator = client.list_blobs(files_path)
        for objects in objects_iterator:
            assert False


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
@pytest.mark.parametrize("engine_name", ["S3Queue", "AzureQueue"])
def test_failed_retry(started_cluster, mode, engine_name):
    node = started_cluster.instances["instance"]
    table_name = f"failed_retry_{mode}_{engine_name}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    file_path = f"{files_path}/trash_test.csv"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    retries_num = 3

    values = [
        ["failed", 1, 1],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    if engine_name == "S3Queue":
        put_s3_file_content(started_cluster, file_path, values_csv)
    else:
        put_azure_file_content(started_cluster, file_path, values_csv)

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "s3queue_loading_retries": retries_num,
            "keeper_path": keeper_path,
            "polling_max_timeout_ms": 5000,
            "polling_backoff_ms": 1000,
        },
        engine_name=engine_name,
    )
    create_mv(node, table_name, dst_table_name)

    failed_node_path = ""
    for _ in range(20):
        zk = started_cluster.get_kazoo_client("zoo1")
        failed_nodes = zk.get_children(f"{keeper_path}/failed/")
        if len(failed_nodes) > 0:
            assert len(failed_nodes) == 1
            failed_node_path = f"{keeper_path}/failed/{failed_nodes[0]}"
        time.sleep(1)

    assert failed_node_path != ""

    retries = 0
    for _ in range(20):
        data, stat = zk.get(failed_node_path)
        json_data = json.loads(data)
        print(f"Failed node metadata: {json_data}")
        assert json_data["file_path"] == file_path
        retries = int(json_data["retries"])
        if retries == retries_num:
            break
        time.sleep(1)

    assert retries == retries_num
    assert 0 == int(node.query(f"SELECT count() FROM {dst_table_name}"))


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_direct_select_file(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"direct_select_file_{mode}"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{mode}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    file_path = f"{files_path}/test.csv"

    values = [
        [12549, 2463, 19893],
        [64021, 38652, 66703],
        [81611, 39650, 83516],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    put_s3_file_content(started_cluster, file_path, values_csv)

    for i in range(3):
        create_table(
            started_cluster,
            node,
            f"{table_name}_{i + 1}",
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_processing_threads_num": 1,
                "enable_hash_ring_filtering": 0,
            },
        )

    assert [
        list(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}_1").splitlines()
    ] == values

    assert [
        list(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}_2").splitlines()
    ] == []

    assert [
        list(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}_3").splitlines()
    ] == []

    # New table with same zookeeper path
    create_table(
        started_cluster,
        node,
        f"{table_name}_4",
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
        },
    )

    assert [
        list(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}_4").splitlines()
    ] == []

    # New table with different zookeeper path
    keeper_path = f"{keeper_path}_2"
    create_table(
        started_cluster,
        node,
        f"{table_name}_4",
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 1,
        },
    )

    assert [
        list(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}_4").splitlines()
    ] == values

    values = [
        [1, 1, 1],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    file_path = f"{files_path}/t.csv"
    put_s3_file_content(started_cluster, file_path, values_csv)

    if mode == "unordered":
        assert [
            list(map(int, l.split()))
            for l in node.query(f"SELECT * FROM {table_name}_4").splitlines()
        ] == values
    elif mode == "ordered":
        assert [
            list(map(int, l.split()))
            for l in node.query(f"SELECT * FROM {table_name}_4").splitlines()
        ] == []


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_direct_select_multiple_files(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"direct_select_multiple_files_{mode}"
    files_path = f"{table_name}_data"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={"keeper_path": keeper_path, "processing_threads_num": 3},
    )
    for i in range(5):
        rand_values = [[random.randint(0, 50) for _ in range(3)] for _ in range(10)]
        values_csv = (
            "\n".join((",".join(map(str, row)) for row in rand_values)) + "\n"
        ).encode()

        file_path = f"{files_path}/test_{i}.csv"
        put_s3_file_content(started_cluster, file_path, values_csv)

        assert [
            list(map(int, l.split()))
            for l in node.query(f"SELECT * FROM {table_name}").splitlines()
        ] == rand_values

    total_values = generate_random_files(started_cluster, files_path, 4, start_ind=5)
    assert {
        tuple(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}").splitlines()
    } == set([tuple(i) for i in total_values])


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_streaming_to_view(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"streaming_to_view_{mode}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"

    total_values = generate_random_files(started_cluster, files_path, 10)
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )
    create_mv(node, table_name, dst_table_name)

    expected_values = set([tuple(i) for i in total_values])
    for i in range(10):
        selected_values = {
            tuple(map(int, l.split()))
            for l in node.query(
                f"SELECT column1, column2, column3 FROM {dst_table_name}"
            ).splitlines()
        }
        if selected_values == expected_values:
            break
        time.sleep(1)
    assert selected_values == expected_values


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_streaming_to_many_views(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"streaming_to_many_views_{mode}"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    loading_retries = 2
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 100,
            "s3queue_loading_retries": loading_retries,
            "polling_backoff_ms": 0,
        },
    )

    tables_num = 10
    mv_tables = [f"{table_name}_{i + 1}_mv" for i in range(tables_num)]
    dst_tables = [f"{table_name}_{i + 1}_dst" for i in range(tables_num)]

    for i in range(tables_num):
        create_mv(node, table_name, dst_tables[i], mv_name=mv_tables[i])

    start_idx = [0]
    expect_files_num = [0]
    expect_rows_num = [0]
    files = []

    def generate_files(files_num=20, row_num=100, file_prefix = "a"):
        files.extend(
            [
                (f"{files_path}/{file_prefix}_{i}.csv", i)
                for i in range(start_idx[0], start_idx[0] + files_num)
            ]
        )
        generate_random_files(
            started_cluster, files_path, files_num, files=files, row_num=row_num
        )
        start_idx[0] += files_num
        expect_files_num[0] += files_num
        expect_rows_num[0] += files_num * row_num

    def check(dst_tables, expect_rows_num, expect_files_num):
        for dst_table_name in dst_tables:

            def get_uniq_paths_count():
                return int(node.query(f"SELECT uniqExact(_path) FROM {dst_table_name}"))

            for _ in range(20):
                if get_uniq_paths_count() == expect_files_num:
                    break
                time.sleep(1)

            processed_files = node.query(
                f"SELECT distinct(_path) FROM {dst_table_name}"
            )
            missing_files = [
                x[0] for x in files if x[0] not in processed_files.strip().split("\n")
            ]
            assert (
                get_uniq_paths_count() == expect_files_num
            ), f"Missing files for {dst_table_name}: {missing_files}"

            def get_rows_count():
                return int(node.query(f"SELECT count() FROM {dst_table_name}"))

            for _ in range(20):
                if get_rows_count() == expect_rows_num:
                    break
                time.sleep(1)

            rows_count = get_rows_count()
            assert (
                expect_rows_num == rows_count
            ), f"Missing or extra data for {dst_table_name}: {rows_count} (expected: {expect_rows_num})"

    generate_files()
    check(dst_tables, expect_rows_num[0], expect_files_num[0])

    # Create incorrect destination table
    broken_dst_table = f"{table_name}_{expect_files_num[0] + 1}_dst"
    create_mv(
        node,
        table_name,
        broken_dst_table,
        mv_name=f"{table_name}_{expect_files_num[0] + 1}_mv",
        format="column1 String, column2 JSON",
        create_dst_table_first=False
    )

    generate_files(file_prefix = "b")
    # there is no gurantee what is inserted to other MV because the insert is failed
    check([broken_dst_table], 0, 0)

    for i in range(20, 40):
        log_message = f"File {files_path}/b_{i}.csv failed at try 2/2, retries node exists: true"
        assert node.contains_in_log(
            log_message
        ), f"Cannot find log message 1 for path {files_path}/b_{i}.csv: {log_message}"
        log_message = (
            f"File {files_path}/b_{i}.csv failed to process and will not be retried"
        )
        assert node.contains_in_log(
            log_message
        ), f"Cannot find log message 2 for path {files_path}/b_{i}.csv: {log_message}"


def test_multiple_tables_meta_mismatch(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"multiple_tables_meta_mismatch"
    # A unique path is necessary for repeatable tests
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
    )
    # check mode
    failed = False
    try:
        create_table(
            started_cluster,
            node,
            f"{table_name}_copy",
            "unordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
            },
        )
    except QueryRuntimeException as e:
        assert "Existing table metadata in ZooKeeper differs in engine mode" in str(e)
        failed = True

    assert failed is True

    # check columns
    try:
        create_table(
            started_cluster,
            node,
            f"{table_name}_copy",
            "ordered",
            files_path,
            format="column1 UInt32, column2 UInt32, column3 UInt32, column4 UInt32",
            additional_settings={
                "keeper_path": keeper_path,
            },
        )
    except QueryRuntimeException as e:
        assert "Existing table metadata in ZooKeeper differs in columns" in str(e)
        failed = True

    assert failed is True

    # check format
    try:
        create_table(
            started_cluster,
            node,
            f"{table_name}_copy",
            "ordered",
            files_path,
            format="column1 UInt32, column2 UInt32, column3 UInt32, column4 UInt32",
            additional_settings={
                "keeper_path": keeper_path,
            },
            file_format="TSV",
        )
    except QueryRuntimeException as e:
        assert "Existing table metadata in ZooKeeper differs in format name" in str(e)
        failed = True

    assert failed is True

    # create working engine
    create_table(
        started_cluster,
        node,
        f"{table_name}_copy",
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
    )

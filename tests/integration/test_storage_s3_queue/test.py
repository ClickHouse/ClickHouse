import io
import logging
import os
import random
import time

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
import json

"""
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/home/sergey/vkr/ClickHouse/build/programs/clickhouse-server
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/home/sergey/vkr/ClickHouse/build/programs/clickhouse-client
export CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH=/home/sergey/vkr/ClickHouse/build/programs/clickhouse-odbc-bridge
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/home/sergey/vkr/ClickHouse/programs/server

"""

MINIO_INTERNAL_PORT = 9001
AVAILABLE_MODES = ["unordered", "ordered"]
AUTH = "'minio','minio123',"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def prepare_s3_bucket(started_cluster):
    # Allows read-write access for bucket without authorization.
    bucket_read_write_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::root/*",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::root/*",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:DeleteObject",
                "Resource": "arn:aws:s3:::root/*",
            },
        ],
    }

    minio_client = started_cluster.minio_client
    minio_client.set_bucket_policy(
        started_cluster.minio_bucket, json.dumps(bucket_read_write_policy)
    )

    started_cluster.minio_restricted_bucket = "{}-with-auth".format(
        started_cluster.minio_bucket
    )
    if minio_client.bucket_exists(started_cluster.minio_restricted_bucket):
        minio_client.remove_bucket(started_cluster.minio_restricted_bucket)

    minio_client.make_bucket(started_cluster.minio_restricted_bucket)


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance_2 = started_cluster.instances["instance2"]

    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")
    instance_2.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")

    minio = started_cluster.minio_client
    objects = list(
        minio.list_objects(started_cluster.minio_restricted_bucket, recursive=True)
    )
    for obj in objects:
        minio.remove_object(started_cluster.minio_restricted_bucket, obj.object_name)
    yield  # run test


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/defaultS3.xml",
                "configs/named_collections.xml",
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
        )
        cluster.add_instance(
            "instance2",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/defaultS3.xml",
                "configs/named_collections.xml",
                "configs/s3queue_log.xml",
            ],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def generate_random_files(
    started_cluster, files_path, count, column_num=3, row_num=10, start_ind=0
):
    files = [
        (f"{files_path}/test_{i}.csv", i) for i in range(start_ind, start_ind + count)
    ]
    files.sort(key=lambda x: x[0])

    print(f"Generating files: {files}")

    total_values = []
    for filename, i in files:
        rand_values = [
            [random.randint(0, 1000) for _ in range(column_num)] for _ in range(row_num)
        ]
        total_values += rand_values
        values_csv = (
            "\n".join((",".join(map(str, row)) for row in rand_values)) + "\n"
        ).encode()
        put_s3_file_content(started_cluster, filename, values_csv)
    return total_values


def put_s3_file_content(started_cluster, filename, data):
    buf = io.BytesIO(data)
    started_cluster.minio_client.put_object(
        started_cluster.minio_bucket, filename, buf, len(data)
    )


def get_s3_file_content(started_cluster, bucket, filename, decode=True):
    # type: (ClickHouseCluster, str, str, bool) -> str
    # Returns content of given S3 file as string.

    data = started_cluster.minio_client.get_object(bucket, filename)
    data_str = b""
    for chunk in data.stream():
        data_str += chunk
    if decode:
        return data_str.decode()
    return data_str


def create_table(
    started_cluster,
    node,
    table_name,
    mode,
    files_path,
    format="column1 UInt32, column2 UInt32, column3 UInt32",
    additional_settings={},
    file_format="CSV",
):
    settings = {
        "s3queue_loading_retries": 0,
        "after_processing": "keep",
        "keeper_path": f"/clickhouse/test_{table_name}",
        "mode": f"{mode}",
    }
    settings.update(additional_settings)

    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{files_path}/"
    node.query(f"DROP TABLE IF EXISTS {table_name}")
    create_query = f"""
        CREATE TABLE {table_name} ({format})
        ENGINE = S3Queue('{url}', {AUTH}'{file_format}')
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
        """
    node.query(create_query)


def create_mv(
    node,
    src_table_name,
    dst_table_name,
    format="column1 UInt32, column2 UInt32, column3 UInt32",
):
    mv_name = f"{dst_table_name}_mv"
    node.query(
        f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {mv_name};

        CREATE TABLE {dst_table_name} ({format}, _path String)
        ENGINE = MergeTree()
        ORDER BY column1;

        CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT *, _path FROM {src_table_name};
        """
    )


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_delete_after_processing(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"test.delete_after_processing_{mode}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    files_num = 5
    row_num = 10

    total_values = generate_random_files(
        started_cluster, files_path, files_num, row_num=row_num
    )
    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={"after_processing": "delete"},
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

    minio = started_cluster.minio_client
    objects = list(minio.list_objects(started_cluster.minio_bucket, recursive=True))
    assert len(objects) == 0


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_failed_retry(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"test.failed_retry_{mode}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    file_path = f"{files_path}/trash_test.csv"
    keeper_path = f"/clickhouse/test_{table_name}"
    retries_num = 3

    values = [
        ["failed", 1, 1],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    put_s3_file_content(started_cluster, file_path, values_csv)

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "s3queue_loading_retries": retries_num,
            "keeper_path": keeper_path,
        },
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
    table_name = f"test.direct_select_file_{mode}"
    keeper_path = f"/clickhouse/test_{table_name}"
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
        },
    )

    assert [
        list(map(int, l.split()))
        for l in node.query(f"SELECT * FROM {table_name}_4").splitlines()
    ] == []

    # New table with different zookeeper path
    keeper_path = f"/clickhouse/test_{table_name}_{mode}_2"
    create_table(
        started_cluster,
        node,
        f"{table_name}_4",
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
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

    create_table(started_cluster, node, table_name, mode, files_path)
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
def test_streaming_to_view_(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"streaming_to_view_{mode}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"

    total_values = generate_random_files(started_cluster, files_path, 10)
    create_table(started_cluster, node, table_name, mode, files_path)
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
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    for i in range(3):
        table = f"{table_name}_{i + 1}"
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
        create_mv(node, table, dst_table_name)

    total_values = generate_random_files(started_cluster, files_path, 5)
    expected_values = set([tuple(i) for i in total_values])

    def select():
        return {
            tuple(map(int, l.split()))
            for l in node.query(
                f"SELECT column1, column2, column3 FROM {dst_table_name}"
            ).splitlines()
        }

    for _ in range(20):
        if select() == expected_values:
            break
        time.sleep(1)
    assert select() == expected_values


def test_multiple_tables_meta_mismatch(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"multiple_tables_meta_mismatch"
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
        assert (
            "Metadata with the same `s3queue_zookeeper_path` was already created but with different settings"
            in str(e)
        )
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
        assert (
            "Table columns structure in ZooKeeper is different from local table structure"
            in str(e)
        )
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


# TODO: Update the modes for this test to include "ordered" once PR #55795 is finished.
@pytest.mark.parametrize("mode", ["unordered"])
def test_multiple_tables_streaming_sync(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"multiple_tables_streaming_sync_{mode}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
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
    table_name = f"multiple_tables_streaming_sync_distributed_{mode}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 300

    for instance in [node, node_2]:
        create_table(
            started_cluster,
            instance,
            table_name,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
            },
        )

    for instance in [node, node_2]:
        create_mv(instance, table_name, dst_table_name)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=1
    )

    def get_count(node, table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(150):
        if (
            get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
        ) == files_to_generate:
            break
        time.sleep(1)

    if (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) != files_to_generate:
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

    assert len(res1) + len(res2) == files_to_generate

    # Checking that all engines have made progress
    assert len(res1) > 0
    assert len(res2) > 0

    assert {tuple(v) for v in res1 + res2} == set([tuple(i) for i in total_values])

    # Checking that all files were processed only once
    time.sleep(10)
    assert (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) == files_to_generate


def test_max_set_age(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"max_set_age"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    max_age = 10
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_tracked_file_ttl_sec": max_age,
            "s3queue_cleanup_interval_min_ms": 0,
            "s3queue_cleanup_interval_max_ms": 0,
        },
    )
    create_mv(node, table_name, dst_table_name)

    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, row_num=1
    )

    expected_rows = 10

    node.wait_for_log_line("Checking node limits")
    node.wait_for_log_line("Node limits check finished")

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()
    assert 10 == int(node.query(f"SELECT uniq(_path) from {dst_table_name}"))

    time.sleep(max_age + 1)

    expected_rows = 20

    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()
    assert 10 == int(node.query(f"SELECT uniq(_path) from {dst_table_name}"))

    paths_count = [
        int(x)
        for x in node.query(
            f"SELECT count() from {dst_table_name} GROUP BY _path"
        ).splitlines()
    ]
    assert 10 == len(paths_count)
    for path_count in paths_count:
        assert 2 == path_count


def test_max_set_size(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"max_set_size"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    max_age = 10
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
    keeper_path = f"/clickhouse/test_{table_name}"
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
    node.wait_for_log_line(f"Reading from file: test_drop_data")
    node.query(f"DROP TABLE {table_name} SYNC")
    assert node.contains_in_log(
        f"StorageS3Queue ({table_name}): Table is being dropped"
    )

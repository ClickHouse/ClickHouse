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
            ],
        )
        cluster.add_instance(
            "instance2",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=["configs/defaultS3.xml", "configs/named_collections.xml"],
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
        ENGINE = S3Queue('{url}', {AUTH}'CSV')
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
    files_path = f"test_meta"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
            SETTINGS
                mode = 'ordered',
                keeper_path = '/clickhouse/test_meta';
        """
    )
    # check mode
    failed = False
    try:
        instance.query(
            f"""
            CREATE TABLE test.s3_queue_copy ({table_format})
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
                SETTINGS
                    mode = 'unordered',
                    keeper_path = '/clickhouse/test_meta';
            """
        )
    except QueryRuntimeException as e:
        assert "Existing table metadata in ZooKeeper differs in engine mode" in str(e)
        failed = True
    assert failed is True

    # check columns
    table_format_copy = table_format + ", column4 UInt32"
    try:
        instance.query(
            f"""
            CREATE TABLE test.s3_queue_copy ({table_format_copy})
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
                SETTINGS
                    mode = 'ordered',
                    keeper_path = '/clickhouse/test_meta';
            """
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
        instance.query(
            f"""
            CREATE TABLE test.s3_queue_copy ({table_format})
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'TSV')
                SETTINGS
                    mode = 'ordered',
                    keeper_path = '/clickhouse/test_meta';
            """
        )
    except QueryRuntimeException as e:
        assert "Existing table metadata in ZooKeeper differs in format name" in str(e)
        failed = True
    assert failed is True

    # create working engine
    instance.query(
        f"""
            CREATE TABLE test.s3_queue_copy ({table_format})
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
                SETTINGS
                    mode = 'ordered',
                    keeper_path = '/clickhouse/test_meta';
            """
    )


def test_max_set_age(started_cluster):
    files_to_generate = 10
    max_age = 1
    files_path = f"test_multiple"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
            SETTINGS
                mode = 'unordered',
                keeper_path = '/clickhouse/test_set_age',
                s3queue_tracked_files_limit = 10,
                s3queue_tracked_file_ttl_sec = {max_age};
        """
    )

    total_values = generate_random_files(
        files_to_generate, files_path, started_cluster, bucket, row_num=1
    )
    get_query = f"SELECT * FROM test.s3_queue"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    assert res1 == total_values
    time.sleep(max_age + 1)

    get_query = f"SELECT * FROM test.s3_queue"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    assert res1 == total_values


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_multiple_tables_streaming_sync(started_cluster, mode):
    files_to_generate = 300
    poll_size = 30
    files_path = f"test_multiple_{mode}"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue;
        DROP TABLE IF EXISTS test.s3_queue_copy;
        DROP TABLE IF EXISTS test.s3_queue_copy_2;

        DROP TABLE IF EXISTS test.s3_queue_persistent;
        DROP TABLE IF EXISTS test.s3_queue_persistent_copy;
        DROP TABLE IF EXISTS test.s3_queue_persistent_copy_2;

        DROP TABLE IF EXISTS test.persistent_s3_queue_mv;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv_copy;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv_copy_2;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
            SETTINGS
                mode = '{mode}',
                keeper_path = '/clickhouse/test_multiple_consumers_sync_{mode}',
                s3queue_polling_size = {poll_size};

        CREATE TABLE test.s3_queue_copy ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
            SETTINGS
                mode = '{mode}',
                keeper_path = '/clickhouse/test_multiple_consumers_sync_{mode}',
                s3queue_polling_size = {poll_size};

        CREATE TABLE test.s3_queue_copy_2 ({table_format})
        ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
        SETTINGS
            mode = '{mode}',
            keeper_path = '/clickhouse/test_multiple_consumers_sync_{mode}',
            s3queue_polling_size = {poll_size};

        CREATE TABLE test.s3_queue_persistent ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;

        CREATE TABLE test.s3_queue_persistent_copy ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;

        CREATE TABLE test.s3_queue_persistent_copy_2 ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;

        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv TO test.s3_queue_persistent AS
        SELECT
            *
        FROM test.s3_queue;

        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv_copy TO test.s3_queue_persistent_copy AS
        SELECT
            *
        FROM test.s3_queue_copy;

        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv_copy_2 TO test.s3_queue_persistent_copy_2 AS
        SELECT
            *
        FROM test.s3_queue_copy_2;
        """
    )
    total_values = generate_random_files(
        files_to_generate, files_path, started_cluster, bucket, row_num=1
    )

    def get_count(table_name):
        return int(run_query(instance, f"SELECT count() FROM {table_name}"))

    for _ in range(100):
        if (
            get_count("test.s3_queue_persistent")
            + get_count("test.s3_queue_persistent_copy")
            + get_count("test.s3_queue_persistent_copy_2")
        ) == files_to_generate:
            break
        time.sleep(1)

    get_query = f"SELECT * FROM test.s3_queue_persistent"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    get_query_copy = f"SELECT * FROM test.s3_queue_persistent_copy"
    res2 = [
        list(map(int, l.split()))
        for l in run_query(instance, get_query_copy).splitlines()
    ]
    get_query_copy_2 = f"SELECT * FROM test.s3_queue_persistent_copy_2"
    res3 = [
        list(map(int, l.split()))
        for l in run_query(instance, get_query_copy_2).splitlines()
    ]
    assert {tuple(v) for v in res1 + res2 + res3} == set(
        [tuple(i) for i in total_values]
    )

    # Checking that all files were processed only once
    time.sleep(10)
    assert (
        get_count("test.s3_queue_persistent")
        + get_count("test.s3_queue_persistent_copy")
        + get_count("test.s3_queue_persistent_copy_2")
    ) == files_to_generate


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_multiple_tables_streaming_sync_distributed(started_cluster, mode):
    files_to_generate = 100
    poll_size = 2
    files_path = f"test_multiple_{mode}"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    instance_2 = started_cluster.instances["instance2"]

    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    for inst in [instance, instance_2]:
        inst.query(
            f"""
        DROP TABLE IF EXISTS test.s3_queue;
        DROP TABLE IF EXISTS test.s3_queue_persistent;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
            SETTINGS
                mode = '{mode}',
                keeper_path = '/clickhouse/test_multiple_consumers_{mode}',
                s3queue_polling_size = {poll_size};

        CREATE TABLE test.s3_queue_persistent ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;
        """
        )

    for inst in [instance, instance_2]:
        inst.query(
            f"""
        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv TO test.s3_queue_persistent AS
        SELECT
            *
        FROM test.s3_queue;
        """
        )

    total_values = generate_random_files(
        files_to_generate, files_path, started_cluster, bucket, row_num=1
    )

    def get_count(node, table_name):
        return int(run_query(node, f"SELECT count() FROM {table_name}"))

    for _ in range(150):
        if (
            get_count(instance, "test.s3_queue_persistent")
            + get_count(instance_2, "test.s3_queue_persistent")
        ) == files_to_generate:
            break
        time.sleep(1)

    get_query = f"SELECT * FROM test.s3_queue_persistent"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    res2 = [
        list(map(int, l.split())) for l in run_query(instance_2, get_query).splitlines()
    ]

    assert len(res1) + len(res2) == files_to_generate

    # Checking that all engines have made progress
    assert len(res1) > 0
    assert len(res2) > 0

    assert {tuple(v) for v in res1 + res2} == set([tuple(i) for i in total_values])

    # Checking that all files were processed only once
    time.sleep(10)
    assert (
        get_count(instance, "test.s3_queue_persistent")
        + get_count(instance_2, "test.s3_queue_persistent")
    ) == files_to_generate


def test_max_set_size(started_cluster):
    files_to_generate = 10
    files_path = f"test_multiple"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/*', {AUTH}'CSV')
            SETTINGS
                mode = 'unordered',
                keeper_path = '/clickhouse/test_set_size',
                s3queue_tracked_files_limit = {files_to_generate - 1};
        """
    )

    total_values = generate_random_files(
        files_to_generate, files_path, started_cluster, bucket, start_ind=0, row_num=1
    )
    get_query = f"SELECT * FROM test.s3_queue"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    assert res1 == total_values

    get_query = f"SELECT * FROM test.s3_queue"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    assert res1 == [total_values[0]]

    get_query = f"SELECT * FROM test.s3_queue"
    res1 = [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ]
    assert res1 == [total_values[1]]

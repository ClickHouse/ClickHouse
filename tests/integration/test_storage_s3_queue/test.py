import io
import logging
import os
import random
import time

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_tools import prepare_s3_bucket


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")

    minio = started_cluster.minio_client
    objects = list(
        minio.list_objects(started_cluster.minio_restricted_bucket, recursive=True)
    )
    for obj in objects:
        minio.remove_object(started_cluster.minio_restricted_bucket, obj.object_name)
    yield  # run test


MINIO_INTERNAL_PORT = 9001
AVAILABLE_MODES = ["ordered", "unordered"]
AUTH = "'minio','minio123',"


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def put_s3_file_content(started_cluster, bucket, filename, data):
    buf = io.BytesIO(data)
    started_cluster.minio_client.put_object(bucket, filename, buf, len(data))


def generate_random_files(
    count, prefix, cluster, bucket, column_num=3, row_num=10, start_ind=0
):
    total_values = []
    for i in range(start_ind, start_ind + count):
        print(i)
        rand_values = [
            [random.randint(0, 50) for _ in range(column_num)] for _ in range(row_num)
        ]
        total_values += rand_values
        values_csv = (
            "\n".join((",".join(map(str, row)) for row in rand_values)) + "\n"
        ).encode()
        filename = f"{prefix}/test_{i}.csv"
        put_s3_file_content(cluster, bucket, filename, values_csv)
    return total_values


# Returns content of given S3 file as string.
def get_s3_file_content(started_cluster, bucket, filename, decode=True):
    # type: (ClickHouseCluster, str, str, bool) -> str

    data = started_cluster.minio_client.get_object(bucket, filename)
    data_str = b""
    for chunk in data.stream():
        data_str += chunk
    if decode:
        return data_str.decode()
    return data_str


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
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


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_direct_select_file(started_cluster, mode):
    auth = "'minio','minio123',"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = [
        [12549, 2463, 19893],
        [64021, 38652, 66703],
        [81611, 39650, 83516],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    filename = f"test.csv"
    put_s3_file_content(started_cluster, bucket, filename, values_csv)
    instance.query(
        """
                    DROP TABLE IF EXISTS test.s3_queue;
                    DROP TABLE IF EXISTS test.s3_queue_2;
                    DROP TABLE IF EXISTS test.s3_queue_3;
                    """
    )

    instance.query(
        f"""
        CREATE TABLE test.s3_queue ({table_format})
        ENGINE = S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/*', {auth}'CSV')
        SETTINGS
            mode = '{mode}',
            keeper_path = '/clickhouse/select_{mode}'
        """
    )

    get_query = f"SELECT * FROM test.s3_queue"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == values

    instance.query(
        f"""
        CREATE TABLE test.s3_queue_2 ({table_format})
        ENGINE = S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/*', {auth}'CSV')
        SETTINGS
            mode = '{mode}',
            keeper_path = '/clickhouse/select_{mode}'
        """
    )

    get_query = f"SELECT * FROM test.s3_queue"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == []
    # New table with same zookeeper path
    get_query = f"SELECT * FROM test.s3_queue_2"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == []
    # New table with different zookeeper path
    instance.query(
        f"""
        CREATE TABLE test.s3_queue_3 ({table_format})
        ENGINE = S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/*', {auth}'CSV')
        SETTINGS
            mode = '{mode}',
            keeper_path='/clickhouse/select_{mode}_2'
        """
    )
    get_query = f"SELECT * FROM test.s3_queue_3"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == values

    values = [
        [1, 1, 1],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    filename = f"t.csv"
    put_s3_file_content(started_cluster, bucket, filename, values_csv)

    get_query = f"SELECT * FROM test.s3_queue_3"
    if mode == "unordered":
        assert [
            list(map(int, l.split()))
            for l in run_query(instance, get_query).splitlines()
        ] == values
    elif mode == "ordered":
        assert [
            list(map(int, l.split()))
            for l in run_query(instance, get_query).splitlines()
        ] == []


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_direct_select_multiple_files(started_cluster, mode):
    prefix = f"multiple_files_{mode}"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    instance.query("drop table if exists test.s3_queue")
    instance.query(
        f"""
        CREATE TABLE test.s3_queue ({table_format})
        ENGINE = S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
        SETTINGS
            mode = '{mode}',
            keeper_path = '/clickhouse/select_multiple_{mode}'
        """
    )

    for i in range(5):
        rand_values = [[random.randint(0, 50) for _ in range(3)] for _ in range(10)]

        values_csv = (
            "\n".join((",".join(map(str, row)) for row in rand_values)) + "\n"
        ).encode()
        filename = f"{prefix}/test_{i}.csv"
        put_s3_file_content(started_cluster, bucket, filename, values_csv)

        get_query = f"SELECT * FROM test.s3_queue"
        assert [
            list(map(int, l.split()))
            for l in run_query(instance, get_query).splitlines()
        ] == rand_values

    total_values = generate_random_files(
        4, prefix, started_cluster, bucket, start_ind=5
    )
    get_query = f"SELECT * FROM test.s3_queue"
    assert {
        tuple(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    } == set([tuple(i) for i in total_values])


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_streaming_to_view_(started_cluster, mode):
    prefix = f"streaming_files_{mode}"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    total_values = generate_random_files(10, prefix, started_cluster, bucket)
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue_persistent;
        DROP TABLE IF EXISTS test.s3_queue;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv;
        
        CREATE TABLE test.s3_queue_persistent ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
            SETTINGS
                mode = '{mode}',
                keeper_path = '/clickhouse/view_{mode}';

        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv TO test.s3_queue_persistent AS
        SELECT
            *
        FROM test.s3_queue;
        """
    )
    expected_values = set([tuple(i) for i in total_values])
    for i in range(10):
        get_query = f"SELECT * FROM test.persistent_s3_queue_mv"

        selected_values = {
            tuple(map(int, l.split()))
            for l in run_query(instance, get_query).splitlines()
        }
        if selected_values != expected_values:
            time.sleep(1)
        else:
            break

    assert selected_values == expected_values


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_streaming_to_many_views(started_cluster, mode):
    prefix = f"streaming_files_{mode}"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    retry_cnt = 10

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue_persistent;
        DROP TABLE IF EXISTS test.s3_queue_persistent_2;
        DROP TABLE IF EXISTS test.s3_queue_persistent_3;
        DROP TABLE IF EXISTS test.s3_queue;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv_2;
        DROP TABLE IF EXISTS test.persistent_s3_queue_mv_3;

        
        CREATE TABLE test.s3_queue_persistent ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;
        
        CREATE TABLE test.s3_queue_persistent_2 ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;
        
        CREATE TABLE test.s3_queue_persistent_3 ({table_format})
        ENGINE = MergeTree()
        ORDER BY column1;

        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
            SETTINGS
                mode = '{mode}',
                keeper_path = '/clickhouse/multiple_view_{mode}';

        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv TO test.s3_queue_persistent AS
        SELECT
            *
        FROM test.s3_queue;
        
        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv_2 TO test.s3_queue_persistent_2 AS
        SELECT
            *
        FROM test.s3_queue;
        
        CREATE MATERIALIZED VIEW test.persistent_s3_queue_mv_3 TO test.s3_queue_persistent_3 AS
        SELECT
            *
        FROM test.s3_queue;
        """
    )
    total_values = generate_random_files(5, prefix, started_cluster, bucket)
    expected_values = set([tuple(i) for i in total_values])

    for i in range(retry_cnt):
        retry = False
        for get_query in [
            f"SELECT * FROM test.s3_queue_persistent",
            f"SELECT * FROM test.s3_queue_persistent_2",
            f"SELECT * FROM test.s3_queue_persistent_3",
        ]:
            selected_values = {
                tuple(map(int, l.split()))
                for l in run_query(instance, get_query).splitlines()
            }
            if i == retry_cnt - 1:
                assert selected_values == expected_values
            if selected_values != expected_values:
                retry = True
                break
        if retry:
            time.sleep(1)
        else:
            break


def test_multiple_tables_meta_mismatch(started_cluster):
    prefix = f"test_meta"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.s3_queue;
    
        CREATE TABLE test.s3_queue ({table_format})
            ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
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
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
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
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
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
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'TSV')
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
                ENGINE=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{prefix}/*', {AUTH}'CSV')
                SETTINGS
                    mode = 'ordered',
                    keeper_path = '/clickhouse/test_meta';
            """
    )

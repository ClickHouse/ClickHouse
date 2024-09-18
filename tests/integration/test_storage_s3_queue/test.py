import io
import logging
import random
import time

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
import json


AVAILABLE_MODES = ["unordered", "ordered"]
DEFAULT_AUTH = ["'minio'", "'minio123'"]
NO_AUTH = ["NOSIGN"]


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


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance_2 = started_cluster.instances["instance2"]

    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")
    instance_2.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")

    minio = started_cluster.minio_client
    objects = list(minio.list_objects(started_cluster.minio_bucket, recursive=True))
    for obj in objects:
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)
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
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance2",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "old_instance",
            with_zookeeper=True,
            image="clickhouse/clickhouse-server",
            tag="23.12",
            stay_alive=True,
            with_installed_binary=True,
            use_old_analyzer=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

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
    started_cluster,
    files_path,
    count,
    column_num=3,
    row_num=10,
    start_ind=0,
    bucket=None,
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
        put_s3_file_content(started_cluster, filename, values_csv, bucket)
    return total_values


def put_s3_file_content(started_cluster, filename, data, bucket=None):
    bucket = started_cluster.minio_bucket if bucket is None else bucket
    buf = io.BytesIO(data)
    started_cluster.minio_client.put_object(bucket, filename, buf, len(data))


def create_table(
    started_cluster,
    node,
    table_name,
    mode,
    files_path,
    format="column1 UInt32, column2 UInt32, column3 UInt32",
    additional_settings={},
    file_format="CSV",
    auth=DEFAULT_AUTH,
    bucket=None,
    expect_error=False,
):
    auth_params = ",".join(auth)
    bucket = started_cluster.minio_bucket if bucket is None else bucket

    settings = {
        "s3queue_loading_retries": 0,
        "after_processing": "keep",
        "keeper_path": f"/clickhouse/test_{table_name}",
        "mode": f"{mode}",
    }
    settings.update(additional_settings)

    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/"
    node.query(f"DROP TABLE IF EXISTS {table_name}")
    create_query = f"""
        CREATE TABLE {table_name} ({format})
        ENGINE = S3Queue('{url}', {auth_params}, {file_format})
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
        """

    if expect_error:
        return node.query_and_get_error(create_query)

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
    row_num = 50
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
    ) or node.contains_in_log(
        f"StorageS3Queue ({table_name}): Shutdown was called, stopping sync"
    )


def test_s3_client_reused(started_cluster):
    node = started_cluster.instances["instance"]
    table_name = f"test.test_s3_client_reused"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
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


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
def test_processing_threads(started_cluster, mode):
    node = started_cluster.instances["instance"]
    table_name = f"processing_threads_{mode}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
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

    for _ in range(100):
        if (get_count(f"{dst_table_name}")) == files_to_generate:
            break
        time.sleep(1)

    assert get_count(dst_table_name) == files_to_generate

    res = [
        list(map(int, l.split()))
        for l in node.query(
            f"SELECT column1, column2, column3 FROM {dst_table_name}"
        ).splitlines()
    ]
    assert {tuple(v) for v in res} == set([tuple(i) for i in total_values])

    if mode == "ordered":
        zk = started_cluster.get_kazoo_client("zoo1")
        processed_nodes = zk.get_children(f"{keeper_path}/processed/")
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
                "s3queue_total_shards_num": shards_num,
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

    if mode == "ordered":
        zk = started_cluster.get_kazoo_client("zoo1")
        processed_nodes = zk.get_children(f"{keeper_path}/processed/")
        assert len(processed_nodes) == shards_num * processing_threads
        shard_nodes = zk.get_children(f"{keeper_path}/shards/")
        assert len(shard_nodes) == shards_num


@pytest.mark.parametrize(
    "mode, processing_threads",
    [
        pytest.param("unordered", 1),
        pytest.param("unordered", 8),
        pytest.param("ordered", 1),
        pytest.param("ordered", 8),
    ],
)
def test_shards_distributed(started_cluster, mode, processing_threads):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    table_name = f"test_shards_distributed_{mode}_{processing_threads}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 300
    row_num = 50
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
                "s3queue_total_shards_num": shards_num,
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
        processed_nodes = zk.get_children(f"{keeper_path}/processed/")
        assert len(processed_nodes) == shards_num * processing_threads
        shard_nodes = zk.get_children(f"{keeper_path}/shards/")
        assert len(shard_nodes) == shards_num

    node.restart_clickhouse()
    time.sleep(10)
    assert (
        get_count(node, dst_table_name) + get_count(node_2, dst_table_name)
    ) == total_rows


def test_settings_check(started_cluster):
    node = started_cluster.instances["instance"]
    node_2 = started_cluster.instances["instance2"]
    table_name = f"test_settings_check"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
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
            "s3queue_total_shards_num": 2,
        },
    )

    assert (
        "Existing table metadata in ZooKeeper differs in s3queue_total_shards_num setting. Stored in ZooKeeper: 2, local: 3"
        in create_table(
            started_cluster,
            node_2,
            table_name,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_processing_threads_num": 5,
                "s3queue_total_shards_num": 3,
            },
            expect_error=True,
        )
    )

    assert (
        "Existing table metadata in ZooKeeper differs in s3queue_processing_threads_num setting. Stored in ZooKeeper: 5, local: 2"
        in create_table(
            started_cluster,
            node_2,
            table_name,
            mode,
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_processing_threads_num": 2,
                "s3queue_total_shards_num": 2,
            },
            expect_error=True,
        )
    )

    assert "s3queue_current_shard_num = 0" in node.query(
        f"SHOW CREATE TABLE {table_name}"
    )

    node.restart_clickhouse()

    assert "s3queue_current_shard_num = 0" in node.query(
        f"SHOW CREATE TABLE {table_name}"
    )

    node.query(f"DROP TABLE {table_name} SYNC")


@pytest.mark.parametrize("processing_threads", [1, 5])
def test_processed_file_setting(started_cluster, processing_threads):
    node = started_cluster.instances["instance"]
    table_name = f"test_processed_file_setting_{processing_threads}"
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
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": processing_threads,
            "s3queue_last_processed_path": f"{files_path}/test_5.csv",
        },
    )
    total_values = generate_random_files(
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
    keeper_path = f"/clickhouse/test_{table_name}"
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
                "s3queue_total_shards_num": 2,
            },
        )

    total_values = generate_random_files(
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


def test_upgrade(started_cluster):
    node = started_cluster.instances["old_instance"]

    table_name = f"test_upgrade"
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
        additional_settings={
            "keeper_path": keeper_path,
        },
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

    assert expected_rows == get_count()

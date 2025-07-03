import gzip
import io
import logging
import os
import random
import threading
import time
import uuid

import pytest

import helpers.client
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.network import PartitionManager
from helpers.s3_tools import prepare_s3_bucket
from helpers.test_tools import exec_query_with_retry
from helpers.config_cluster import minio_secret_key

MINIO_INTERNAL_PORT = 9001

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
S3_DATA = [
    "minio_data/archive1.tar",
    "minio_data/archive2.tar",
    "minio_data/archive3.tar.gz",
]


def put_s3_file_content(started_cluster, bucket, filename, data):
    buf = io.BytesIO(data)
    started_cluster.minio_client.put_object(bucket, filename, buf, len(data))


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
            "restricted_dummy",
            main_configs=["configs/config_for_test_remote_host_filter.xml"],
            with_minio=True,
        )
        cluster.add_instance(
            "dummy",
            with_minio=True,
            main_configs=[
                "configs/remote_servers.xml",
                "configs/defaultS3.xml",
                "configs/named_collections.xml",
                "configs/schema_cache.xml",
                "configs/blob_log.xml",
                "configs/filesystem_caches.xml",
                "configs/text_log.xml",
            ],
            user_configs=[
                "configs/access.xml",
                "configs/users.xml",
                "configs/s3_retry.xml",
            ],
        )
        cluster.add_instance(
            "dummy_without_named_collections",
            with_minio=True,
            main_configs=[
                "configs/defaultS3.xml",
                "configs/named_collections.xml",
                "configs/schema_cache.xml",
            ],
            user_configs=["configs/access.xml"],
        )
        cluster.add_instance(
            "s3_max_redirects",
            with_minio=True,
            main_configs=["configs/defaultS3.xml"],
            user_configs=["configs/s3_max_redirects.xml", "configs/s3_retry.xml"],
        )
        cluster.add_instance(
            "s3_non_default",
            with_minio=True,
        )
        cluster.add_instance(
            "s3_with_environment_credentials",
            with_minio=True,
            env_variables={
                "AWS_ACCESS_KEY_ID": "minio",
                "AWS_SECRET_ACCESS_KEY": "ClickHouse_Minio_P@ssw0rd",
            },
            main_configs=["configs/use_environment_credentials.xml"],
        )
        cluster.add_instance(
            "dummy2",
            with_minio=True,
            main_configs=[
                "configs/remote_servers.xml",
                "configs/defaultS3.xml",
                "configs/named_collections.xml",
                "configs/schema_cache.xml",
                "configs/blob_log.xml",
                "configs/filesystem_caches.xml",
                "configs/test_logging.xml",
            ],
            user_configs=[
                "configs/access.xml",
                "configs/users.xml",
                "configs/s3_retry.xml",
                "configs/process_archives_as_whole_with_cluster.xml",
            ],
        )
        cluster.add_instance(
            "dummy_old",
            with_minio=True,
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag="25.3.3.42",
            stay_alive=True,
            main_configs=[
                "configs/remote_servers.xml",
                "configs/defaultS3.xml",
                "configs/named_collections.xml",
                "configs/schema_cache.xml",
                "configs/blob_log.xml",
                "configs/filesystem_caches.xml",
            ],
            user_configs=[
                "configs/access.xml",
                "configs/users.xml",
                "configs/s3_retry.xml",
            ],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")
        run_s3_mocks(cluster)

        for file in S3_DATA:
            cluster.minio_client.fput_object(
                bucket_name=cluster.minio_bucket,
                object_name=file,
                file_path=os.path.join(SCRIPT_DIR, file),
            )

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, *args, **kwargs):
    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, *args, **kwargs)
    logging.info("Query finished")

    return result


# Test simple put. Also checks that wrong credentials produce an error with every compression method.
@pytest.mark.parametrize(
    "maybe_auth,positive,compression",
    [
        pytest.param("", True, "auto", id="positive"),
        pytest.param(
            f"'minio','{minio_secret_key}',", True, "auto", id="auth_positive"
        ),
        pytest.param("'wrongid','wrongkey',", False, "auto", id="auto"),
        pytest.param("'wrongid','wrongkey',", False, "gzip", id="gzip"),
        pytest.param("'wrongid','wrongkey',", False, "deflate", id="deflate"),
        pytest.param("'wrongid','wrongkey',", False, "brotli", id="brotli"),
        pytest.param("'wrongid','wrongkey',", False, "xz", id="xz"),
        pytest.param("'wrongid','wrongkey',", False, "zstd", id="zstd"),
    ],
)
def test_put(started_cluster, maybe_auth, positive, compression):
    # type: (ClickHouseCluster, str, bool, str) -> None

    bucket = (
        started_cluster.minio_bucket
        if not maybe_auth
        else started_cluster.minio_restricted_bucket
    )
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    values_csv = "1,2,3\n3,2,1\n78,43,45\n"
    filename = "test.csv"
    put_query = f"""insert into table function s3('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{filename}',
                    {maybe_auth}'CSV', '{table_format}', '{compression}') settings s3_truncate_on_insert=1 values {values}"""

    try:
        run_query(instance, put_query)
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive
        assert values_csv == get_s3_file_content(started_cluster, bucket, filename)


def test_partition_by(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    partition_by = "column3"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    filename = "test_{_partition_id}.csv"
    put_query = f"""INSERT INTO TABLE FUNCTION
        s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{filename}', 'CSV', '{table_format}')
        PARTITION BY {partition_by} VALUES {values}"""

    run_query(instance, put_query)

    assert "1,2,3\n" == get_s3_file_content(started_cluster, bucket, f"{id}/test_3.csv")
    assert "3,2,1\n" == get_s3_file_content(started_cluster, bucket, f"{id}/test_1.csv")
    assert "78,43,45\n" == get_s3_file_content(
        started_cluster, bucket, f"{id}/test_45.csv"
    )

    filename = "test2_{_partition_id}.csv"
    instance.query(
        f"create table p ({table_format}) engine=S3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{filename}', 'CSV') partition by column3"
    )
    instance.query(f"insert into p values {values}")
    assert "1,2,3\n" == get_s3_file_content(
        started_cluster, bucket, f"{id}/test2_3.csv"
    )
    assert "3,2,1\n" == get_s3_file_content(
        started_cluster, bucket, f"{id}/test2_1.csv"
    )
    assert "78,43,45\n" == get_s3_file_content(
        started_cluster, bucket, f"{id}/test2_45.csv"
    )

    instance.query("drop table p")


def test_partition_by_string_column(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "col_num UInt32, col_str String"
    partition_by = "col_str"
    values = "(1, 'foo/bar'), (3, 'йцук'), (78, '你好')"
    filename = "test_{_partition_id}.csv"
    put_query = f"""INSERT INTO TABLE FUNCTION
        s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{filename}', 'CSV', '{table_format}')
        PARTITION BY {partition_by} VALUES {values}"""

    run_query(instance, put_query)

    assert '1,"foo/bar"\n' == get_s3_file_content(
        started_cluster, bucket, f"{id}/test_foo/bar.csv"
    )
    assert '3,"йцук"\n' == get_s3_file_content(
        started_cluster, bucket, f"{id}/test_йцук.csv"
    )
    assert '78,"你好"\n' == get_s3_file_content(
        started_cluster, bucket, f"{id}/test_你好.csv"
    )


def test_partition_by_const_column(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    partition_by = "'88'"
    values_csv = "1,2,3\n3,2,1\n78,43,45\n"
    filename = "test_{_partition_id}.csv"
    put_query = f"""INSERT INTO TABLE FUNCTION
        s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{filename}', 'CSV', '{table_format}')
        PARTITION BY {partition_by} VALUES {values}"""

    run_query(instance, put_query)

    assert values_csv == get_s3_file_content(
        started_cluster, bucket, f"{id}/test_88.csv"
    )


@pytest.mark.parametrize("special", ["space", "plus"])
def test_get_file_with_special(started_cluster, special):
    symbol = {"space": " ", "plus": "+"}[special]
    urlsafe_symbol = {"space": "%20", "plus": "%2B"}[special]
    auth = f"'minio','{minio_secret_key}',"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["dummy"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = [
        [12549, 2463, 19893],
        [64021, 38652, 66703],
        [81611, 39650, 83516],
        [11079, 59507, 61546],
        [51764, 69952, 6876],
        [41165, 90293, 29095],
        [40167, 78432, 48309],
        [81629, 81327, 11855],
        [55852, 21643, 98507],
        [6738, 54643, 41155],
    ]
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    filename = f"get_file_with_{special}_{symbol}two.csv"
    put_s3_file_content(started_cluster, bucket, filename, values_csv)

    get_query = f"SELECT * FROM s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/get_file_with_{special}_{urlsafe_symbol}two.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == values

    get_query = f"SELECT * FROM s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/get_file_with_{special}*.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == values

    get_query = f"SELECT * FROM s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/get_file_with_{special}_{urlsafe_symbol}*.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert [
        list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
    ] == values


@pytest.mark.parametrize("special", ["space", "plus", "plus2"])
def test_get_path_with_special(started_cluster, special):
    symbol = {"space": "%20", "plus": "%2B", "plus2": "%2B"}[special]
    safe_symbol = {"space": "%20", "plus": "+", "plus2": "%2B"}[special]
    auth = "'minio','{minio_secret_key}',"
    table_format = "column1 String"
    instance = started_cluster.instances["dummy"]
    get_query = f"SELECT * FROM s3('http://resolver:8082/get-my-path/{safe_symbol}.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert run_query(instance, get_query).splitlines() == [f"/{symbol}.csv"]


# Test put no data to S3.
@pytest.mark.parametrize(
    "auth", [pytest.param(f"'minio','{minio_secret_key}',", id="minio")]
)
def test_empty_put(started_cluster, auth):
    # type: (ClickHouseCluster, str) -> None
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    drop_empty_table_query = "DROP TABLE IF EXISTS empty_table"
    create_empty_table_query = (
        f"CREATE TABLE empty_table ({table_format}) ENGINE = Null()"
    )

    run_query(instance, drop_empty_table_query)
    run_query(instance, create_empty_table_query)

    filename = "empty_put_test.csv"
    put_query = f"""insert into table function
        s3('http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{id}/{filename}', {auth} 'CSV', '{table_format}')
        select * from empty_table"""

    run_query(instance, put_query)

    assert (
        run_query(
            instance,
            f"""select count(*) from
            s3('http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{id}/{filename}', {auth} 'CSV', '{table_format}')""",
        )
        == "0\n"
    )


# Test put values in CSV format.
@pytest.mark.parametrize(
    "maybe_auth,positive",
    [
        pytest.param("", True, id="positive"),
        pytest.param(f"'minio', '{minio_secret_key}',", True, id="auth_positive"),
        pytest.param("'wrongid','wrongkey',", False, id="negative"),
    ],
)
def test_put_csv(started_cluster, maybe_auth, positive):
    # type: (ClickHouseCluster, bool, str) -> None

    bucket = (
        started_cluster.minio_bucket
        if not maybe_auth
        else started_cluster.minio_restricted_bucket
    )
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') settings s3_truncate_on_insert=1 format CSV".format(
        started_cluster.minio_ip,
        MINIO_INTERNAL_PORT,
        bucket,
        filename,
        maybe_auth,
        table_format,
    )
    csv_data = "8,9,16\n11,18,13\n22,14,2\n"

    try:
        run_query(instance, put_query, stdin=csv_data)
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive
        assert csv_data == get_s3_file_content(started_cluster, bucket, filename)


# Test put and get with S3 server redirect.
def test_put_get_with_redirect(started_cluster):
    # type: (ClickHouseCluster) -> None

    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    values_csv = "1,1,1\n1,1,1\n11,11,11\n"
    filename = "test.csv"
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') settings s3_truncate_on_insert=1 values {}".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        filename,
        table_format,
        values,
    )
    run_query(instance, query)

    assert values_csv == get_s3_file_content(started_cluster, bucket, filename)

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/{}', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        filename,
        table_format,
    )
    stdout = run_query(instance, query)

    assert list(map(str.split, stdout.splitlines())) == [
        ["1", "1", "1", "1"],
        ["1", "1", "1", "1"],
        ["11", "11", "11", "1331"],
    ]


# Test put with restricted S3 server redirect.
def test_put_with_zero_redirect(started_cluster):
    # type: (ClickHouseCluster) -> None

    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["s3_max_redirects"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    filename = "test.csv"

    # Should work without redirect
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') settings s3_truncate_on_insert=1 values {}".format(
        started_cluster.minio_ip,
        MINIO_INTERNAL_PORT,
        bucket,
        filename,
        table_format,
        values,
    )
    run_query(instance, query)

    # Should not work with redirect
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') settings s3_truncate_on_insert=1 values {}".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        filename,
        table_format,
        values,
    )
    exception_raised = False
    try:
        run_query(instance, query)
    except Exception as e:
        assert str(e).find("Too many redirects while trying to access") != -1
        exception_raised = True
    finally:
        assert exception_raised


def test_put_get_with_globs(started_cluster):
    # type: (ClickHouseCluster) -> None
    unique_prefix = random.randint(1, 10000)
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    for i in range(10):
        for j in range(10):
            path = "{}/{}_{}/{}.csv".format(
                unique_prefix, i, random.choice(["a", "b", "c", "d"]), j
            )
            max_path = max(path, max_path)
            values = "({},{},{})".format(i, j, i + j)
            query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
                started_cluster.minio_ip,
                MINIO_INTERNAL_PORT,
                bucket,
                path,
                table_format,
                values,
            )
            run_query(instance, query)

    query = "select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from s3('http://{}:{}/{}/{}/*_{{a,b,c,d}}/%3f.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        unique_prefix,
        table_format,
    )
    assert run_query(instance, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(
            bucket=bucket, max_path=max_path
        )
    ]

    minio = started_cluster.minio_client
    for obj in list(
        minio.list_objects(
            started_cluster.minio_bucket,
            prefix="{}/".format(unique_prefix),
            recursive=True,
        )
    ):
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)


# Test multipart put.
@pytest.mark.parametrize(
    "maybe_auth,positive",
    [
        pytest.param("", True, id="positive"),
        pytest.param("'wrongid','wrongkey'", False, id="negative"),
        # ("'minio','{minio_secret_key}',",True), Redirect with credentials not working with nginx.
    ],
)
def test_multipart(started_cluster, maybe_auth, positive):
    # type: (ClickHouseCluster, str, bool) -> None

    id = uuid.uuid4()
    bucket = (
        started_cluster.minio_bucket
        if not maybe_auth
        else started_cluster.minio_restricted_bucket
    )
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    # Minimum size of part is 5 Mb for Minio.
    # See: https://github.com/minio/minio/blob/master/docs/minio-limits.md
    min_part_size_bytes = 5 * 1024 * 1024
    csv_size_bytes = int(min_part_size_bytes * 1.5)  # To have 2 parts.

    one_line_length = 6  # 3 digits, 2 commas, 1 line separator.

    total_rows = csv_size_bytes // one_line_length
    # Generate data having size more than one part
    int_data = [[1, 2, 3] for i in range(total_rows)]
    csv_data = "".join(["{},{},{}\n".format(x, y, z) for x, y, z in int_data])

    assert len(csv_data) > min_part_size_bytes

    filename = f"{id}/test_multipart.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') format CSV".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        filename,
        maybe_auth,
        table_format,
    )
    put_query_id = uuid.uuid4().hex
    try:
        run_query(
            instance,
            put_query,
            stdin=csv_data,
            settings={
                "s3_min_upload_part_size": min_part_size_bytes,
                "s3_max_single_part_upload_size": 0,
            },
            query_id=put_query_id,
        )
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive

        # Use proxy access logs to count number of parts uploaded to Minio.
        proxy_logs = started_cluster.get_container_logs("proxy1")  # type: str
        assert proxy_logs.count("PUT /{}/{}".format(bucket, filename)) >= 2

        assert csv_data == get_s3_file_content(started_cluster, bucket, filename)

    # select uploaded data from many threads
    select_query = (
        "select sum(column1), sum(column2), sum(column3) "
        "from s3('http://{host}:{port}/{bucket}/{filename}', {auth}'CSV', '{table_format}')".format(
            host=started_cluster.minio_redirect_host,
            port=started_cluster.minio_redirect_port,
            bucket=bucket,
            filename=filename,
            auth=maybe_auth,
            table_format=table_format,
        )
    )
    try:
        select_result = run_query(
            instance,
            select_query,
            settings={
                "max_download_threads": random.randint(4, 16),
                "max_download_buffer_size": 1024 * 1024,
            },
        )
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive
        assert (
            select_result
            == "\t".join(map(str, [total_rows, total_rows * 2, total_rows * 3])) + "\n"
        )

    if positive:
        instance.query("SYSTEM FLUSH LOGS")
        blob_storage_log = instance.query(f"SELECT * FROM system.blob_storage_log")

        result = instance.query(
            f"""SELECT
                countIf(event_type == 'MultiPartUploadCreate'),
                countIf(event_type == 'MultiPartUploadWrite'),
                countIf(event_type == 'MultiPartUploadComplete'),
                count()
            FROM system.blob_storage_log WHERE query_id = '{put_query_id}'"""
        )
        r = result.strip().split("\t")
        assert int(r[0]) == 1, blob_storage_log
        assert int(r[1]) >= 1, blob_storage_log
        assert int(r[2]) == 1, blob_storage_log
        assert int(r[0]) + int(r[1]) + int(r[2]) == int(r[3]), blob_storage_log


def test_remote_host_filter(started_cluster):
    instance = started_cluster.instances["restricted_dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(
        "invalid_host", MINIO_INTERNAL_PORT, started_cluster.minio_bucket, format
    )
    assert "not allowed in configuration file" in instance.query_and_get_error(query)

    other_values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(
        "invalid_host",
        MINIO_INTERNAL_PORT,
        started_cluster.minio_bucket,
        format,
        other_values,
    )
    assert "not allowed in configuration file" in instance.query_and_get_error(query)


def test_wrong_s3_syntax(started_cluster):
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    expected_err_msg = "Code: 42"  # NUMBER_OF_ARGUMENTS_DOESNT_MATCH

    query = "create table test_table_s3_syntax (id UInt32) ENGINE = S3('', '', '', '', '', '', '')"
    assert expected_err_msg in instance.query_and_get_error(query)

    expected_err_msg = "Code: 36"  # BAD_ARGUMENTS

    query = "create table test_table_s3_syntax (id UInt32) ENGINE = S3('')"
    assert expected_err_msg in instance.query_and_get_error(query)


# https://en.wikipedia.org/wiki/One_Thousand_and_One_Nights
def test_s3_glob_scheherazade(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1)"
    nights_per_job = 1001 // 30
    jobs = []
    for night in range(0, 1001, nights_per_job):

        def add_tales(start, end):
            for i in range(start, end):
                path = "night_{}/tale.csv".format(i)
                query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
                    started_cluster.minio_ip,
                    MINIO_INTERNAL_PORT,
                    bucket,
                    path,
                    table_format,
                    values,
                )
                run_query(instance, query)

        jobs.append(
            threading.Thread(
                target=add_tales, args=(night, min(night + nights_per_job, 1001))
            )
        )
        jobs[-1].start()

    for job in jobs:
        job.join()

    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/night_*/tale.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        table_format,
    )
    assert run_query(instance, query).splitlines() == ["1001\t1001\t1001\t1001"]


def test_s3_enum_glob_should_not_list(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1)"
    jobs = []

    glob1 = [2, 3, 4, 5]
    glob2 = [1, 32, 48, 97, 11]
    nights = [x * 100 + y for x in glob1 for y in glob2]

    for night in nights:
        path = f"shard_{night // 100}/night_{night % 100}/tale.csv"
        print(path)
        query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') settings s3_truncate_on_insert=1 values {}".format(
            started_cluster.minio_ip,
            MINIO_INTERNAL_PORT,
            bucket,
            path,
            table_format,
            values,
        )
        run_query(instance, query)

    for job in jobs:
        job.join()

    # Enum of multiple elements
    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/shard_2/night_{{32,48,97,11}}/tale.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        table_format,
    )
    query_id = f"validate_no_s3_list_requests{uuid.uuid4()}"
    assert run_query(instance, query, query_id=query_id).splitlines() == ["4\t4\t4\t4"]

    # Enum of one element
    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/shard_2/night_{{32}}/tale.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        table_format,
    )
    query_id = f"validate_no_s3_list_requests{uuid.uuid4()}"
    assert run_query(instance, query, query_id=query_id).splitlines() == ["1\t1\t1\t1"]

    # Enum of one element of one char
    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/shard_2/night_{{1}}/tale.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        table_format,
    )
    query_id = f"validate_no_s3_list_requests{uuid.uuid4()}"
    assert run_query(instance, query, query_id=query_id).splitlines() == ["1\t1\t1\t1"]

    instance.query("SYSTEM FLUSH LOGS")
    list_request_count = instance.query(
        f"select ProfileEvents['S3ListObjects'] from system.query_log where query_id = '{query_id}' and type = 'QueryFinish'"
    )
    assert list_request_count == "0\n"


# a bit simplified version of scheherazade test
# checks e.g. `prefix{1,2}/file*.csv`, where there are more than 1000 files under prefix1.
def test_s3_glob_many_objects_under_selection(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1)"
    jobs = []
    for thread_num in range(16):

        def create_files(thread_num):
            for f_num in range(thread_num * 63, thread_num * 63 + 63):
                path = f"folder1/file{f_num}.csv"
                query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') settings s3_truncate_on_insert=1 values {}".format(
                    started_cluster.minio_ip,
                    MINIO_INTERNAL_PORT,
                    bucket,
                    path,
                    table_format,
                    values,
                )
                run_query(instance, query)

        jobs.append(threading.Thread(target=create_files, args=(thread_num,)))
        jobs[-1].start()

    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') settings s3_truncate_on_insert=1 values {}".format(
        started_cluster.minio_ip,
        MINIO_INTERNAL_PORT,
        bucket,
        f"folder2/file0.csv",
        table_format,
        values,
    )
    run_query(instance, query)

    for job in jobs:
        job.join()

    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/folder{{1,2}}/file*.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host,
        started_cluster.minio_redirect_port,
        bucket,
        table_format,
    )
    assert run_query(instance, query).splitlines() == ["1009\t1009\t1009\t1009"]


def run_s3_mocks(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            ("mock_s3.py", "resolver", "8080"),
            ("unstable_server.py", "resolver", "8081"),
            ("echo.py", "resolver", "8082"),
            ("no_list_objects.py", "resolver", "8083"),
        ],
    )


def replace_config(path, old, new):
    config = open(path, "r")
    config_lines = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(path, "w")
    config.writelines(config_lines)
    config.close()


def test_custom_auth_headers(started_cluster):
    config_path = os.path.join(
        SCRIPT_DIR,
        "./{}/dummy/configs/config.d/defaultS3.xml".format(
            started_cluster.instances_dir_name
        ),
    )

    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = "select * from s3('http://resolver:8080/{bucket}/{file}', 'CSV', '{table_format}')".format(
        bucket=started_cluster.minio_restricted_bucket,
        file=filename,
        table_format=table_format,
    )

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    result = run_query(instance, get_query)
    assert result == "1\t2\t3\n"

    instance.query("DROP TABLE IF EXISTS test")
    instance.query(
        "CREATE TABLE test ({table_format}) ENGINE = S3('http://resolver:8080/{bucket}/{file}', 'CSV')".format(
            bucket=started_cluster.minio_restricted_bucket,
            file=filename,
            table_format=table_format,
        )
    )
    assert run_query(instance, "SELECT * FROM test") == "1\t2\t3\n"

    replace_config(
        config_path,
        "<header>Authorization: Bearer TOKEN",
        "<header>Authorization: Bearer INVALID_TOKEN",
    )
    instance.query("SYSTEM RELOAD CONFIG")
    ret, err = instance.query_and_get_answer_with_error("SELECT * FROM test")
    assert ret == "" and err != ""
    replace_config(
        config_path,
        "<header>Authorization: Bearer INVALID_TOKEN",
        "<header>Authorization: Bearer TOKEN",
    )
    instance.query("SYSTEM RELOAD CONFIG")
    assert run_query(instance, "SELECT * FROM test") == "1\t2\t3\n"
    instance.query("DROP TABLE test")


def test_custom_auth_headers_exclusion(started_cluster):
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = f"SELECT * FROM s3('http://resolver:8080/{started_cluster.minio_restricted_bucket}/restricteddirectory/{filename}', 'CSV', '{table_format}')"

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        result = run_query(instance, get_query)
        print(result)

    assert ei.value.returncode == 243
    assert "HTTP response code: 403" in ei.value.stderr


def test_infinite_redirect(started_cluster):
    bucket = "redirected"
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = f"select * from s3('http://resolver:{started_cluster.minio_redirect_port}/{bucket}/{filename}', 'CSV', '{table_format}')"
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    exception_raised = False
    try:
        run_query(instance, get_query)
    except Exception as e:
        assert str(e).find("Too many redirects while trying to access") != -1
        exception_raised = True
    finally:
        assert exception_raised


@pytest.mark.parametrize(
    "extension,method",
    [pytest.param("bin", "gzip", id="bin"), pytest.param("gz", "auto", id="gz")],
)
def test_storage_s3_get_gzip(started_cluster, extension, method):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    filename = f"test_get_gzip.{extension}"
    name = f"test_get_gzip_{extension}"
    data = [
        "Sophia Intrieri,55",
        "Jack Taylor,71",
        "Christopher Silva,66",
        "Clifton Purser,35",
        "Richard Aceuedo,43",
        "Lisa Hensley,31",
        "Alice Wehrley,1",
        "Mary Farmer,47",
        "Samara Ramirez,19",
        "Shirley Lloyd,51",
        "Santos Cowger,0",
        "Richard Mundt,88",
        "Jerry Gonzalez,15",
        "Angela James,10",
        "Norman Ortega,33",
        "",
    ]
    run_query(instance, f"DROP TABLE IF EXISTS {name}")

    buf = io.BytesIO()
    compressed = gzip.GzipFile(fileobj=buf, mode="wb")
    compressed.write(("\n".join(data)).encode())
    compressed.close()
    put_s3_file_content(started_cluster, bucket, filename, buf.getvalue())

    run_query(
        instance,
        f"""CREATE TABLE {name} (name String, id UInt32) ENGINE = S3(
                                'http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{filename}',
                                'CSV',
                                '{method}')""",
    )

    run_query(instance, f"SELECT sum(id) FROM {name}").splitlines() == ["565"]
    run_query(instance, f"DROP TABLE {name}")


def test_storage_s3_get_unstable(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    table_format = "column1 Int64, column2 Int64, column3 Int64, column4 Int64"
    get_query = f"SELECT count(), sum(column3), sum(column4) FROM s3('http://resolver:8081/{started_cluster.minio_bucket}/test.csv', 'CSV', '{table_format}') SETTINGS s3_max_single_read_retries=30 FORMAT CSV"
    result = run_query(instance, get_query)
    assert result.splitlines() == ["500001,500000,0"]


def test_storage_s3_get_slow(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    table_format = "column1 Int64, column2 Int64, column3 Int64, column4 Int64"
    get_query = f"SELECT count(), sum(column3), sum(column4) FROM s3('http://resolver:8081/{started_cluster.minio_bucket}/slow_send_test.csv', 'CSV', '{table_format}') FORMAT CSV"
    result = run_query(instance, get_query)
    assert result.splitlines() == ["500001,500000,0"]


def test_storage_s3_put_uncompressed(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    filename = f"{id}/test_put_uncompressed.bin"
    name = "test_put_uncompressed"
    data = [
        "'Gloria Thompson',99",
        "'Matthew Tang',98",
        "'Patsy Anderson',23",
        "'Nancy Badillo',93",
        "'Roy Hunt',5",
        "'Adam Kirk',51",
        "'Joshua Douds',28",
        "'Jolene Ryan',0",
        "'Roxanne Padilla',50",
        "'Howard Roberts',41",
        "'Ricardo Broughton',13",
        "'Roland Speer',83",
        "'Cathy Cohan',58",
        "'Kathie Dawson',100",
        "'Gregg Mcquistion',11",
    ]
    run_query(
        instance,
        "CREATE TABLE {} (name String, id UInt32) ENGINE = S3('http://{}:{}/{}/{}', 'CSV')".format(
            name, started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, filename
        ),
    )
    insert_query_id = uuid.uuid4().hex
    data_sep = "),("
    run_query(
        instance,
        "INSERT INTO {} VALUES ({})".format(name, data_sep.join(data)),
        query_id=insert_query_id,
    )

    run_query(instance, "SELECT sum(id) FROM {}".format(name)).splitlines() == ["753"]

    uncompressed_content = get_s3_file_content(started_cluster, bucket, filename)
    assert sum([int(i.split(",")[1]) for i in uncompressed_content.splitlines()]) == 753

    instance.query("SYSTEM FLUSH LOGS")
    blob_storage_log = instance.query(f"SELECT * FROM system.blob_storage_log")

    result = instance.query(
        f"""SELECT
            countIf(event_type == 'Upload'),
            countIf(remote_path == '{filename}'),
            countIf(bucket == '{bucket}'),
            count()
        FROM system.blob_storage_log WHERE query_id = '{insert_query_id}'"""
    )
    r = result.strip().split("\t")
    assert int(r[0]) >= 1, blob_storage_log
    assert all(col == r[0] for col in r), blob_storage_log
    run_query(instance, f"DROP TABLE {name}")


@pytest.mark.parametrize(
    "extension,method",
    [pytest.param("bin", "gzip", id="bin"), pytest.param("gz", "auto", id="gz")],
)
def test_storage_s3_put_gzip(started_cluster, extension, method):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    filename = f"{id}/test_put_gzip.{extension}"
    name = f"test_put_gzip_{extension}"
    data = [
        "'Joseph Tomlinson',5",
        "'Earnest Essary',44",
        "'Matha Pannell',24",
        "'Michael Shavers',46",
        "'Elias Groce',38",
        "'Pamela Bramlet',50",
        "'Lewis Harrell',49",
        "'Tamara Fyall',58",
        "'George Dixon',38",
        "'Alice Walls',49",
        "'Paula Mais',24",
        "'Myrtle Pelt',93",
        "'Sylvia Naffziger',18",
        "'Amanda Cave',83",
        "'Yolanda Joseph',89",
    ]
    run_query(
        instance,
        f"""CREATE TABLE {name} (name String, id UInt32) ENGINE = S3(
                                'http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{filename}',
                                'CSV',
                                '{method}')""",
    )

    run_query(instance, f"INSERT INTO {name} VALUES ({'),('.join(data)})")

    run_query(instance, f"SELECT sum(id) FROM {name}").splitlines() == ["708"]

    buf = io.BytesIO(
        get_s3_file_content(started_cluster, bucket, filename, decode=False)
    )
    f = gzip.GzipFile(fileobj=buf, mode="rb")
    uncompressed_content = f.read().decode()
    assert sum([int(i.split(",")[1]) for i in uncompressed_content.splitlines()]) == 708
    run_query(instance, f"DROP TABLE {name}")


def test_truncate_table(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    name = "truncate"

    instance.query(
        "CREATE TABLE {} (id UInt32) ENGINE = S3('http://{}:{}/{}/{}', 'CSV')".format(
            name, started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, name
        )
    )

    instance.query("INSERT INTO {} SELECT number FROM numbers(10)".format(name))
    result = instance.query("SELECT * FROM {}".format(name))
    assert result == instance.query("SELECT number FROM numbers(10)")
    instance.query("TRUNCATE TABLE {}".format(name))

    minio = started_cluster.minio_client
    timeout = 30
    while timeout > 0:
        if (
            len(list(minio.list_objects(started_cluster.minio_bucket, "truncate/")))
            == 0
        ):
            break
        timeout -= 1
        time.sleep(1)
    assert len(list(minio.list_objects(started_cluster.minio_bucket, "truncate/"))) == 0
    # FIXME: there was a bug in test and it was never checked.
    # Currently read from truncated table fails with
    # DB::Exception: Failed to get object info: No response body..
    # HTTP response code: 404: while reading truncate: While executing S3Source
    # assert instance.query("SELECT * FROM {}".format(name)) == ""
    instance.query(f"DROP TABLE {name} SYNC")
    assert (
        instance.query(f"SELECT count() FROM system.tables where name='{name}'")
        == "0\n"
    )


def test_predefined_connection_configuration(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances[
        "dummy_without_named_collections"
    ]  # type: ClickHouseInstance
    name = "test_table"

    instance.query("CREATE USER user")
    instance.query("GRANT CREATE ON *.* TO user")
    instance.query("GRANT SOURCES ON *.* TO user")
    instance.query("GRANT SELECT ON *.* TO user")

    instance.query(f"drop table if exists {name}", user="user")
    error = instance.query_and_get_error(
        f"CREATE TABLE {name} (id UInt32) ENGINE = S3(s3_conf1, format='CSV')",
        user="user",
    )
    assert (
        "To execute this query, it's necessary to have the grant NAMED COLLECTION ON s3_conf1"
        in error
    )

    instance.query("GRANT NAMED COLLECTION ON s3_conf1 TO user", user="admin")
    instance.query(
        f"CREATE TABLE {name} (id UInt32) ENGINE = S3(s3_conf1, format='CSV')",
        user="user",
    )

    instance.query(
        f"INSERT INTO {name} SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=1"
    )
    result = instance.query(f"SELECT * FROM {name}")
    assert result == instance.query("SELECT number FROM numbers(10)")

    result = instance.query(
        "SELECT * FROM s3(s3_conf1, format='CSV', structure='id UInt32')", user="user"
    )
    assert result == instance.query("SELECT number FROM numbers(10)")

    error = instance.query_and_get_error("SELECT * FROM s3(no_collection)", user="user")
    assert (
        "To execute this query, it's necessary to have the grant NAMED COLLECTION ON no_collection"
        in error
    )
    instance2 = started_cluster.instances["dummy"]  # has named collection access
    error = instance2.query_and_get_error("SELECT * FROM s3(no_collection)")
    assert "There is no named collection `no_collection`" in error
    instance.query("DROP USER user")
    instance.query(f"DROP TABLE {name}")


result = ""


def test_url_reconnect_in_the_middle(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    table_format = "id String, data String"
    filename = "test_url_reconnect_{}.tsv".format(random.randint(0, 1000))

    instance.query(
        f"""insert into table function
                   s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{filename}', 'TSV', '{table_format}')
                   select number, randomPrintableASCII(number % 1000) from numbers(1000000)"""
    )

    with PartitionManager() as pm:
        pm_rule_reject = {
            "probability": 0.02,
            "destination": instance.ip_address,
            "source_port": started_cluster.minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm_rule_drop_all = {
            "destination": instance.ip_address,
            "source_port": started_cluster.minio_port,
            "action": "DROP",
        }
        pm._add_rule(pm_rule_reject)

        def select():
            global result
            result = instance.query(
                f"""select count(), sum(cityHash64(x)) from (select toUInt64(id) + sleep(0.1) as x from
                url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{filename}', 'TSV', '{table_format}')
                settings http_max_tries = 10, http_retry_max_backoff_ms=2000, http_send_timeout=1, http_receive_timeout=1)"""
            )
            assert result == "1000000\t3914219105369203805\n"

        thread = threading.Thread(target=select)
        thread.start()
        time.sleep(4)
        pm._add_rule(pm_rule_drop_all)

        time.sleep(2)
        pm._delete_rule(pm_rule_drop_all)
        pm._delete_rule(pm_rule_reject)

        thread.join()

        assert result == "1000000\t3914219105369203805\n"


# At the time of writing the actual read bytes are respectively 148 and 169, so -10% to not be flaky
@pytest.mark.parametrize(
    "format_name,expected_bytes_read", [("Parquet", 133), ("ORC", 150)]
)
def test_seekable_formats(started_cluster, format_name, expected_bytes_read):
    expected_lines = 1500000
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance

    table_function = f"s3(s3_{format_name.lower()}, structure='a Int32, b String', format='{format_name}')"
    exec_query_with_retry(
        instance,
        f"INSERT INTO TABLE FUNCTION {table_function} SELECT number, randomString(100) FROM numbers({expected_lines}) settings s3_truncate_on_insert=1",
        timeout=300,
    )

    result = instance.query(f"SELECT count() FROM {table_function}")
    assert int(result) == expected_lines

    result = instance.query(
        f"SELECT count() FROM {table_function} SETTINGS max_memory_usage='60M', max_download_threads=1"
    )
    assert int(result) == expected_lines

    instance.query(f"SELECT * FROM {table_function} FORMAT Null")

    instance.query("SYSTEM FLUSH LOGS")
    result = instance.query(
        f"SELECT formatReadableSize(ProfileEvents['ReadBufferFromS3Bytes']) FROM system.query_log WHERE startsWith(query, 'SELECT * FROM s3') AND memory_usage > 0 AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1"
    )
    result = result.strip()
    assert result.endswith("MiB")
    result = result[: result.index(".")]
    assert int(result) > 140


@pytest.mark.parametrize("format_name", ["Parquet", "ORC"])
def test_seekable_formats_url(started_cluster, format_name):
    bucket = started_cluster.minio_bucket
    expected_lines = 1500000
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance

    format_name_lower = format_name.lower()
    table_function = f"s3(s3_{format_name_lower}, structure='a Int32, b String', format='{format_name}')"
    exec_query_with_retry(
        instance,
        f"INSERT INTO TABLE FUNCTION {table_function} SELECT number, randomString(100) FROM numbers({expected_lines}) settings s3_truncate_on_insert=1",
        timeout=300,
    )

    result = instance.query(f"SELECT count() FROM {table_function}")
    assert int(result) == expected_lines

    url_function = f"url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_{format_name_lower}', '{format_name}', 'a Int32, b String')"
    result = instance.query(
        f"SELECT count() FROM {url_function} SETTINGS max_memory_usage='60M'"
    )
    assert int(result) == expected_lines


def test_empty_file(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    name = "empty"
    url = f"http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{name}"

    minio = started_cluster.minio_client
    minio.put_object(bucket, name, io.BytesIO(b""), 0)

    table_function = f"s3('{url}', 'CSV', 'id Int32')"
    result = instance.query(f"SELECT count() FROM {table_function}")
    assert int(result) == 0


def test_insert_with_path_with_globs(started_cluster):
    instance = started_cluster.instances["dummy"]

    table_function_3 = f"s3('http://minio1:9001/root/test_parquet*', 'minio', '{minio_secret_key}', 'Parquet', 'a Int32, b String')"
    instance.query_and_get_error(
        f"insert into table function {table_function_3} SELECT number, randomString(100) FROM numbers(500)"
    )


def test_s3_schema_inference(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into table function s3(s3_native, structure='a Int32, b String', format='Native') select number, randomString(100) from numbers(5000000) SETTINGS s3_truncate_on_insert=1"
    )
    result = instance.query(f"desc s3(s3_native, format='Native')")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = instance.query(f"select count(*) from s3(s3_native, format='Native')")
    assert int(result) == 5000000

    instance.query(
        f"create table schema_inference engine=S3(s3_native, format='Native')"
    )
    result = instance.query(f"desc schema_inference")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = instance.query(f"select count(*) from schema_inference")
    assert int(result) == 5000000

    table_function = f"url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_native', 'Native')"
    result = instance.query(f"desc {table_function}")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = instance.query(f"select count(*) from {table_function}")
    assert int(result) == 5000000

    instance.query(
        f"create table schema_inference_2 engine=URL('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_native', 'Native')"
    )
    result = instance.query(f"desc schema_inference_2")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = instance.query(f"select count(*) from schema_inference_2")
    assert int(result) == 5000000

    table_function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_native', 'Native')"
    result = instance.query(f"desc {table_function}")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = instance.query(f"select count(*) from {table_function}")
    assert int(result) == 5000000

    instance.query("drop table schema_inference")
    instance.query("drop table schema_inference_2")


def test_empty_file(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    name = "empty"
    url = f"http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{name}"

    minio = started_cluster.minio_client
    minio.put_object(bucket, name, io.BytesIO(b""), 0)

    table_function = f"s3('{url}', 'CSV', 'id Int32')"
    result = instance.query(f"SELECT count() FROM {table_function}")
    assert int(result) == 0


def test_overwrite(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    table_function = f"s3(s3_parquet, structure='a Int32, b String', format='Parquet')"
    instance.query(f"create table test_overwrite as {table_function}")
    instance.query(f"truncate table test_overwrite")
    instance.query(
        f"insert into test_overwrite select number, randomString(100) from numbers(50) settings s3_truncate_on_insert=1"
    )
    instance.query_and_get_error(
        f"insert into test_overwrite select number, randomString(100) from numbers(100)"
    )
    instance.query(
        f"insert into test_overwrite select number, randomString(100) from numbers(200) settings s3_truncate_on_insert=1"
    )

    result = instance.query(f"select count() from test_overwrite")
    assert int(result) == 200
    instance.query(f"drop table test_overwrite")


def test_create_new_files_on_insert(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    table_function = f"s3(s3_parquet, structure='a Int32, b String', format='Parquet')"
    instance.query(f"create table test_multiple_inserts as {table_function}")
    instance.query(f"truncate table test_multiple_inserts")
    instance.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(10) settings s3_truncate_on_insert=1"
    )
    instance.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(20) settings s3_create_new_file_on_insert=1"
    )
    instance.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(30) settings s3_create_new_file_on_insert=1"
    )

    result = instance.query(f"select count() from test_multiple_inserts")
    assert int(result) == 60

    instance.query(f"drop table test_multiple_inserts")

    table_function = (
        f"s3(s3_parquet_gz, structure='a Int32, b String', format='Parquet')"
    )
    instance.query(f"create table test_multiple_inserts as {table_function}")
    instance.query(f"truncate table test_multiple_inserts")
    instance.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(10) settings s3_truncate_on_insert=1"
    )
    instance.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(20) settings s3_create_new_file_on_insert=1"
    )
    instance.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(30) settings s3_create_new_file_on_insert=1"
    )

    result = instance.query(f"select count() from test_multiple_inserts")
    assert int(result) == 60
    instance.query("drop table test_multiple_inserts")


def test_format_detection(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(f"create table arrow_table_s3 (x UInt64) engine=S3(s3_arrow)")
    instance.query(
        f"insert into arrow_table_s3 select 1 settings s3_truncate_on_insert=1"
    )
    result = instance.query(f"select * from s3(s3_arrow)")
    assert int(result) == 1

    result = instance.query(
        f"select * from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow')"
    )
    assert int(result) == 1

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow')"
    )
    assert int(result) == 1

    instance.query(f"create table parquet_table_s3 (x UInt64) engine=S3(s3_parquet2)")
    instance.query(
        f"insert into parquet_table_s3 select 1 settings s3_truncate_on_insert=1"
    )
    result = instance.query(f"select * from s3(s3_parquet2)")
    assert int(result) == 1

    result = instance.query(
        f"select * from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.parquet')"
    )
    assert int(result) == 1

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.parquet')"
    )
    assert int(result) == 1
    instance.query(f"drop table arrow_table_s3")
    instance.query(f"drop table parquet_table_s3")


def test_schema_inference_from_globs(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test1.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select NULL"
    )
    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test2.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select 0"
    )

    url_filename = "test{1,2}.jsoncompacteachrow"
    result = instance.query(
        f"desc url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{url_filename}') settings input_format_json_infer_incomplete_types_as_strings=0"
    )
    assert result.strip() == "c1\tNullable(Int64)"

    result = instance.query(
        f"select * from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{url_filename}') settings input_format_json_infer_incomplete_types_as_strings=0"
    )
    assert sorted(result.split()) == ["0", "\\N"]

    result = instance.query(
        f"desc s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test*.jsoncompacteachrow') settings input_format_json_infer_incomplete_types_as_strings=0"
    )
    assert result.strip() == "c1\tNullable(Int64)"

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test*.jsoncompacteachrow') settings input_format_json_infer_incomplete_types_as_strings=0"
    )
    assert sorted(result.split()) == ["0", "\\N"]

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test3.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select NULL"
    )

    url_filename = "test{1,3}.jsoncompacteachrow"

    result = instance.query_and_get_error(
        f"desc s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{url_filename}') settings schema_inference_use_cache_for_s3=0, input_format_json_infer_incomplete_types_as_strings=0"
    )

    assert "All attempts to extract table structure from files failed" in result

    result = instance.query_and_get_error(
        f"desc url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{url_filename}') settings schema_inference_use_cache_for_url=0, input_format_json_infer_incomplete_types_as_strings=0"
    )

    assert "All attempts to extract table structure from files failed" in result

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test0.jsoncompacteachrow', 'TSV', 'x String') select '[123;]'"
    )

    result = instance.query_and_get_error(
        f"desc s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test*.jsoncompacteachrow') settings schema_inference_use_cache_for_s3=0, input_format_json_infer_incomplete_types_as_strings=0"
    )

    assert "CANNOT_EXTRACT_TABLE_STRUCTURE" in result

    url_filename = "test{0,1,2,3}.jsoncompacteachrow"

    result = instance.query_and_get_error(
        f"desc url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/{url_filename}') settings schema_inference_use_cache_for_url=0, input_format_json_infer_incomplete_types_as_strings=0"
    )

    assert "CANNOT_EXTRACT_TABLE_STRUCTURE" in result


def test_signatures(started_cluster):
    session_token = "session token that will not be checked by MiniIO"
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(f"create table test_signatures (x UInt64) engine=S3(s3_arrow)")
    instance.query(f"truncate table test_signatures")
    instance.query(f"insert into test_signatures select 1")

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow')"
    )
    assert int(result) == 1

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'Arrow', 'x UInt64')"
    )
    assert int(result) == 1

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'minio', '{minio_secret_key}')"
    )
    assert int(result) == 1

    error = instance.query_and_get_error(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'minio', '{minio_secret_key}', '{session_token}')"
    )
    assert "S3_ERROR" in error

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'Arrow', 'x UInt64', 'auto')"
    )
    assert int(result) == 1

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'minio', '{minio_secret_key}', 'Arrow')"
    )
    assert int(result) == 1

    error = instance.query_and_get_error(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'minio', '{minio_secret_key}', '{session_token}', 'Arrow')"
    )
    assert "S3_ERROR" in error

    error = instance.query_and_get_error(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'minio', '{minio_secret_key}', '{session_token}', 'Arrow', 'x UInt64')"
    )
    assert "S3_ERROR" in error

    error = instance.query_and_get_error(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test.arrow', 'minio', '{minio_secret_key}', '{session_token}', 'Arrow', 'x UInt64', 'auto')"
    )
    assert "S3_ERROR" in error

    instance.query(f"drop table test_signatures")


def test_select_columns(started_cluster):
    bucket = started_cluster.minio_bucket
    id = uuid.uuid4()
    instance = started_cluster.instances["dummy"]
    name = "test_table2"
    structure = "id UInt32, value1 Int32, value2 Int32"

    instance.query(f"drop table if exists {name}")
    instance.query(
        f"CREATE TABLE {name} ({structure}) ENGINE = S3(s3_conf1, format='Parquet')"
    )

    limit = 10000000
    instance.query(
        f"INSERT INTO {name} SELECT * FROM generateRandom('{structure}') LIMIT {limit} SETTINGS s3_truncate_on_insert=1"
    )
    instance.query(f"SELECT value2, '{id}' FROM {name}")

    instance.query("SYSTEM FLUSH LOGS")
    result1 = instance.query(
        f"SELECT ProfileEvents['ReadBufferFromS3Bytes'] FROM system.query_log WHERE type='QueryFinish' and query LIKE 'SELECT value2, ''{id}'' FROM {name}'"
    )

    instance.query(f"SELECT *, '{id}' FROM {name}")
    instance.query("SYSTEM FLUSH LOGS")
    result2 = instance.query(
        f"SELECT ProfileEvents['ReadBufferFromS3Bytes'] FROM system.query_log WHERE type='QueryFinish' and query LIKE 'SELECT *, ''{id}'' FROM {name}'"
    )

    assert round(int(result2) / int(result1)) == 3


def test_insert_select_schema_inference(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test_insert_select.native') select toUInt64(1) as x"
    )
    result = instance.query(
        f"desc s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test_insert_select.native')"
    )
    assert result.strip() == "x\tUInt64"

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{id}/test_insert_select.native')"
    )
    assert int(result) == 1


def test_parallel_reading_with_memory_limit(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_memory_limit.native') select * from numbers(1000000) SETTINGS s3_truncate_on_insert=1"
    )

    # max_download_buffer_size=1048576 -> Trigger parallel http reads
    # max_memory_usage=1000 -> Cause an exception in parallel read and server should not crash
    # max_block_size=65409/max_untracked_memory=2097152 -> Ensure that query does not succeed
    result = instance.query_and_get_error(
        f"select * from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_memory_limit.native') settings max_memory_usage=1000,max_untracked_memory=2097152,max_block_size=65409,max_download_buffer_size=1048576"
    )

    assert "Query memory limit exceeded" in result

    time.sleep(5)

    # Check that server didn't crash
    result = instance.query("select 1")
    assert int(result) == 1


def test_wrong_format_usage(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_wrong_format.native') select * from numbers(10e6) SETTINGS s3_truncate_on_insert=1"
    )
    # size(test_wrong_format.native) = 10e6*8+16(header) ~= 76MiB

    # ensure that not all file will be loaded into memory
    result = instance.query_and_get_error(
        f"desc s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_wrong_format.native', 'Parquet') settings input_format_allow_seeks=0, max_memory_usage='10Mi'"
    )

    assert "Not a Parquet file" in result


def check_profile_event_for_query(
    instance, file, storage_name, started_cluster, bucket, profile_event, amount
):
    instance.query("system flush logs")
    query_pattern = f"{storage_name}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file}'".replace(
        "'", "\\'"
    )
    res = int(
        instance.query(
            f"select ProfileEvents['{profile_event}'] from system.query_log where query like '%{query_pattern}%' and query not like '%ProfileEvents%' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
        )
    )

    assert res == amount


def check_cache_misses(instance, file, storage_name, started_cluster, bucket, amount=1):
    check_profile_event_for_query(
        instance,
        file,
        storage_name,
        started_cluster,
        bucket,
        "SchemaInferenceCacheMisses",
        amount,
    )


def check_cache_hits(instance, file, storage_name, started_cluster, bucket, amount=1):
    check_profile_event_for_query(
        instance,
        file,
        storage_name,
        started_cluster,
        bucket,
        "SchemaInferenceCacheHits",
        amount,
    )


def check_cache_invalidations(
    instance, file, storage_name, started_cluster, bucket, amount=1
):
    check_profile_event_for_query(
        instance,
        file,
        storage_name,
        started_cluster,
        bucket,
        "SchemaInferenceCacheInvalidations",
        amount,
    )


def check_cache_evictions(
    instance, file, storage_name, started_cluster, bucket, amount=1
):
    check_profile_event_for_query(
        instance,
        file,
        storage_name,
        started_cluster,
        bucket,
        "SchemaInferenceCacheEvictions",
        amount,
    )


def check_cahce_num_rows_hits(
    instance, file, storage_name, started_cluster, bucket, amount=1
):
    check_profile_event_for_query(
        instance,
        file,
        storage_name,
        started_cluster,
        bucket,
        "SchemaInferenceCacheNumRowsHits",
        amount,
    )


def run_describe_query(instance, file, storage_name, started_cluster, bucket):
    query = f"desc {storage_name}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file}')"
    instance.query(query)


def run_count_query(instance, file, storage_name, started_cluster, bucket):
    query = f"select count() from {storage_name}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file}', auto, 'x UInt64')"
    return instance.query(query)


def check_cache(instance, expected_files):
    sources = instance.query("select source from system.schema_inference_cache")
    assert sorted(map(lambda x: x.strip().split("/")[-1], sources.split())) == sorted(
        expected_files
    )


def test_schema_inference_cache(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    def test(storage_name):
        instance.query("system drop schema cache")
        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache0.jsonl') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        run_describe_query(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )
        check_cache(instance, ["test_cache0.jsonl"])
        check_cache_misses(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache0.jsonl') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        run_describe_query(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_invalidations(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache1.jsonl') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        run_describe_query(
            instance, "test_cache1.jsonl", storage_name, started_cluster, bucket
        )
        check_cache(instance, ["test_cache0.jsonl", "test_cache1.jsonl"])
        check_cache_misses(
            instance, "test_cache1.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache1.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache1.jsonl", storage_name, started_cluster, bucket
        )

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache2.jsonl') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        run_describe_query(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )
        check_cache(instance, ["test_cache1.jsonl", "test_cache2.jsonl"])
        check_cache_misses(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_evictions(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache1.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache1.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )
        check_cache(instance, ["test_cache0.jsonl", "test_cache1.jsonl"])
        check_cache_misses(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_evictions(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )
        check_cache(instance, ["test_cache0.jsonl", "test_cache2.jsonl"])
        check_cache_misses(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_evictions(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache2.jsonl", storage_name, started_cluster, bucket
        )

        run_describe_query(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache0.jsonl", storage_name, started_cluster, bucket
        )

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache3.jsonl') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        files = "test_cache{0,1,2,3}.jsonl"
        run_describe_query(instance, files, storage_name, started_cluster, bucket)
        check_cache_hits(instance, files, storage_name, started_cluster, bucket)

        instance.query(f"system drop schema cache for {storage_name}")
        check_cache(instance, [])

        run_describe_query(instance, files, storage_name, started_cluster, bucket)
        check_cache_misses(instance, files, storage_name, started_cluster, bucket, 4)

        instance.query("system drop schema cache")
        check_cache(instance, [])

        run_describe_query(instance, files, storage_name, started_cluster, bucket)
        check_cache_misses(instance, files, storage_name, started_cluster, bucket, 4)

        instance.query("system drop schema cache")

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache0.csv') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        res = run_count_query(
            instance, "test_cache0.csv", storage_name, started_cluster, bucket
        )

        assert int(res) == 100

        check_cache(instance, ["test_cache0.csv"])
        check_cache_misses(
            instance, "test_cache0.csv", storage_name, started_cluster, bucket
        )

        res = run_count_query(
            instance, "test_cache0.csv", storage_name, started_cluster, bucket
        )
        assert int(res) == 100

        check_cache_hits(
            instance, "test_cache0.csv", storage_name, started_cluster, bucket
        )

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache0.csv') select * from numbers(200) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        res = run_count_query(
            instance, "test_cache0.csv", storage_name, started_cluster, bucket
        )

        assert int(res) == 200

        check_cache_invalidations(
            instance, "test_cache0.csv", storage_name, started_cluster, bucket
        )

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache1.csv') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        res = run_count_query(
            instance, "test_cache1.csv", storage_name, started_cluster, bucket
        )

        assert int(res) == 100
        check_cache(instance, ["test_cache0.csv", "test_cache1.csv"])
        check_cache_misses(
            instance, "test_cache1.csv", storage_name, started_cluster, bucket
        )

        res = run_count_query(
            instance, "test_cache1.csv", storage_name, started_cluster, bucket
        )
        assert int(res) == 100
        check_cache_hits(
            instance, "test_cache1.csv", storage_name, started_cluster, bucket
        )

        res = run_count_query(
            instance, "test_cache{0,1}.csv", storage_name, started_cluster, bucket
        )
        assert int(res) == 300
        check_cache_hits(
            instance, "test_cache{0,1}.csv", storage_name, started_cluster, bucket, 2
        )

        instance.query(f"system drop schema cache for {storage_name}")
        check_cache(instance, [])

        res = run_count_query(
            instance, "test_cache{0,1}.csv", storage_name, started_cluster, bucket
        )
        assert int(res) == 300
        check_cache_misses(
            instance, "test_cache{0,1}.csv", storage_name, started_cluster, bucket, 2
        )

        instance.query(f"system drop schema cache for {storage_name}")
        check_cache(instance, [])

        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache.parquet') select * from numbers(100) settings s3_truncate_on_insert=1"
        )
        time.sleep(1)

        res = instance.query(
            f"select count() from {storage_name}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache.parquet')"
        )
        assert int(res) == 100
        check_cache_misses(
            instance, "test_cache.parquet", storage_name, started_cluster, bucket
        )
        check_cache_hits(
            instance, "test_cache.parquet", storage_name, started_cluster, bucket
        )
        check_cahce_num_rows_hits(
            instance, "test_cache.parquet", storage_name, started_cluster, bucket
        )

    test("s3")
    test("url")


def test_ast_auth_headers(started_cluster):
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["s3_non_default"]  # type: ClickHouseInstance
    filename = "test.csv"

    result = instance.query_and_get_error(
        f"select count() from s3('http://resolver:8080/{bucket}/{filename}', 'CSV', 'dummy String')"
    )

    assert "HTTP response code: 403" in result
    assert "S3_ERROR" in result

    result = instance.query(
        f"select * from s3('http://resolver:8080/{bucket}/{filename}', 'CSV', headers(Authorization=`Bearer TOKEN`))"
    )

    assert result.strip() == "1\t2\t3"


def test_environment_credentials(started_cluster):
    bucket = started_cluster.minio_restricted_bucket

    instance = started_cluster.instances["s3_with_environment_credentials"]
    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache3.jsonl') select * from numbers(100) settings s3_truncate_on_insert=1"
    )
    assert (
        "100"
        == instance.query(
            f"select count() from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache3.jsonl')"
        ).strip()
    )

    # manually defined access key should override from env
    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"select count() from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache4.jsonl', 'aws', 'aws123')"
        )

        assert ei.value.returncode == 243
        assert "HTTP response code: 403" in ei.value.stderr


def test_s3_list_objects_failure(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    filename = "test_no_list_{_partition_id}.csv"

    put_query = f"""
        INSERT INTO TABLE FUNCTION
        s3('http://resolver:8083/{bucket}/{filename}', 'CSV', 'c1 UInt32')
        PARTITION BY c1 % 20
        SELECT number FROM numbers(100)
        SETTINGS s3_truncate_on_insert=1
    """

    run_query(instance, put_query)

    T = 10
    for _ in range(0, T):
        started_cluster.exec_in_container(
            started_cluster.get_container_id("resolver"),
            [
                "curl",
                "-X",
                "POST",
                f"http://localhost:8083/reset_counters?max={random.randint(1, 15)}",
            ],
        )

        get_query = """
            SELECT sleep({seconds}) FROM s3('http://resolver:8083/{bucket}/test_no_list_*', 'CSV', 'c1 UInt32')
            SETTINGS s3_list_object_keys_size = 1, max_threads = {max_threads}, enable_s3_requests_logging = 1
            """.format(
            bucket=bucket, seconds=random.random(), max_threads=random.randint(2, 20)
        )

        with pytest.raises(helpers.client.QueryRuntimeException) as ei:
            result = run_query(instance, get_query)
            print(result)

        assert ei.value.returncode == 243
        assert "Could not list objects" in ei.value.stderr


def test_skip_empty_files(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files1.parquet', TSVRaw) select * from numbers(0) settings s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files2.parquet') select * from numbers(1) settings s3_truncate_on_insert=1"
    )

    def test(engine, setting):
        instance.query_and_get_error(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files1.parquet') settings {setting}=0"
        )

        instance.query_and_get_error(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files1.parquet', auto, 'number UINt64') settings {setting}=0"
        )

        instance.query_and_get_error(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files1.parquet') settings {setting}=1"
        )

        res = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files1.parquet', auto, 'number UInt64') settings {setting}=1"
        )

        assert len(res) == 0

        instance.query_and_get_error(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files{{1,2}}.parquet') settings {setting}=0"
        )

        instance.query_and_get_error(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files{{1,2}}.parquet', auto, 'number UInt64') settings {setting}=0"
        )

        res = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files{{1,2}}.parquet') settings {setting}=1"
        )

        assert int(res) == 0

        res = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files{{1,2}}.parquet', auto, 'number UInt64') settings {setting}=1"
        )

        assert int(res) == 0

    test("s3", "s3_skip_empty_files")
    test("url", "engine_url_skip_empty_files")

    res = instance.query(
        f"select * from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files{{1|2}}.parquet') settings engine_url_skip_empty_files=1"
    )

    assert int(res) == 0

    res = instance.query(
        f"select * from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/skip_empty_files{{11|1|22}}.parquet', auto, 'number UInt64') settings engine_url_skip_empty_files=1"
    )

    assert len(res.strip()) == 0


def test_read_subcolumns(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.tsv', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') select  ((1, 2), 3) SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') select  ((1, 2), 3)  SETTINGS s3_truncate_on_insert=1"
    )

    res = instance.query(
        f"select a.b.d, _path, a.b, _file, a.e from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.tsv', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "2\troot/test_subcolumns.tsv\t(1,2)\ttest_subcolumns.tsv\t3\n"

    res = instance.query(
        f"select a.b.d, _path, a.b, _file, a.e from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "2\troot/test_subcolumns.jsonl\t(1,2)\ttest_subcolumns.jsonl\t3\n"

    res = instance.query(
        f"select x.b.d, _path, x.b, _file, x.e from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "0\troot/test_subcolumns.jsonl\t(0,0)\ttest_subcolumns.jsonl\t0\n"

    res = instance.query(
        f"select x.b.d, _path, x.b, _file, x.e from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32) default ((42, 42), 42)')"
    )

    assert res == "42\troot/test_subcolumns.jsonl\t(42,42)\ttest_subcolumns.jsonl\t42\n"

    res = instance.query(
        f"select a.b.d, _path, a.b, _file, a.e from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.tsv', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "2\t/root/test_subcolumns.tsv\t(1,2)\ttest_subcolumns.tsv\t3\n"

    res = instance.query(
        f"select a.b.d, _path, a.b, _file, a.e from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "2\t/root/test_subcolumns.jsonl\t(1,2)\ttest_subcolumns.jsonl\t3\n"

    res = instance.query(
        f"select x.b.d, _path, x.b, _file, x.e from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "0\t/root/test_subcolumns.jsonl\t(0,0)\ttest_subcolumns.jsonl\t0\n"

    res = instance.query(
        f"select x.b.d, _path, x.b, _file, x.e from url('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumns.jsonl', auto, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32) default ((42, 42), 42)')"
    )

    assert (
        res == "42\t/root/test_subcolumns.jsonl\t(42,42)\ttest_subcolumns.jsonl\t42\n"
    )


def test_read_subcolumn_time(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumn_time.tsv', auto, 'a UInt32') select  (42)  SETTINGS s3_truncate_on_insert=1"
    )

    res = instance.query(
        f"select a, dateDiff('minute', _time, now()) < 59 from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_subcolumn_time.tsv', auto, 'a UInt32')"
    )

    assert res == "42\t1\n"


def test_filtering_by_file_or_path(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_filter1.tsv', auto, 'x UInt64') select 1 SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_filter2.tsv', auto, 'x UInt64') select 2 SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_filter3.tsv', auto, 'x UInt64') select 3 SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"select count(), '{id}' from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_filter*.tsv') where _file = 'test_filter1.tsv'"
    )

    instance.query("SYSTEM FLUSH LOGS")

    result = instance.query(
        f"SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query like '%{id}%' AND type='QueryFinish'"
    )

    assert int(result) == 1

    assert 0 == int(
        instance.query(
            f"select count() from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_filter*.tsv') where _file = 'kek'"
        )
    )


def test_union_schema_inference_mode(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["s3_non_default"]
    file_name_prefix = f"test_union_schema_inference_{id}_"

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}1.jsonl') select 1 as a SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}2.jsonl') select 2 as b SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}3.jsonl') select 2 as c SETTINGS s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}4.jsonl', TSV) select 'Error' SETTINGS s3_truncate_on_insert=1"
    )

    for engine in ["s3", "url"]:
        instance.query("system drop schema cache for s3")

        result = instance.query(
            f"desc {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}{{1,2,3}}.jsonl') settings schema_inference_mode='union', describe_compact_output=1 format TSV"
        )
        assert result == "a\tNullable(Int64)\nb\tNullable(Int64)\nc\tNullable(Int64)\n"

        result = instance.query(
            f"select schema_inference_mode, splitByChar('/', source)[-1] as file, schema from system.schema_inference_cache where source like '%{file_name_prefix}%' order by file format TSV"
        )
        assert (
            result == f"UNION\t{file_name_prefix}1.jsonl\ta Nullable(Int64)\n"
            f"UNION\t{file_name_prefix}2.jsonl\tb Nullable(Int64)\n"
            f"UNION\t{file_name_prefix}3.jsonl\tc Nullable(Int64)\n"
        )
        result = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}{{1,2,3}}.jsonl') order by tuple(*) settings schema_inference_mode='union', describe_compact_output=1 format TSV"
        )
        assert result == "1\t\\N\t\\N\n" "\\N\t2\t\\N\n" "\\N\t\\N\t2\n"

        instance.query(f"system drop schema cache for {engine}")
        result = instance.query(
            f"desc {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}2.jsonl') settings schema_inference_mode='union', describe_compact_output=1 format TSV"
        )
        assert result == "b\tNullable(Int64)\n"

        result = instance.query(
            f"desc {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}{{1,2,3}}.jsonl') settings schema_inference_mode='union', describe_compact_output=1 format TSV"
        )
        assert (
            result == "a\tNullable(Int64)\n"
            "b\tNullable(Int64)\n"
            "c\tNullable(Int64)\n"
        )

        error = instance.query_and_get_error(
            f"desc {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{file_name_prefix}{{1,2,3,4}}.jsonl') settings schema_inference_mode='union', describe_compact_output=1 format TSV"
        )
        assert "CANNOT_EXTRACT_TABLE_STRUCTURE" in error


def test_s3_format_detection(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection0', 'JSONEachRow', 'x UInt64, y String') select number, 'str_' || toString(number) from numbers(0) settings s3_truncate_on_insert=1"
    )

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection1', 'JSONEachRow', 'x UInt64, y String') select number, 'str_' || toString(number) from numbers(5) settings s3_truncate_on_insert=1"
    )

    expected_result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection1', 'JSONEachRow', 'x UInt64, y String')"
    )

    expected_desc_result = instance.query(
        f"desc s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection1', 'JSONEachRow')"
    )

    for engine in ["s3", "url"]:
        desc_result = instance.query(
            f"desc {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection1')"
        )

        assert desc_result == expected_desc_result

        result = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection1')"
        )

        assert result == expected_result

        result = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection1', auto, 'x UInt64, y String')"
        )

        assert result == expected_result

        result = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection{{0,1}}', auto, 'x UInt64, y String')"
        )

        assert result == expected_result

        instance.query(f"system drop schema cache for {engine}")

        result = instance.query(
            f"select * from {engine}('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_format_detection{{0,1}}', auto, 'x UInt64, y String')"
        )

        assert result == expected_result


def test_respect_object_existence_on_partitioned_write(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write42.csv', 'CSV', 'x UInt64') select 42 settings s3_truncate_on_insert=1"
    )

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write42.csv')"
    )

    assert int(result) == 42

    error = instance.query_and_get_error(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write{{_partition_id}}.csv', 'CSV', 'x UInt64') partition by 42 select 42 settings s3_truncate_on_insert=0"
    )

    assert "BAD_ARGUMENTS" in error

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write{{_partition_id}}.csv', 'CSV', 'x UInt64') partition by 42 select 43 settings s3_truncate_on_insert=1"
    )

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write42.csv')"
    )

    assert int(result) == 43

    instance.query(
        f"insert into table function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write{{_partition_id}}.csv', 'CSV', 'x UInt64') partition by 42 select 44 settings s3_truncate_on_insert=0, s3_create_new_file_on_insert=1"
    )

    result = instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_partitioned_write42.1.csv')"
    )

    assert int(result) == 44


def test_filesystem_cache(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    table_name = f"test_filesystem_cache-{uuid.uuid4()}"

    instance.query(
        f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_name}.tsv', auto, 'x UInt64') select number from numbers(100) SETTINGS s3_truncate_on_insert=1"
    )

    query_id = f"{table_name}-{uuid.uuid4()}"
    instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_name}.tsv') SETTINGS filesystem_cache_name = 'cache1', enable_filesystem_cache=1",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    count = int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    assert count == 290
    assert 0 < int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    instance.query("SYSTEM DROP SCHEMA CACHE")

    query_id = f"{table_name}-{uuid.uuid4()}"
    instance.query(
        f"select * from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{table_name}.tsv') SETTINGS filesystem_cache_name = 'cache1', enable_filesystem_cache=1",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    assert count * 2 == int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    instance.query("SYSTEM FLUSH LOGS")

    total_count = int(instance.query(f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ilike '%Boundary alignment:%'"))
    assert total_count > 0
    count = int(instance.query(f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ilike '%Boundary alignment: 0%'"))
    assert count == total_count


def test_archive(started_cluster):
    id = uuid.uuid4()
    bucket = started_cluster.minio_bucket
    table_name = f"test_archive-{id}"
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    node = started_cluster.instances["dummy"]
    node2 = started_cluster.instances["dummy2"]
    node_old = started_cluster.instances["dummy_old"]

    assert "false" == node2.query("SELECT getSetting('cluster_function_process_archive_on_multiple_nodes')").strip()
    assert "true" == node.query("SELECT getSetting('cluster_function_process_archive_on_multiple_nodes')").strip()

    function = f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/minio_data/archive* :: example*.csv', 'minio', '{minio_secret_key}')"

    expected_paths = 7
    expected_paths_list = node.query(f"SELECT distinct(_path) FROM {function}")

    cluster_function_old = f"s3Cluster(cluster_with_old_server, 'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/minio_data/archive* :: example*.csv', 'minio', '{minio_secret_key}')"
    cluster_function_new = f"s3Cluster(cluster, 'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/minio_data/archive* :: example*.csv', 'minio', '{minio_secret_key}')"

    paths_list_new = node.query(f"SELECT distinct(_path) FROM {cluster_function_new} SETTINGS cluster_function_process_archive_on_multiple_nodes = 1")
    assert "Failed to get object info" in node.query_and_get_error(
        f"SELECT distinct(_path) FROM {cluster_function_old} SETTINGS max_threads=1"
    )

    assert expected_paths == int(node.query(f"SELECT uniqExact(_path) FROM {function}"))

    query_id = f"query_{uuid.uuid4()}"
    assert expected_paths == int(
        node2.query(
            f"SELECT uniqExact(_path) FROM {cluster_function_old}",
            query_id=query_id,
        )
    )
    node.query("SYSTEM FLUSH LOGS")
    node.contains_in_log("Will send over the whole archive")

    assert expected_paths == int(
        node.query(
            f"SELECT uniqExact(_path) FROM {cluster_function_new} SETTINGS cluster_function_process_archive_on_multiple_nodes = 1",
        )
    ), f"Processed files {paths_list_new}, expected: {expected_paths_list}"

    expected_count = 14
    assert expected_count == int(node.query(f"SELECT count() FROM {function}"))
    assert "Failed to get object info" in node.query_and_get_error(
        f"SELECT count() FROM {cluster_function_old} SETTINGS max_threads=1"
    )

    query_id = f"query_{uuid.uuid4()}"
    # Implementation with whole archive sending can have duplicates,
    # this is was a mistake in implementation.
    assert expected_count <= int(
        node2.query(
            f"SELECT count() FROM {cluster_function_old}", query_id = query_id
        )
    )
    node2.query("SYSTEM FLUSH LOGS")
    assert 7 == int(node2.query(f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' AND message ilike '%send over the whole%'"))

    assert expected_count == int(
        node.query(f"SELECT count() FROM {cluster_function_new} SETTINGS cluster_function_process_archive_on_multiple_nodes = 1")
    )

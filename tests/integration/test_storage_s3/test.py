import gzip
import json
import logging
import os
import io
import random
import threading
import time

import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance, get_instances_dir

MINIO_INTERNAL_PORT = 9001

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

CONFIG_PATH = os.path.join(SCRIPT_DIR, './{}/dummy/configs/config.d/defaultS3.xml'.format(get_instances_dir()))


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(started_cluster):
    # Allows read-write access for bucket without authorization.
    bucket_read_write_policy = {"Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:GetBucketLocation",
                                        "Resource": "arn:aws:s3:::root"
                                    },
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:ListBucket",
                                        "Resource": "arn:aws:s3:::root"
                                    },
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:GetObject",
                                        "Resource": "arn:aws:s3:::root/*"
                                    },
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:PutObject",
                                        "Resource": "arn:aws:s3:::root/*"
                                    }
                                ]}

    minio_client = started_cluster.minio_client
    minio_client.set_bucket_policy(started_cluster.minio_bucket, json.dumps(bucket_read_write_policy))

    started_cluster.minio_restricted_bucket = "{}-with-auth".format(started_cluster.minio_bucket)
    if minio_client.bucket_exists(started_cluster.minio_restricted_bucket):
        minio_client.remove_bucket(started_cluster.minio_restricted_bucket)

    minio_client.make_bucket(started_cluster.minio_restricted_bucket)


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
        cluster.add_instance("restricted_dummy", main_configs=["configs/config_for_test_remote_host_filter.xml"],
                             with_minio=True)
        cluster.add_instance("dummy", with_minio=True, main_configs=["configs/defaultS3.xml"])
        cluster.add_instance("s3_max_redirects", with_minio=True, main_configs=["configs/defaultS3.xml"],
                             user_configs=["configs/s3_max_redirects.xml"])
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")
        run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


# Test simple put. Also checks that wrong credentials produce an error with every compression method.
@pytest.mark.parametrize("maybe_auth,positive,compression", [
    pytest.param("", True, 'auto', id="positive"),
    pytest.param("'minio','minio123',", True, 'auto', id="auth_positive"),
    pytest.param("'wrongid','wrongkey',", False, 'auto', id="auto"),
    pytest.param("'wrongid','wrongkey',", False, 'gzip', id="gzip"),
    pytest.param("'wrongid','wrongkey',", False, 'deflate', id="deflate"),
    pytest.param("'wrongid','wrongkey',", False, 'brotli', id="brotli"),
    pytest.param("'wrongid','wrongkey',", False, 'xz', id="xz"),
    pytest.param("'wrongid','wrongkey',", False, 'zstd', id="zstd")
])
def test_put(started_cluster, maybe_auth, positive, compression):
    # type: (ClickHouseCluster) -> None

    bucket = started_cluster.minio_bucket if not maybe_auth else started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    values_csv = "1,2,3\n3,2,1\n78,43,45\n"
    filename = "test.csv"
    put_query = f"""insert into table function s3('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{filename}',
                    {maybe_auth}'CSV', '{table_format}', {compression}) values {values}"""

    try:
        run_query(instance, put_query)
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive
        assert values_csv == get_s3_file_content(started_cluster, bucket, filename)


@pytest.mark.parametrize("special", [
    "space",
    "plus"
])
def test_get_file_with_special(started_cluster, special):
    symbol = {"space": " ", "plus": "+"}[special]
    urlsafe_symbol = {"space": "%20", "plus": "%2B"}[special]
    auth = "'minio','minio123',"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["dummy"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = [[12549, 2463, 19893], [64021, 38652, 66703], [81611, 39650, 83516], [11079, 59507, 61546], [51764, 69952, 6876], [41165, 90293, 29095], [40167, 78432, 48309], [81629, 81327, 11855], [55852, 21643, 98507], [6738, 54643, 41155]]
    values_csv = ('\n'.join((','.join(map(str, row)) for row in values)) + '\n').encode()
    filename = f"get_file_with_{special}_{symbol}two.csv"
    put_s3_file_content(started_cluster, bucket, filename, values_csv)

    get_query = f"SELECT * FROM s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/get_file_with_{special}_{urlsafe_symbol}two.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert [list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()] == values

    get_query = f"SELECT * FROM s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/get_file_with_{special}*.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert [list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()] == values

    get_query = f"SELECT * FROM s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/get_file_with_{special}_{urlsafe_symbol}*.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert [list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()] == values


@pytest.mark.parametrize("special", [
    "space",
    "plus",
    "plus2"
])
def test_get_path_with_special(started_cluster, special):
    symbol = {"space": "%20", "plus": "%2B", "plus2": "%2B"}[special]
    safe_symbol = {"space": "%20", "plus": "+", "plus2": "%2B"}[special]
    auth = "'minio','minio123',"
    table_format = "column1 String"
    instance = started_cluster.instances["dummy"]
    get_query = f"SELECT * FROM s3('http://resolver:8082/get-my-path/{safe_symbol}.csv', {auth}'CSV', '{table_format}') FORMAT TSV"
    assert run_query(instance, get_query).splitlines() == [f"/{symbol}.csv"]


# Test put no data to S3.
@pytest.mark.parametrize("auth", [
    pytest.param("'minio','minio123',", id="minio")
])
def test_empty_put(started_cluster, auth):
    # type: (ClickHouseCluster, str) -> None

    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    create_empty_table_query = """
        CREATE TABLE empty_table (
        {}
        ) ENGINE = Null()
    """.format(table_format)

    run_query(instance, create_empty_table_query)

    filename = "empty_put_test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') select * from empty_table".format(
        started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, filename, auth, table_format)

    run_query(instance, put_query)

    try:
        run_query(instance, "select count(*) from s3('http://{}:{}/{}/{}', {}'CSV', '{}')".format(
            started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, filename, auth, table_format))

        assert False, "Query should be failed."
    except helpers.client.QueryRuntimeException as e:
        assert str(e).find("The specified key does not exist") != 0


# Test put values in CSV format.
@pytest.mark.parametrize("maybe_auth,positive", [
    pytest.param("", True, id="positive"),
    pytest.param("'minio','minio123',", True, id="auth_positive"),
    pytest.param("'wrongid','wrongkey',", False, id="negative"),
])
def test_put_csv(started_cluster, maybe_auth, positive):
    # type: (ClickHouseCluster, bool, str) -> None

    bucket = started_cluster.minio_bucket if not maybe_auth else started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') format CSV".format(
        started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, filename, maybe_auth, table_format)
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
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        started_cluster.minio_redirect_host, started_cluster.minio_redirect_port, bucket, filename, table_format, values)
    run_query(instance, query)

    assert values_csv == get_s3_file_content(started_cluster, bucket, filename)

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/{}', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host, started_cluster.minio_redirect_port, bucket, filename, table_format)
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
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, filename, table_format, values)
    run_query(instance, query)

    # Should not work with redirect
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        started_cluster.minio_redirect_host, started_cluster.minio_redirect_port, bucket, filename, table_format, values)
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

    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    for i in range(10):
        for j in range(10):
            path = "{}_{}/{}.csv".format(i, random.choice(['a', 'b', 'c', 'd']), j)
            max_path = max(path, max_path)
            values = "({},{},{})".format(i, j, i + j)
            query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
                started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, path, table_format, values)
            run_query(instance, query)

    query = "select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from s3('http://{}:{}/{}/*_{{a,b,c,d}}/%3f.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host, started_cluster.minio_redirect_port, bucket, table_format)
    assert run_query(instance, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(bucket=bucket, max_path=max_path)]


# Test multipart put.
@pytest.mark.parametrize("maybe_auth,positive", [
    pytest.param("", True, id="positive"),
    pytest.param("'wrongid','wrongkey'", False, id="negative"),
    # ("'minio','minio123',",True), Redirect with credentials not working with nginx.
])
def test_multipart_put(started_cluster, maybe_auth, positive):
    # type: (ClickHouseCluster) -> None

    bucket = started_cluster.minio_bucket if not maybe_auth else started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    # Minimum size of part is 5 Mb for Minio.
    # See: https://github.com/minio/minio/blob/master/docs/minio-limits.md
    min_part_size_bytes = 5 * 1024 * 1024
    csv_size_bytes = int(min_part_size_bytes * 1.5)  # To have 2 parts.

    one_line_length = 6  # 3 digits, 2 commas, 1 line separator.

    # Generate data having size more than one part
    int_data = [[1, 2, 3] for i in range(csv_size_bytes // one_line_length)]
    csv_data = "".join(["{},{},{}\n".format(x, y, z) for x, y, z in int_data])

    assert len(csv_data) > min_part_size_bytes

    filename = "test_multipart.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') format CSV".format(
        started_cluster.minio_redirect_host, started_cluster.minio_redirect_port, bucket, filename, maybe_auth, table_format)

    try:
        run_query(instance, put_query, stdin=csv_data, settings={'s3_min_upload_part_size': min_part_size_bytes,
                                                                 's3_max_single_part_upload_size': 0})
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive

        # Use proxy access logs to count number of parts uploaded to Minio.
        proxy_logs = started_cluster.get_container_logs("proxy1")  # type: str
        assert proxy_logs.count("PUT /{}/{}".format(bucket, filename)) >= 2

        assert csv_data == get_s3_file_content(started_cluster, bucket, filename)


def test_remote_host_filter(started_cluster):
    instance = started_cluster.instances["restricted_dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(
        "invalid_host", MINIO_INTERNAL_PORT, started_cluster.minio_bucket, format)
    assert "not allowed in config.xml" in instance.query_and_get_error(query)

    other_values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(
        "invalid_host", MINIO_INTERNAL_PORT, started_cluster.minio_bucket, format, other_values)
    assert "not allowed in config.xml" in instance.query_and_get_error(query)


@pytest.mark.parametrize("s3_storage_args", [
    pytest.param("''", id="1_argument"),
    pytest.param("'','','','','',''", id="6_arguments"),
])
def test_wrong_s3_syntax(started_cluster, s3_storage_args):
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    expected_err_msg = "Code: 42"  # NUMBER_OF_ARGUMENTS_DOESNT_MATCH

    query = "create table test_table_s3_syntax (id UInt32) ENGINE = S3({})".format(s3_storage_args)
    assert expected_err_msg in instance.query_and_get_error(query)


# https://en.wikipedia.org/wiki/One_Thousand_and_One_Nights
def test_s3_glob_scheherazade(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    values = "(1, 1, 1)"
    nights_per_job = 1001 // 30
    jobs = []
    for night in range(0, 1001, nights_per_job):
        def add_tales(start, end):
            for i in range(start, end):
                path = "night_{}/tale.csv".format(i)
                query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
                    started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, path, table_format, values)
                run_query(instance, query)

        jobs.append(threading.Thread(target=add_tales, args=(night, min(night + nights_per_job, 1001))))
        jobs[-1].start()

    for job in jobs:
        job.join()

    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/night_*/tale.csv', 'CSV', '{}')".format(
        started_cluster.minio_redirect_host, started_cluster.minio_redirect_port, bucket, table_format)
    assert run_query(instance, query).splitlines() == ["1001\t1001\t1001\t1001"]



def run_s3_mocks(started_cluster):
    logging.info("Starting s3 mocks")
    mocks = (
        ("mock_s3.py", "resolver", "8080"),
        ("unstable_server.py", "resolver", "8081"),
        ("echo.py", "resolver", "8082"),
    )
    for mock_filename, container, port in mocks:
        container_id = started_cluster.get_container_id(container)
        current_dir = os.path.dirname(__file__)
        started_cluster.copy_file_to_container(container_id, os.path.join(current_dir, "s3_mocks", mock_filename), mock_filename)
        started_cluster.exec_in_container(container_id, ["python", mock_filename, port], detach=True)

    # Wait for S3 mocks to start
    for mock_filename, container, port in mocks:
        num_attempts = 100
        for attempt in range(num_attempts):
            ping_response = started_cluster.exec_in_container(started_cluster.get_container_id(container),
                                                              ["curl", "-s", f"http://localhost:{port}/"], nothrow=True)
            if ping_response != 'OK':
                if attempt == num_attempts - 1:
                    assert ping_response == 'OK', 'Expected "OK", but got "{}"'.format(ping_response)
                else:
                    time.sleep(1)
            else:
                logging.debug(f"mock {mock_filename} ({port}) answered {ping_response} on attempt {attempt}")
                break

    logging.info("S3 mocks started")


def replace_config(old, new):
    config = open(CONFIG_PATH, 'r')
    config_lines = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(CONFIG_PATH, 'w')
    config.writelines(config_lines)
    config.close()


def test_custom_auth_headers(started_cluster):
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = "select * from s3('http://resolver:8080/{bucket}/{file}', 'CSV', '{table_format}')".format(
        bucket=started_cluster.minio_restricted_bucket,
        file=filename,
        table_format=table_format)

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    result = run_query(instance, get_query)
    assert result == '1\t2\t3\n'

    instance.query(
        "CREATE TABLE test ({table_format}) ENGINE = S3('http://resolver:8080/{bucket}/{file}', 'CSV')".format(
            bucket=started_cluster.minio_restricted_bucket,
            file=filename,
            table_format=table_format
        ))
    assert run_query(instance, "SELECT * FROM test") == '1\t2\t3\n'

    replace_config("<header>Authorization: Bearer TOKEN", "<header>Authorization: Bearer INVALID_TOKEN")
    instance.query("SYSTEM RELOAD CONFIG")
    ret, err = instance.query_and_get_answer_with_error("SELECT * FROM test")
    assert ret == "" and err != ""
    replace_config("<header>Authorization: Bearer INVALID_TOKEN", "<header>Authorization: Bearer TOKEN")
    instance.query("SYSTEM RELOAD CONFIG")
    assert run_query(instance, "SELECT * FROM test") == '1\t2\t3\n'


def test_custom_auth_headers_exclusion(started_cluster):
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = f"SELECT * FROM s3('http://resolver:8080/{started_cluster.minio_restricted_bucket}/restricteddirectory/{filename}', 'CSV', '{table_format}')"

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        result = run_query(instance, get_query)
        print(result)

    assert ei.value.returncode == 243
    assert 'Forbidden Error' in ei.value.stderr


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
@pytest.mark.parametrize("extension,method", [
    pytest.param("bin", "gzip", id="bin"),
    pytest.param("gz", "auto", id="gz"),
])
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
        ""
    ]
    buf = io.BytesIO()
    compressed = gzip.GzipFile(fileobj=buf, mode="wb")
    compressed.write(("\n".join(data)).encode())
    compressed.close()
    put_s3_file_content(started_cluster, bucket, filename, buf.getvalue())

    run_query(instance, f"""CREATE TABLE {name} (name String, id UInt32) ENGINE = S3(
                                'http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{filename}',
                                'CSV',
                                '{method}')""")

    run_query(instance, "SELECT sum(id) FROM {}".format(name)).splitlines() == ["565"]


def test_storage_s3_get_unstable(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    table_format = "column1 Int64, column2 Int64, column3 Int64, column4 Int64"
    get_query = f"SELECT count(), sum(column3), sum(column4) FROM s3('http://resolver:8081/{started_cluster.minio_bucket}/test.csv', 'CSV', '{table_format}') FORMAT CSV"
    result = run_query(instance, get_query)
    assert result.splitlines() == ["500001,500000,0"]


def test_storage_s3_put_uncompressed(started_cluster):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    filename = "test_put_uncompressed.bin"
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
    run_query(instance, "CREATE TABLE {} (name String, id UInt32) ENGINE = S3('http://{}:{}/{}/{}', 'CSV')".format(
        name, started_cluster.minio_ip, MINIO_INTERNAL_PORT, bucket, filename))

    run_query(instance, "INSERT INTO {} VALUES ({})".format(name, "),(".join(data)))

    run_query(instance, "SELECT sum(id) FROM {}".format(name)).splitlines() == ["753"]

    uncompressed_content = get_s3_file_content(started_cluster, bucket, filename)
    assert sum([ int(i.split(',')[1]) for i in uncompressed_content.splitlines() ]) == 753


@pytest.mark.parametrize("extension,method", [
    pytest.param("bin", "gzip", id="bin"),
    pytest.param("gz", "auto", id="gz")
])
def test_storage_s3_put_gzip(started_cluster, extension, method):
    bucket = started_cluster.minio_bucket
    instance = started_cluster.instances["dummy"]
    filename = f"test_put_gzip.{extension}"
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
        "'Yolanda Joseph',89"
    ]
    run_query(instance, f"""CREATE TABLE {name} (name String, id UInt32) ENGINE = S3(
                                'http://{started_cluster.minio_ip}:{MINIO_INTERNAL_PORT}/{bucket}/{filename}',
                                'CSV',
                                '{method}')""")

    run_query(instance, f"INSERT INTO {name} VALUES ({'),('.join(data)})")

    run_query(instance, f"SELECT sum(id) FROM {name}").splitlines() == ["708"]

    buf = io.BytesIO(get_s3_file_content(started_cluster, bucket, filename, decode=False))
    f = gzip.GzipFile(fileobj=buf, mode="rb")
    uncompressed_content = f.read().decode()
    assert sum([ int(i.split(',')[1]) for i in uncompressed_content.splitlines() ]) == 708

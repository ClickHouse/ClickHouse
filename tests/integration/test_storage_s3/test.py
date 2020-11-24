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
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(cluster):
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

    minio_client = cluster.minio_client
    minio_client.set_bucket_policy(cluster.minio_bucket, json.dumps(bucket_read_write_policy))

    cluster.minio_restricted_bucket = "{}-with-auth".format(cluster.minio_bucket)
    if minio_client.bucket_exists(cluster.minio_restricted_bucket):
        minio_client.remove_bucket(cluster.minio_restricted_bucket)

    minio_client.make_bucket(cluster.minio_restricted_bucket)


def put_s3_file_content(cluster, bucket, filename, data):
    buf = io.BytesIO(data)
    cluster.minio_client.put_object(bucket, filename, buf, len(data))


# Returns content of given S3 file as string.
def get_s3_file_content(cluster, bucket, filename, decode=True):
    # type: (ClickHouseCluster, str) -> str

    data = cluster.minio_client.get_object(bucket, filename)
    data_str = b""
    for chunk in data.stream():
        data_str += chunk
    if decode:
        return data_str.decode()
    return data_str


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("restricted_dummy", main_configs=["configs/config_for_test_remote_host_filter.xml"],
                             with_minio=True)
        cluster.add_instance("dummy", with_minio=True, main_configs=["configs/defaultS3.xml"])
        cluster.add_instance("s3_max_redirects", with_minio=True, main_configs=["configs/defaultS3.xml"], user_configs=["configs/s3_max_redirects.xml"])
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")
        run_s3_mock(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


# Test simple put.
@pytest.mark.parametrize("maybe_auth,positive", [
    ("", True),
    ("'minio','minio123',", True),
    ("'wrongid','wrongkey',", False)
])
def test_put(cluster, maybe_auth, positive):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket if not maybe_auth else cluster.minio_restricted_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    values_csv = "1,2,3\n3,2,1\n78,43,45\n"
    filename = "test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') values {}".format(
        cluster.minio_host, cluster.minio_port, bucket, filename, maybe_auth, table_format, values)

    try:
        run_query(instance, put_query)
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive
        assert values_csv == get_s3_file_content(cluster, bucket, filename)


# Test put no data to S3.
@pytest.mark.parametrize("auth", [
    "'minio','minio123',"
])
def test_empty_put(cluster, auth):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    create_empty_table_query = """
        CREATE TABLE empty_table (
        {}
        ) ENGINE = Null()
    """.format(table_format)

    run_query(instance, create_empty_table_query)

    filename = "empty_put_test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') select * from empty_table".format(
        cluster.minio_host, cluster.minio_port, bucket, filename, auth, table_format)

    run_query(instance, put_query)

    try:
        run_query(instance, "select count(*) from s3('http://{}:{}/{}/{}', {}'CSV', '{}')".format(
            cluster.minio_host, cluster.minio_port, bucket, filename, auth, table_format))

        assert False, "Query should be failed."
    except helpers.client.QueryRuntimeException as e:
        assert str(e).find("The specified key does not exist") != 0


# Test put values in CSV format.
@pytest.mark.parametrize("maybe_auth,positive", [
    ("", True),
    ("'minio','minio123',", True),
    ("'wrongid','wrongkey',", False)
])
def test_put_csv(cluster, maybe_auth, positive):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket if not maybe_auth else cluster.minio_restricted_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', {}'CSV', '{}') format CSV".format(
        cluster.minio_host, cluster.minio_port, bucket, filename, maybe_auth, table_format)
    csv_data = "8,9,16\n11,18,13\n22,14,2\n"

    try:
        run_query(instance, put_query, stdin=csv_data)
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive
        assert csv_data == get_s3_file_content(cluster, bucket, filename)


# Test put and get with S3 server redirect.
def test_put_get_with_redirect(cluster):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    values_csv = "1,1,1\n1,1,1\n11,11,11\n"
    filename = "test.csv"
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, bucket, filename, table_format, values)
    run_query(instance, query)

    assert values_csv == get_s3_file_content(cluster, bucket, filename)

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/{}', 'CSV', '{}')".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, bucket, filename, table_format)
    stdout = run_query(instance, query)

    assert list(map(str.split, stdout.splitlines())) == [
        ["1", "1", "1", "1"],
        ["1", "1", "1", "1"],
        ["11", "11", "11", "1331"],
    ]


# Test put with restricted S3 server redirect.
def test_put_with_zero_redirect(cluster):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket
    instance = cluster.instances["s3_max_redirects"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    filename = "test.csv"

    # Should work without redirect
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        cluster.minio_host, cluster.minio_port, bucket, filename, table_format, values)
    run_query(instance, query)

    # Should not work with redirect
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, bucket, filename, table_format, values)
    exception_raised = False
    try:
        run_query(instance, query)
    except Exception as e:
        assert str(e).find("Too many redirects while trying to access") != -1
        exception_raised = True
    finally:
        assert exception_raised


def test_put_get_with_globs(cluster):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    for i in range(10):
        for j in range(10):
            path = "{}_{}/{}.csv".format(i, random.choice(['a', 'b', 'c', 'd']), j)
            max_path = max(path, max_path)
            values = "({},{},{})".format(i, j, i + j)
            query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
                cluster.minio_host, cluster.minio_port, bucket, path, table_format, values)
            run_query(instance, query)

    query = "select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from s3('http://{}:{}/{}/*_{{a,b,c,d}}/%3f.csv', 'CSV', '{}')".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, bucket, table_format)
    assert run_query(instance, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(bucket=bucket, max_path=max_path)]


# Test multipart put.
@pytest.mark.parametrize("maybe_auth,positive", [
    ("", True),
    # ("'minio','minio123',",True), Redirect with credentials not working with nginx.
    ("'wrongid','wrongkey',", False)
])
def test_multipart_put(cluster, maybe_auth, positive):
    # type: (ClickHouseCluster) -> None

    bucket = cluster.minio_bucket if not maybe_auth else cluster.minio_restricted_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
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
        cluster.minio_redirect_host, cluster.minio_redirect_port, bucket, filename, maybe_auth, table_format)

    try:
        run_query(instance, put_query, stdin=csv_data, settings={'s3_min_upload_part_size': min_part_size_bytes})
    except helpers.client.QueryRuntimeException:
        if positive:
            raise
    else:
        assert positive

        # Use proxy access logs to count number of parts uploaded to Minio.
        proxy_logs = cluster.get_container_logs("proxy1")  # type: str
        assert proxy_logs.count("PUT /{}/{}".format(bucket, filename)) >= 2

        assert csv_data == get_s3_file_content(cluster, bucket, filename)


def test_remote_host_filter(cluster):
    instance = cluster.instances["restricted_dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(
        "invalid_host", cluster.minio_port, cluster.minio_bucket, format)
    assert "not allowed in config.xml" in instance.query_and_get_error(query)

    other_values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(
        "invalid_host", cluster.minio_port, cluster.minio_bucket, format, other_values)
    assert "not allowed in config.xml" in instance.query_and_get_error(query)


@pytest.mark.parametrize("s3_storage_args", [
    "''",  # 1 arguments
    "'','','','','',''"  # 6 arguments
])
def test_wrong_s3_syntax(cluster, s3_storage_args):
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    expected_err_msg = "Code: 42"  # NUMBER_OF_ARGUMENTS_DOESNT_MATCH

    query = "create table test_table_s3_syntax (id UInt32) ENGINE = S3({})".format(s3_storage_args)
    assert expected_err_msg in instance.query_and_get_error(query)


# https://en.wikipedia.org/wiki/One_Thousand_and_One_Nights
def test_s3_glob_scheherazade(cluster):
    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
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
                    cluster.minio_host, cluster.minio_port, bucket, path, table_format, values)
                run_query(instance, query)

        jobs.append(threading.Thread(target=add_tales, args=(night, min(night + nights_per_job, 1001))))
        jobs[-1].start()

    for job in jobs:
        job.join()

    query = "select count(), sum(column1), sum(column2), sum(column3) from s3('http://{}:{}/{}/night_*/tale.csv', 'CSV', '{}')".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, bucket, table_format)
    assert run_query(instance, query).splitlines() == ["1001\t1001\t1001\t1001"]


def run_s3_mock(cluster):
    logging.info("Starting s3 mock")
    container_id = cluster.get_container_id('resolver')
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(container_id, os.path.join(current_dir, "s3_mock", "mock_s3.py"), "mock_s3.py")
    cluster.exec_in_container(container_id, ["python", "mock_s3.py"], detach=True)

    # Wait for S3 mock start
    for attempt in range(10):
        ping_response = cluster.exec_in_container(cluster.get_container_id('resolver'),
                                                  ["curl", "-s", "http://resolver:8080/"], nothrow=True)
        if ping_response != 'OK':
            if attempt == 9:
                assert ping_response == 'OK', 'Expected "OK", but got "{}"'.format(ping_response)
            else:
                time.sleep(1)
        else:
            break

    logging.info("S3 mock started")


def test_custom_auth_headers(cluster):
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = "select * from s3('http://resolver:8080/{bucket}/{file}', 'CSV', '{table_format}')".format(
        bucket=cluster.minio_restricted_bucket,
        file=filename,
        table_format=table_format)

    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    result = run_query(instance, get_query)
    assert result == '1\t2\t3\n'


def test_infinite_redirect(cluster):
    bucket = "redirected"
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = "select * from s3('http://resolver:8080/{bucket}/{file}', 'CSV', '{table_format}')".format(
        bucket=bucket,
        file=filename,
        table_format=table_format)
    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    exception_raised = False
    try:
        run_query(instance, get_query)
    except Exception as e:
        assert str(e).find("Too many redirects while trying to access") != -1
        exception_raised = True
    finally:
        assert exception_raised


def test_storage_s3_get_gzip(cluster):
    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]
    filename = "test_get_gzip.bin"
    name = "test_get_gzip"
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
    put_s3_file_content(cluster, bucket, filename, buf.getvalue())

    try:
        run_query(instance, "CREATE TABLE {} (name String, id UInt32) ENGINE = S3('http://{}:{}/{}/{}', 'CSV', 'gzip')".format(
            name, cluster.minio_host, cluster.minio_port, bucket, filename))

        run_query(instance, "SELECT sum(id) FROM {}".format(name)).splitlines() == ["565"]

    finally:
        run_query(instance, "DROP TABLE {}".format(name))


def test_storage_s3_put_uncompressed(cluster):
    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]
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
    try:
        run_query(instance, "CREATE TABLE {} (name String, id UInt32) ENGINE = S3('http://{}:{}/{}/{}', 'CSV')".format(
            name, cluster.minio_host, cluster.minio_port, bucket, filename))

        run_query(instance, "INSERT INTO {} VALUES ({})".format(name, "),(".join(data)))

        run_query(instance, "SELECT sum(id) FROM {}".format(name)).splitlines() == ["753"]

        uncompressed_content = get_s3_file_content(cluster, bucket, filename)
        assert sum([ int(i.split(',')[1]) for i in uncompressed_content.splitlines() ]) == 753
    finally:
        run_query(instance, "DROP TABLE {}".format(name))


def test_storage_s3_put_gzip(cluster):
    bucket = cluster.minio_bucket
    instance = cluster.instances["dummy"]
    filename = "test_put_gzip.bin"
    name = "test_put_gzip"
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
    try:
        run_query(instance, "CREATE TABLE {} (name String, id UInt32) ENGINE = S3('http://{}:{}/{}/{}', 'CSV', 'gzip')".format(
            name, cluster.minio_host, cluster.minio_port, bucket, filename))

        run_query(instance, "INSERT INTO {} VALUES ({})".format(name, "),(".join(data)))

        run_query(instance, "SELECT sum(id) FROM {}".format(name)).splitlines() == ["708"]

        buf = io.BytesIO(get_s3_file_content(cluster, bucket, filename, decode=False))
        f = gzip.GzipFile(fileobj=buf, mode="rb")
        uncompressed_content = f.read().decode()
        assert sum([ int(i.split(',')[1]) for i in uncompressed_content.splitlines() ]) == 708
    finally:
        run_query(instance, "DROP TABLE {}".format(name))

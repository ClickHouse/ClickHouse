import json
import logging

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(cluster):
    minio_client = cluster.minio_client

    if minio_client.bucket_exists(cluster.minio_bucket):
        minio_client.remove_bucket(cluster.minio_bucket)

    minio_client.make_bucket(cluster.minio_bucket)

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

    minio_client.set_bucket_policy(cluster.minio_bucket, json.dumps(bucket_read_write_policy))


# Returns content of given S3 file as string.
def get_s3_file_content(cluster, filename):
    # type: (ClickHouseCluster, str) -> str

    data = cluster.minio_client.get_object(cluster.minio_bucket, filename)
    data_str = ""
    for chunk in data.stream():
        data_str += chunk
    return data_str


# Returns nginx access log lines.
def get_nginx_access_logs():
    handle = open("/nginx/access.log", "r")
    data = handle.readlines()
    handle.close()
    return data


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("dummy", with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

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
def test_put(cluster):
    # type: (ClickHouseCluster) -> None

    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    values_csv = "1,2,3\n3,2,1\n78,43,45\n"
    filename = "test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        cluster.minio_host, cluster.minio_port, cluster.minio_bucket, filename, table_format, values)
    run_query(instance, put_query)

    assert values_csv == get_s3_file_content(cluster, filename)


# Test put values in CSV format.
def test_put_csv(cluster):
    # type: (ClickHouseCluster) -> None

    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') format CSV".format(
        cluster.minio_host, cluster.minio_port, cluster.minio_bucket, filename, table_format)
    csv_data = "8,9,16\n11,18,13\n22,14,2\n"
    run_query(instance, put_query, stdin=csv_data)

    assert csv_data == get_s3_file_content(cluster, filename)


# Test put and get with S3 server redirect.
def test_put_get_with_redirect(cluster):
    # type: (ClickHouseCluster) -> None

    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    values_csv = "1,1,1\n1,1,1\n11,11,11\n"
    filename = "test.csv"
    query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') values {}".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, cluster.minio_bucket, filename, table_format, values)
    run_query(instance, query)

    assert values_csv == get_s3_file_content(cluster, filename)

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/{}', 'CSV', '{}')".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, cluster.minio_bucket, filename, table_format)
    stdout = run_query(instance, query)

    assert list(map(str.split, stdout.splitlines())) == [
        ["1", "1", "1", "1"],
        ["1", "1", "1", "1"],
        ["11", "11", "11", "1331"],
    ]


# Test multipart put.
def test_multipart_put(cluster):
    # type: (ClickHouseCluster) -> None

    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"

    # Minimum size of part is 5 Mb for Minio.
    # See: https://github.com/minio/minio/blob/master/docs/minio-limits.md
    min_part_size_bytes = 5 * 1024 * 1024
    csv_size_bytes = int(min_part_size_bytes * 1.5)  # To have 2 parts.

    one_line_length = 6  # 3 digits, 2 commas, 1 line separator.

    # Generate data having size more than one part
    int_data = [[1, 2, 3] for i in range(csv_size_bytes / one_line_length)]
    csv_data = "".join(["{},{},{}\n".format(x, y, z) for x, y, z in int_data])

    assert len(csv_data) > min_part_size_bytes

    filename = "test_multipart.csv"
    put_query = "insert into table function s3('http://{}:{}/{}/{}', 'CSV', '{}') format CSV".format(
        cluster.minio_redirect_host, cluster.minio_redirect_port, cluster.minio_bucket, filename, table_format)

    run_query(instance, put_query, stdin=csv_data, settings={'s3_min_upload_part_size': min_part_size_bytes})

    # Use Nginx access logs to count number of parts uploaded to Minio.
    nginx_logs = get_nginx_access_logs()
    uploaded_parts = filter(lambda log_line: log_line.find(filename) >= 0 and log_line.find("PUT") >= 0, nginx_logs)
    assert uploaded_parts > 1

    assert csv_data == get_s3_file_content(cluster, filename)


def test_remote_host_filter(started_cluster):
    instance = started_cluster.instances["dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    put_communication_data(started_cluster, "=== RemoteHostFilter test ===")
    query = "select *, column1*column2*column3 from s3('http://{}:{}/', 'CSV', '{}')".format("invalid_host", started_cluster.redirecting_to_http_port, format)
    assert "not allowed in config.xml" in instance.query_and_get_error(query)

    other_values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format("invalid_host", started_cluster.redirecting_preserving_data_port, started_cluster.bucket, format, other_values)
    assert "not allowed in config.xml" in instance.query_and_get_error(query)

import httplib
import json
import logging
import os
import time
import traceback

import pytest

from helpers.cluster import ClickHouseCluster


logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


def get_communication_data(started_cluster):
    conn = httplib.HTTPConnection(started_cluster.instances["dummy"].ip_address, started_cluster.communication_port)
    conn.request("GET", "/")
    r = conn.getresponse()
    raw_data = r.read()
    conn.close()
    return json.loads(raw_data)


def put_communication_data(started_cluster, body):
    conn = httplib.HTTPConnection(started_cluster.instances["dummy"].ip_address, started_cluster.communication_port)
    conn.request("PUT", "/", body)
    r = conn.getresponse()
    conn.close()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        instance = cluster.add_instance("dummy")
        cluster.start()

        cluster.communication_port = 10000
        instance.copy_file_to_container(os.path.join(os.path.dirname(__file__), "test_server.py"), "test_server.py")
        cluster.bucket = "abc"
        instance.exec_in_container(["python", "test_server.py", str(cluster.communication_port), cluster.bucket], detach=True)
        cluster.mock_host = instance.ip_address

        for i in range(10):
            try:
                data = get_communication_data(cluster)
                cluster.redirecting_to_http_port = data["redirecting_to_http_port"]
                cluster.preserving_data_port = data["preserving_data_port"]
                cluster.multipart_preserving_data_port = data["multipart_preserving_data_port"]
                cluster.redirecting_preserving_data_port = data["redirecting_preserving_data_port"]
            except:
                logging.error(traceback.format_exc())
                time.sleep(0.5)
            else:
                break
        else:
            assert False, "Could not initialize mock server"

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")
    return result


def test_get_with_redirect(started_cluster):
    instance = started_cluster.instances["dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    put_communication_data(started_cluster, "=== Get with redirect test ===")
    query = "select *, column1*column2*column3 from s3('http://{}:{}/', 'CSV', '{}')".format(started_cluster.mock_host, started_cluster.redirecting_to_http_port, format)
    stdout = run_query(instance, query)
    data = get_communication_data(started_cluster)
    expected = [ [str(row[0]), str(row[1]), str(row[2]), str(row[0]*row[1]*row[2])] for row in data["redirect_csv_data"] ]
    assert list(map(str.split, stdout.splitlines())) == expected
    

def test_put(started_cluster):
    instance = started_cluster.instances["dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    logging.info("Phase 3")
    put_communication_data(started_cluster, "=== Put test ===")
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(started_cluster.mock_host, started_cluster.preserving_data_port, started_cluster.bucket, format, values)
    run_query(instance, put_query)
    data = get_communication_data(started_cluster)
    received_data_completed = data["received_data_completed"]
    received_data = data["received_data"]
    finalize_data = data["finalize_data"]
    finalize_data_query = data["finalize_data_query"]
    assert received_data[-1].decode() == "1,2,3\n3,2,1\n78,43,45\n"
    assert received_data_completed
    assert finalize_data == "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>hello-etag</ETag></Part></CompleteMultipartUpload>"
    assert finalize_data_query == "uploadId=TEST"

    
def test_put_csv(started_cluster):
    instance = started_cluster.instances["dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    put_communication_data(started_cluster, "=== Put test CSV ===")
    put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') format CSV".format(started_cluster.mock_host, started_cluster.preserving_data_port, started_cluster.bucket, format)
    csv_data = "8,9,16\n11,18,13\n22,14,2\n"
    run_query(instance, put_query, stdin=csv_data)
    data = get_communication_data(started_cluster)
    received_data_completed = data["received_data_completed"]
    received_data = data["received_data"]
    finalize_data = data["finalize_data"]
    finalize_data_query = data["finalize_data_query"]
    assert received_data[-1].decode() == csv_data
    assert received_data_completed
    assert finalize_data == "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>hello-etag</ETag></Part></CompleteMultipartUpload>"
    assert finalize_data_query == "uploadId=TEST"

    
def test_put_with_redirect(started_cluster):
    instance = started_cluster.instances["dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    put_communication_data(started_cluster, "=== Put with redirect test ===")
    other_values = "(1, 1, 1), (1, 1, 1), (11, 11, 11)"
    query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(started_cluster.mock_host, started_cluster.redirecting_preserving_data_port, started_cluster.bucket, format, other_values)
    run_query(instance, query)

    query = "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(started_cluster.mock_host, started_cluster.preserving_data_port, started_cluster.bucket, format)
    stdout = run_query(instance, query)
    assert list(map(str.split, stdout.splitlines())) == [
        ["1", "1", "1", "1"],
        ["1", "1", "1", "1"],
        ["11", "11", "11", "1331"],
    ]
    data = get_communication_data(started_cluster)
    received_data = data["received_data"]
    assert received_data[-1].decode() == "1,1,1\n1,1,1\n11,11,11\n"


def test_multipart_put(started_cluster):
    instance = started_cluster.instances["dummy"]
    format = "column1 UInt32, column2 UInt32, column3 UInt32"

    put_communication_data(started_cluster, "=== Multipart test ===")
    long_data = [[i, i+1, i+2] for i in range(100000)]
    long_values = "".join([ "{},{},{}\n".format(x,y,z) for x, y, z in long_data ])
    put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') format CSV".format(started_cluster.mock_host, started_cluster.multipart_preserving_data_port, started_cluster.bucket, format)
    run_query(instance, put_query, stdin=long_values, settings={'s3_min_upload_part_size': 1000000})
    data = get_communication_data(started_cluster)
    assert "multipart_received_data" in data
    received_data = data["multipart_received_data"]
    assert received_data[-1].decode() == "".join([ "{},{},{}\n".format(x, y, z) for x, y, z in long_data ])
    assert 1 < data["multipart_parts"] < 10000

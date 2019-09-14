import pytest

from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        instance = cluster.add_instance('dummy')
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


import httplib
import json
import logging
import os
import time
import traceback


logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

def test_simple(started_cluster):
    instance = started_cluster.instances['dummy']
    instance.copy_file_to_container(os.path.join(os.path.dirname(__file__), 'test_server.py'), 'test_server.py')
    communication_port = 10000
    bucket = 'abc'
    instance.exec_in_container(['python', 'test_server.py', str(communication_port), bucket], detach=True)

    def get_data():
        conn = httplib.HTTPConnection(started_cluster.instances['dummy'].ip_address, communication_port)
        conn.request("GET", "/")
        r = conn.getresponse()
        raw_data = r.read()
        conn.close()
        return json.loads(raw_data)

    format = 'column1 UInt32, column2 UInt32, column3 UInt32'
    values = '(1, 2, 3), (3, 2, 1), (78, 43, 45)'
    other_values = '(1, 1, 1), (1, 1, 1), (11, 11, 11)'
    for i in range(10):
        try:
            data = get_data()
            redirecting_to_http_port = data['redirecting_to_http_port']
            preserving_data_port = data['preserving_data_port']
            redirecting_preserving_data_port = data['redirecting_preserving_data_port']
        except:
            logging.error(traceback.format_exc())
            time.sleep(0.5)
        else:
            break
    else:
        assert False, 'Could not initialize mock server'

    mock_host = started_cluster.instances['dummy'].ip_address

    def run_query(query):
        logging.info('Running query "{}"...'.format(query))
        result = instance.query(query)
        logging.info('Query finished')
        return result
    
    
    prepare_put_queries = [
        "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(mock_host, preserving_data_port, bucket, format, values),
    ]
    
    queries = [
        "select *, column1*column2*column3 from s3('http://{}:{}/', 'CSV', '{}')".format(mock_host, redirecting_to_http_port, format),
    ]
    
    put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(mock_host, preserving_data_port, bucket, format, values)
    
    redirect_put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(mock_host, redirecting_preserving_data_port, bucket, format, other_values)
    
    check_queries = [
        "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(mock_host, preserving_data_port, bucket, format),
    ]
    
    try:
        logging.info('Phase 1')
        for query in prepare_put_queries:
            run_query(query)
    
        logging.info('Phase 2')
        for query in queries:
            stdout = run_query(query)
            assert list(map(str.split, stdout.splitlines())) == [
                ['42', '87', '44', '160776'],
                ['55', '33', '81', '147015'],
                ['1', '0', '9', '0'],
            ]
    
        logging.info('Phase 3')
        query = put_query
        run_query(query)
        data = get_data()
        received_data_completed = data['received_data_completed']
        received_data = data['received_data']
        finalize_data = data['finalize_data']
        finalize_data_query = data['finalize_data_query']
        assert received_data[-1].decode() == '1,2,3\n3,2,1\n78,43,45\n'
        assert received_data_completed
        assert finalize_data == '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>hello-etag</ETag></Part></CompleteMultipartUpload>'
        assert finalize_data_query == 'uploadId=TEST'
    
        logging.info('Phase 4')
        query = redirect_put_query
        run_query(query)
    
        for query in check_queries:
            logging.info(query)
            stdout = run_query(query)
            assert list(map(str.split, stdout.splitlines())) == [
                ['1', '1', '1', '1'],
                ['1', '1', '1', '1'],
                ['11', '11', '11', '1331'],
            ]
        data = get_data()
        received_data = data['received_data']
        assert received_data[-1].decode() == '1,1,1\n1,1,1\n11,11,11\n'

        # FIXME tests for multipart
    
    except:
        logging.error(traceback.format_exc())
        raise

    else:
        logging.info('Done')

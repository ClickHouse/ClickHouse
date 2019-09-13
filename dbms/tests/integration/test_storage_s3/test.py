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


import json
import os
import time


def test_sophisticated_default(started_cluster):
    instance = started_cluster.instances['dummy']
    instance.copy_file_to_container(os.path.join(os.path.dirname(__file__), 'test_server.py'), 'test_server.py')
    communication_path = '/test_sophisticated_default'
    instance.exec_in_container(['python', 'test_server.py', communication_path], detach=True)

    format = 'column1 UInt32, column2 UInt32, column3 UInt32'
    values = '(1, 2, 3), (3, 2, 1), (78, 43, 45)'
    other_values = '(1, 1, 1), (1, 1, 1), (11, 11, 11)'
    for i in range(10):
        try:
            raw = instance.exec_in_container(['cat', communication_path])
            data = json.loads(instance.exec_in_container(['cat', communication_path]))
            redirecting_to_http_port = data['redirecting_to_http_port']
            redirecting_to_https_port = data['redirecting_to_https_port']
            preserving_data_port = data['preserving_data_port']
            redirecting_preserving_data_port = data['redirecting_preserving_data_port']
            localhost = data['localhost']
        except:
            time.sleep(0.5)
        else:
            break
    else:
        assert False, 'Could not initialize mock server' + str(raw)

    redirecting_host = localhost
    bucket = 'abc'

    def run_query(query):
        print('Running query "{}"...'.format(query))
        result = instance.query(query)
        print('Query finished')
        return result
    
    
    prepare_put_queries = [
        "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(localhost, preserving_data_port, bucket, format, values),
    ]
    
    queries = [
        "select *, column1*column2*column3 from s3('http://{}:{}/', 'CSV', '{}')".format(redirecting_host, redirecting_to_http_port, format),
    ]
    
    put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(redirecting_host, preserving_data_port, bucket, format, values)
    
    redirect_put_query = "insert into table function s3('http://{}:{}/{}/test.csv', 'CSV', '{}') values {}".format(redirecting_host, redirecting_preserving_data_port, bucket, format, other_values)
    
    check_queries = [
        "select *, column1*column2*column3 from s3('http://{}:{}/{}/test.csv', 'CSV', '{}')".format(localhost, preserving_data_port, bucket, format),
    ]
    
    try:
        print('Phase 1')
        for query in prepare_put_queries:
            run_query(query)
    
        print('Phase 2')
        for query in queries:
            stdout = run_query(query)
            assert list(map(str.split, stdout.splitlines())) == [
                ['1', '2', '3', '6'],
                ['3', '2', '1', '6'],
                ['78', '43', '45', '150930'],
            ]
    
        print('Phase 3')
        query = put_query
        run_query(query)
        for i in range(10):
            try:
                data = json.loads(instance.exec_in_container(['cat', communication_path]))
                received_data_completed = data['received_data_completed']
                received_data = data['received_data']
                finalize_data = data['finalize_data']
                finalize_data_query = data['finalize_data_query']
            except:
                time.sleep(0.5)
            else:
                break
        else:
            assert False, 'Could not read data from mock server'+str(data)
        assert received_data[-1].decode() == '1,2,3\n3,2,1\n78,43,45\n'
        assert received_data_completed
        assert finalize_data == '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>hello-etag</ETag></Part></CompleteMultipartUpload>'
        assert finalize_data_query == 'uploadId=TEST'
    
        print('Phase 4')
        query = redirect_put_query
        run_query(query)
    
        for query in check_queries:
            print(query)
            stdout = run_query(query)
            assert list(map(str.split, stdout.splitlines())) == [
                ['1', '1', '1', '1'],
                ['1', '1', '1', '1'],
                ['11', '11', '11', '1331'],
            ]
    
    finally:
        print('Done')

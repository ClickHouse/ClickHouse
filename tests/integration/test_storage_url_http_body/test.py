import os

import pytest

from helpers.cluster import ClickHouseCluster

from . import http_headers_echo_server, redirect_server

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("node")


def run_server(container_id, file_name, hostname, port, *args):
    script_dir = os.path.dirname(os.path.realpath(__file__))

    cluster.copy_file_to_container(
        container_id,
        os.path.join(script_dir, file_name),
        f"/{file_name}",
    )

    cmd_args = [hostname, port] + list(args)
    cmd_args_val = " ".join([str(x) for x in cmd_args])

    cluster.exec_in_container(
        container_id,
        [
            "bash",
            "-c",
            f"python3 /{file_name} {cmd_args_val} > {file_name}.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    for _ in range(0, 10):
        ping_response = cluster.exec_in_container(
            container_id,
            ["curl", "-s", f"http://{hostname}:{port}/"],
            nothrow=True,
        )

        if '{"status":"ok"}' in ping_response:
            return

    raise Exception("Echo server is not responding")


def run_echo_server():
    container_id = cluster.get_container_id("node")
    run_server(container_id, "http_headers_echo_server.py", "localhost", 8000)


def run_redirect_server():
    container_id = cluster.get_container_id("node")
    run_server(container_id, "redirect_server.py", "localhost", 8080, "localhost", 8000)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_redirect_server()
        run_echo_server()

        yield cluster
    finally:
        cluster.shutdown()


def run_test(input_query, expected):
    server.query(input_query)
    result = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )
    print(result)
    assert expected in result
    

def test_simple_string_in_http_body(started_cluster):
    query = "SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body('test'))"
    simple_string = 'test'
    
    run_test(query, simple_string)
    
def test_simple_subquery_in_http_body(started_cluster):
    query = "SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body((SELECT 1 + 2)))"
    simple_subquery_body = '{"plus(1, 2)":3}'
    
    run_test(query, simple_subquery_body)
 
 
generator_subquery = '''
SELECT * FROM (
    SELECT 1 as id, 'Vasya' as name, 123 as number UNION ALL
    SELECT 2 as id, 'Kolya' as name, 456 as number UNION ALL
    SELECT 3 as id, 'Dima' as name,  789 as number)
ORDER BY id
'''

def test_subquery_result_default_format_in_http_body(started_cluster): # default format is JSONLines
    json_each_row_dump = '''{"id":1,"name":"Vasya","number":123}\n{"id":2,"name":"Kolya","number":456}\n{"id":3,"name":"Dima","number":789}'''
    query = f"SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(({generator_subquery})))"
    run_test(query, json_each_row_dump)


def test_subquery_result_json_in_http_body(started_cluster):
    json_each_row_dump = '''{"id":1,"name":"Vasya","number":123}\n{"id":2,"name":"Kolya","number":456}\n{"id":3,"name":"Dima","number":789}'''
    query = f"SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(({generator_subquery}), JSONLines))"
    run_test(query, json_each_row_dump)


def test_subquery_result_csv_in_http_body(started_cluster):
    tab_separate_each_row_dump = '''1	Vasya	123\n2	Kolya	456\n3	Dima	789'''
    query = f"SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(({generator_subquery}), TabSeparated))"
    run_test(query, tab_separate_each_row_dump)


def test_subquery_result_csv_in_http_body(started_cluster):
    csv_separate_each_row_dump = '''"id","name","number"\n1,"Vasya",123\n2,"Kolya",456\n3,"Dima",789'''
    query = f"SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(({generator_subquery}), CSVWithNames))"
    run_test(query, csv_separate_each_row_dump)
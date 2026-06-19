import os

import pytest

from helpers.cluster import ClickHouseCluster

from . import body_dependent_response_server, http_headers_echo_server, redirect_server

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


def run_body_dependent_server():
    container_id = cluster.get_container_id("node")
    run_server(container_id, "body_dependent_response_server.py", "localhost", 8001)


def run_redirect_server():
    container_id = cluster.get_container_id("node")
    run_server(container_id, "redirect_server.py", "localhost", 8080, "localhost", 8000)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_redirect_server()
        run_echo_server()
        run_body_dependent_server()

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


def test_subquery_result_tsv_in_http_body(started_cluster):
    tab_separate_each_row_dump = '''1	Vasya	123\n2	Kolya	456\n3	Dima	789'''
    query = f"SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(({generator_subquery}), TabSeparated))"
    run_test(query, tab_separate_each_row_dump)


def test_subquery_result_csv_in_http_body(started_cluster):
    csv_separate_each_row_dump = '''"id","name","number"\n1,"Vasya",123\n2,"Kolya",456\n3,"Dima",789'''
    query = f"SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(({generator_subquery}), CSVWithNames))"
    run_test(query, csv_separate_each_row_dump)


def test_empty_string_in_http_body(started_cluster):
    # `body('')` is a specified, empty payload. It must still promote the request
    # from GET to POST and send an empty body, instead of behaving as if there were
    # no `body` argument at all.
    query = "SELECT * FROM url('http://localhost:8000/', headers('type'='string'), body(''))"
    server.query(query)

    method = server.exec_in_container(
        ["cat", http_headers_echo_server.METHOD_PATH], user="root"
    )
    body = server.exec_in_container(
        ["cat", http_headers_echo_server.RESULT_PATH], user="root"
    )
    assert method.strip() == "POST"
    assert body == ""


def test_schema_cache_is_body_aware(started_cluster):
    # The response schema depends on the request body, so the schema-inference cache (keyed by
    # URL/format/settings) must not be shared across different bodies. With caching enabled, a
    # second query with a different body must infer its own schema instead of reusing the first.
    settings = {
        "schema_inference_use_cache_for_url": 1,
        "schema_inference_cache_require_modification_time_for_url": 0,
    }
    columns_a = server.query(
        "DESC url('http://localhost:8001/', JSONEachRow, body('schema_a'))",
        settings=settings,
    )
    columns_bc = server.query(
        "DESC url('http://localhost:8001/', JSONEachRow, body('schema_bc'))",
        settings=settings,
    )
    assert "col_a" in columns_a
    assert "col_b" in columns_bc and "col_c" in columns_bc
    # The second schema must not be contaminated by the first body's cached columns.
    assert "col_a" not in columns_bc


def test_count_cache_is_body_aware(started_cluster):
    # The row count depends on the request body, so the URL row-count cache must not be shared
    # across different bodies. With caching enabled, a second count() with a different body must
    # send its own request instead of reusing the first body's cached count.
    settings = {
        "use_cache_for_count_from_files": 1,
        "schema_inference_cache_require_modification_time_for_url": 0,
    }
    count_one = server.query(
        "SELECT count() FROM url('http://localhost:8001/', JSONEachRow, 'v UInt8', body('rows_1'))",
        settings=settings,
    ).strip()
    count_three = server.query(
        "SELECT count() FROM url('http://localhost:8001/', JSONEachRow, 'v UInt8', body('rows_3'))",
        settings=settings,
    ).strip()
    assert count_one == "1"
    # The second count must not reuse the cached count from the first body.
    assert count_three == "3"
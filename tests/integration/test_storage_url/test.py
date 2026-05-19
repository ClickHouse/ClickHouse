import pytest
import time
import uuid
import os
import json

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)
URL_WILDCARD_EXPERIMENTAL_SETTING = "allow_experimental_url_wildcard_from_index_pages=1"
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/conf.xml", "configs/named_collections.xml", "configs/query_log.xml"],
    user_configs=["configs/users.xml"],
    with_nginx=True,
    with_minio=True,
)

@pytest.fixture(scope="module", autouse=True)
def setup_node():
    try:
        cluster.start()
        start_mock_servers(cluster, os.path.dirname(__file__), [("index_pages_server.py", "resolver", 8087)])
        node1.query(
            "insert into table function url(url1) partition by column3 values (1, 2, 3), (3, 2, 1), (1, 3, 2)"
        )
        yield
    finally:
        cluster.shutdown()


def reset_index_page_server_stats():
    resolver_id = cluster.get_container_id("resolver")
    cluster.exec_in_container(
        resolver_id,
        ["curl", "-s", "http://localhost:8087/__reset__"],
    )


def get_index_page_server_stats():
    resolver_id = cluster.get_container_id("resolver")
    return json.loads(
        cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", "http://localhost:8087/__stats__"],
        )
    )


def with_url_wildcard_setting(query):
    if "SETTINGS" in query:
        return f"{query}, {URL_WILDCARD_EXPERIMENTAL_SETTING}"
    return f"{query} SETTINGS {URL_WILDCARD_EXPERIMENTAL_SETTING}"


def test_partition_by():
    result = node1.query(
        f"select * from url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        f"select * from url('http://nginx:80/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        f"select * from url('http://nginx:80/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t2\t3"


def test_url_cluster():
    result = node1.query(
        f"select * from urlCluster('test_cluster_two_shards', 'http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        f"select * from urlCluster('test_cluster_two_shards', 'http://nginx:80/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        f"select * from urlCluster('test_cluster_two_shards', 'http://nginx:80/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t2\t3"


def test_url_cluster_secure():
    query_id = f"{uuid.uuid4()}"

    METADATA_SERVER_HOSTNAME = "node_imds"
    METADATA_SERVER_PORT = 8080
    url = f"http://adminka:secretPasswordick@{METADATA_SERVER_HOSTNAME}:{METADATA_SERVER_PORT}"
    node1.query(
        f"CREATE TABLE leaked_secret_test (column1 UInt32, column2 UInt32, column3 UInt32) ENGINE = URL('{url}', 'TSV')",
        query_id=query_id
    )
    node1.query("SYSTEM FLUSH LOGS")

    result = node1.query(
        f"select query from clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost,system.query_log) where query_id='{query_id}'"
    )

    assert 'leaked_secret_test' in result
    assert 'secretPasswordick' not in result

    node1.query("DROP TABLE leaked_secret_test")

def test_url_cluster_with_named_collection():
    result = node1.query(
        f"select * from urlCluster(test_cluster_one_shard_three_replicas_localhost, test_url)"
    )
    assert result.strip() == "3\t2\t1"

    result = node1.query(
        f"select * from urlCluster(test_cluster_one_shard_three_replicas_localhost, test_url, structure='auto')"
    )
    assert result.strip() == "3\t2\t1"


def test_url_wildcard_from_index_pages():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "12"


def test_url_wildcard_schema_inference():
    result = node1.query(
        with_url_wildcard_setting("DESCRIBE TABLE url('http://resolver:8087/data/**/part*.tsv', 'TSV')")
    )
    assert result == TSV([["c1", "Nullable(Int64)"]])

    result = node1.query(
        with_url_wildcard_setting("SELECT sum(c1) FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV')")
    )
    assert result.strip() == "12"


def test_url_wildcard_size_virtual_column():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(size) FROM ("
        "SELECT _file, any(_size) AS size "
        "FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64') "
        "GROUP BY _file)")
    )
    assert result.strip() == "8"


def test_url_wildcard_headers_virtual_column():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(length(mapKeys(_headers))) "
            "FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64') "
        )
    )
    assert int(result.strip()) > 0


def test_url_wildcard_empty_listing():
    result = node1.query(
        with_url_wildcard_setting("SELECT count() FROM url('http://resolver:8087/data/empty/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "0"


def test_url_wildcard_missing_listing():
    error = node1.query_and_get_error(
        with_url_wildcard_setting("SELECT count() FROM url('http://resolver:8087/missing/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert "There is no path" in error


def test_url_wildcard_oversize_index_page():
    error = node1.query_and_get_error(
        with_url_wildcard_setting("SELECT count() FROM url('http://resolver:8087/data/oversize/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert "exceeds max_http_index_page_size" in error


def test_url_wildcard_query_fragment_matching():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/query/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "6"


def test_url_wildcard_falls_back_to_plain_url_scan_when_href_links_are_invalid():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/invalid_href_fallback/part*.tsv', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "12"


def test_url_wildcard_reads_files_when_head_is_not_supported():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/head_fallback/part*.tsv', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "7"


def test_url_wildcard_with_headers():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url("
        "'http://resolver:8087/data/headers/**/part*.tsv', "
        "'TSV', "
        "'x UInt64', "
        "headers('X-Test-Header'='1'))")
    )
    assert result.strip() == "15"


def test_url_wildcard_expands_query_templates():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/**/part*.tsv?x={1,2}', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "24"


def test_url_wildcard_expands_address_templates():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://{resolver,resolver}:8087/data/source_query/**/part*.tsv?token=abc', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "26"


def test_url_wildcard_combines_address_templates_failover_options_and_globs():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://{resolver,resolver}:8087/data/source_query/**/part*.tsv?token={bad|abc}', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "26"


def test_url_wildcard_expands_numeric_ranges_with_index_pages():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/range/**/part{1..2}.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "30"


def test_url_wildcard_glob_patterns_with_recursive_index_pages():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/recursive_glob/**/part{a,b}.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "3"

    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/recursive_glob/**/part{1..2}.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "30"


def test_url_wildcard_question_mark_is_not_path_glob():
    error = node1.query_and_get_error(
        with_url_wildcard_setting("SELECT count() FROM url('http://resolver:8087/data/glob/part?.tsv', 'TSV', 'x UInt64')")
    )
    assert "No such file" in error or "HTTP status code: 404" in error


def test_url_wildcard_rejects_invalid_url_template_range():
    error = node1.query_and_get_error(
        with_url_wildcard_setting("SELECT count() FROM url('http://resolver:8087/data/range/**/part*.tsv?x={a..2}', 'TSV', 'x UInt64')")
    )
    assert "Incorrect left number" in error


def test_url_wildcard_glob_patterns():
    # '?' is reserved in URLs as a query delimiter, so use '*' to cover wildcard matching in URL paths.
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/glob/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "15"

    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/glob/part{a,b,c}.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "6"

    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/glob/part{1..2}.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "9"


def test_url_wildcard_deduplicates_normalized_links():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/duplicates/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "9"


def test_url_wildcard_listing_order():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/order/**/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS glob_expansion_max_elements=1"
        )
    )
    assert result.strip() == "10"


def test_url_wildcard_normalizes_leading_slashes():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087///data/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "12"


def test_url_wildcard_uses_head_for_metadata_probe():
    reset_index_page_server_stats()

    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/2025/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS use_hive_partitioning=0"
        )
    )
    assert result.strip() == "12"

    stats = get_index_page_server_stats()
    assert stats["HEAD /data/2025/part1.tsv"] == 1
    assert stats["HEAD /data/2025/part2.tsv"] == 1
    assert stats["GET /data/2025/part1.tsv"] == 1
    assert stats["GET /data/2025/part2.tsv"] == 1


def test_url_wildcard_resets_headers_between_files():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT _file, any(_headers['X-Source-File']), any(_headers['X-Probe-Method']) "
            "FROM url('http://resolver:8087/data/mixed_headers/part*.tsv', 'TSV', 'x UInt64') "
            "GROUP BY _file "
            "ORDER BY _file"
        )
    )
    assert result == "part1.tsv\tpart1.tsv\tGET\npart2.tsv\tpart2.tsv\tGET\n"


def test_url_wildcard_limits_directory_traversal():
    error = node1.query_and_get_error(
        with_url_wildcard_setting(
            "SELECT count() FROM url('http://resolver:8087/data/deep/**/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS url_wildcard_max_directories_to_read=3"
        )
    )
    assert "Too many directories while expanding URL wildcard" in error
    assert "url_wildcard_max_directories_to_read" in error


def test_url_wildcard_preserves_index_entry_query():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/query_override/part*.tsv?x=1', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "6"


def test_url_wildcard_preserves_query_for_directory_listing():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/query_directory/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "11"


def test_url_wildcard_preserves_source_query_for_directory_listing():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/source_query/**/part*.tsv?token=abc', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "13"


def test_url_wildcard_deduplicates_after_source_query_inheritance():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/source_query_duplicate/**/part*.tsv?token=abc', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "17"


def test_url_wildcard_preserves_failover_options():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/source_query/**/part*.tsv?token={bad|abc}', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "13"


def test_url_engine_wildcard_preserves_failover_options():
    table_name = "url_wildcard_failover"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    try:
        node1.query(
            f"CREATE TABLE {table_name} (x UInt64) "
            "ENGINE = URL('http://resolver:8087/data/source_query/**/part*.tsv?token={bad|abc}', 'TSV')",
            settings={"allow_experimental_url_wildcard_from_index_pages": 1},
        )
        result = node1.query(f"SELECT sum(x) FROM {table_name}")
        assert result.strip() == "13"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_url_engine_wildcard_limit_uses_query_setting():
    table_name = "url_wildcard_directory_limit"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    try:
        node1.query(
            f"CREATE TABLE {table_name} (x UInt64) "
            "ENGINE = URL('http://resolver:8087/data/deep/**/part*.tsv', 'TSV')",
            settings={"allow_experimental_url_wildcard_from_index_pages": 1},
        )

        error = node1.query_and_get_error(
            f"SELECT count() FROM {table_name} "
            "SETTINGS allow_experimental_url_wildcard_from_index_pages=1, url_wildcard_max_directories_to_read=3"
        )
        assert "Too many directories while expanding URL wildcard" in error
        assert "url_wildcard_max_directories_to_read" in error
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_url_wildcard_is_experimental():
    error = node1.query_and_get_error(
        "SELECT sum(x) FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')"
    )
    assert "allow_experimental_url_wildcard_from_index_pages" in error


def test_table_function_url_access_rights():
    node1.query("CREATE USER OR REPLACE u1")

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        f"SELECT * FROM url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    )

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        f"SELECT * FROM url('http://nginx:80/test_1', 'TSV')", user="u1"
    )

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        f"DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    )

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        f"DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV')", user="u1"
    )

    node1.query("GRANT READ ON URL TO u1")
    assert node1.query(
        f"DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV')",
        user="u1",
    ) == TSV(
        [
            ["c1", "Nullable(Int64)"],
            ["c2", "Nullable(Int64)"],
            ["c3", "Nullable(Int64)"],
        ]
    )


@pytest.mark.parametrize("file_format", ["Parquet", "CSV", "TSV", "JSONEachRow"])
def test_file_formats(file_format):
    # Generate random URL with timestamp to make test idempotent
    # Note: we could have just deleted a file using requests.delete(url)
    # But it seems we can do it only from inside the container (this is not reliable)
    timestamp = int(time.time() * 1000000)
    url = f"http://nginx:80/{file_format}_file_{timestamp}"

    values = ", ".join([f"({i}, {i + 1}, {i + 2})" for i in range(100)])
    node1.query(
        f"insert into table function url(url_file, url = '{url}', format = '{file_format}') values",
        stdin=values,
    )

    for download_threads in [1, 4, 16]:
        result = node1.query(
            f"""
SELECT *
FROM url('{url}', '{file_format}')
LIMIT 10
SETTINGS remote_read_min_bytes_for_seek = 1, max_read_buffer_size = 1, max_download_buffer_size = 1, max_download_threads = {download_threads}
"""
        )

        expected_result = ""
        for i in range(10):
            expected_result += f"{i}\t{i + 1}\t{i + 2}\n"

        assert result == expected_result

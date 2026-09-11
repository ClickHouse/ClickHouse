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
        start_mock_servers(
            cluster,
            os.path.dirname(__file__),
            [
                ("index_pages_server.py", "resolver", 8087),
                ("index_pages_server_redirect_target.py", "resolver", 8088),
            ],
        )
        node1.query(
            "insert into table function url(url1) partition by column3 values (1, 2, 3), (3, 2, 1), (1, 3, 2)"
        )
        yield
    finally:
        cluster.shutdown()


def reset_index_page_server_stats(port=8087):
    resolver_id = cluster.get_container_id("resolver")
    cluster.exec_in_container(
        resolver_id,
        ["curl", "-s", f"http://localhost:{port}/__reset__"],
    )


def get_index_page_server_stats(port=8087):
    resolver_id = cluster.get_container_id("resolver")
    return json.loads(
        cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", f"http://localhost:{port}/__stats__"],
        )
    )


def with_url_wildcard_setting(query):
    if "SETTINGS" in query:
        return f"{query}, {URL_WILDCARD_EXPERIMENTAL_SETTING}"
    return f"{query} SETTINGS {URL_WILDCARD_EXPERIMENTAL_SETTING}"


def test_partition_by():
    result = node1.query(
        "select * from url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        "select * from url('http://nginx:80/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        "select * from url('http://nginx:80/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t2\t3"


def test_url_cluster():
    result = node1.query(
        "select * from urlCluster('test_cluster_two_shards', 'http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        "select * from urlCluster('test_cluster_two_shards', 'http://nginx:80/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        "select * from urlCluster('test_cluster_two_shards', 'http://nginx:80/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t2\t3"


def test_url_cluster_supports_url_wildcard_from_index_pages():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM urlCluster('test_cluster_two_shards', "
            "'http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "12"

    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(c1) FROM urlCluster('test_cluster_two_shards', "
            "'http://resolver:8087/data/**/part*.tsv', 'TSV')"
        )
    )
    assert result.strip() == "12"


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
        "select * from urlCluster(test_cluster_one_shard_three_replicas_localhost, test_url)"
    )
    assert result.strip() == "3\t2\t1"

    result = node1.query(
        "select * from urlCluster(test_cluster_one_shard_three_replicas_localhost, test_url, structure='auto')"
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


def test_url_wildcard_schema_inference_checks_headers_before_reading():
    reset_index_page_server_stats()

    error = node1.query_and_get_error(
        with_url_wildcard_setting(
            "DESCRIBE TABLE url("
            "'http://resolver:8087/data/**/part*.tsv', "
            "'TSV', "
            "headers('X-Forbidden-Url-Wildcard'='1'))"
        )
    )
    assert "HTTP header" in error
    assert "X-Forbidden-Url-Wildcard" in error
    assert "http_forbid_headers" in error

    stats = get_index_page_server_stats()
    assert stats == {}


def test_url_wildcard_engine_checks_headers_before_reading():
    reset_index_page_server_stats()

    node1.query("DROP TABLE IF EXISTS url_wildcard_engine_forbidden_header")
    error = node1.query_and_get_error(
        "CREATE TABLE url_wildcard_engine_forbidden_header "
        "ENGINE = URL("
        "'http://resolver:8087/data/**/part*.tsv', "
        "'TSV', "
        "headers('X-Forbidden-Url-Wildcard'='1'))",
        settings={"allow_experimental_url_wildcard_from_index_pages": 1},
    )
    assert "HTTP header" in error
    assert "X-Forbidden-Url-Wildcard" in error
    assert "http_forbid_headers" in error

    # The header filter must be enforced before schema/format inference reads any index page.
    stats = get_index_page_server_stats()
    assert stats == {}


def test_url_engine_persists_inferred_format_with_compression_argument():
    table_name = "url_engine_auto_format_with_compression"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    try:
        node1.query(
            f"CREATE TABLE {table_name} (x UInt64) "
            "ENGINE = URL('http://resolver:8087/data/2025/part1.tsv', 'auto', 'none')"
        )

        result = node1.query(f"SELECT sum(x) FROM {table_name}")
        assert result.strip() == "3"

        # `SHOW CREATE TABLE` without an explicit format is returned as `TabSeparated`, which escapes
        # the single quotes (`'` becomes `\'`). Use `TSVRaw` so the create statement is returned verbatim
        # and the positional `format`/`compression` arguments can be matched as written.
        create_query = node1.query(f"SHOW CREATE TABLE {table_name} FORMAT TSVRaw")
        assert "'auto', 'none'" not in create_query
        assert "'TSV', 'none'" in create_query
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_url_wildcard_time_virtual_column_is_null_without_last_modified():
    # The mock index server never sends a `Last-Modified` header, so the modification time is
    # unknown and `_time` must be reported as NULL rather than the default epoch `1970-01-01`.
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT any(_time) IS NULL "
            "FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "1"


def test_url_wildcard_size_virtual_column():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(size) FROM ("
        "SELECT _file, any(_size) AS size "
        "FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64') "
        "GROUP BY _file)")
    )
    assert result.strip() == "8"


def test_url_wildcard_body_without_content_length():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) "
            "FROM url('http://resolver:8087/data/no_content_length_get/**/part*.tsv', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "29"


def test_url_wildcard_unknown_size_virtual_column():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x), any(_size) IS NULL "
            "FROM url('http://resolver:8087/data/unknown_size/**/part*.tsv', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "31\t1"


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


def test_url_wildcard_with_all_positional_args_and_headers():
    # Regression: the wildcard path used to validate the raw argument count before stripping the
    # optional `headers(...)` argument, so a full positional signature plus `headers(...)` was
    # wrongly rejected with NUMBER_OF_ARGUMENTS_DOESNT_MATCH even though the plain `url` path
    # accepts it. Here url + format + structure + compression + headers = 5 AST children.
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url("
        "'http://resolver:8087/data/headers/**/part*.tsv', "
        "'TSV', "
        "'x UInt64', "
        "'none', "
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
    # The HTML index lists `a/` before `b/`, so recursive `**` expansion must read
    # `a/part1.tsv` (10) before `b/part1.tsv` (20). Read single-threaded so the output
    # order reflects the listing order (`glob_expansion_max_elements` is a hard limit
    # that now raises an exception when exceeded, so it can no longer be used to
    # truncate the listing to its first element).
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT groupArray(x) FROM url('http://resolver:8087/data/order/**/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS max_threads=1, max_download_threads=1"
        )
    )
    assert result.strip() == "[10,20]"


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


def test_url_wildcard_headers_are_not_read_from_page_cache():
    reset_index_page_server_stats()

    warmup_result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x), countIf(_etag = 'mixed-part1.tsv'), countIf(_etag = 'mixed-part2.tsv') "
            "FROM url('http://resolver:8087/data/mixed_headers/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS use_page_cache_for_object_storage=1"
        )
    )
    assert warmup_result == "3\t1\t1\n"

    result = node1.query(
        with_url_wildcard_setting(
            "SELECT _file, any(_headers['X-Source-File']), any(_headers['X-Probe-Method']) "
            "FROM url('http://resolver:8087/data/mixed_headers/part*.tsv', 'TSV', 'x UInt64') "
            "GROUP BY _file "
            "ORDER BY _file "
            "SETTINGS use_page_cache_for_object_storage=1"
        )
    )
    assert result == "part1.tsv\tpart1.tsv\tGET\npart2.tsv\tpart2.tsv\tGET\n"

    stats = get_index_page_server_stats()
    assert stats["GET /data/mixed_headers/part1.tsv"] == 2
    assert stats["GET /data/mixed_headers/part2.tsv"] == 2


def test_url_wildcard_page_cache_uses_web_source_identity():
    # The two web sources (`?shard=0` / `?shard=1`) expose the same object path (`part.tsv`) with the
    # same ETag but different contents. With `use_page_cache_for_object_storage`, the page cache must
    # key on the web source identity (the shard's base URL and query), otherwise the second shard would
    # be served the first shard's cached page (the path and ETag collide) and the sum would be wrong.
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('"
            "http://resolver:8087/data/page_cache_identity/part*.tsv?shard={0,1}', "
            "'TSV', 'x UInt64') "
            "SETTINGS use_page_cache_for_object_storage=1"
        )
    )
    assert result.strip() == "303"


def test_url_wildcard_limits_directory_traversal():
    error = node1.query_and_get_error(
        with_url_wildcard_setting(
            "SELECT count() FROM url('http://resolver:8087/data/deep/**/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS url_wildcard_max_directories_to_read=3"
        )
    )
    assert "Too many directories while expanding URL wildcard" in error
    assert "url_wildcard_max_directories_to_read" in error


def test_url_wildcard_ignores_apache_sort_links():
    # An Apache `mod_autoindex` page exposes ordering anchors such as `?C=N;O=D` that resolve to the
    # current listing directory with only a different query. They must not be expanded as child
    # directories: otherwise recursive expansion re-fetches the same page and exhausts the directory
    # budget. With a tight budget the real files are still listed and read.
    # With the fix only `apache_sort/` and `subdir/` are listed (2 directories). Without it, the three
    # ordering anchors are also followed as directories, exceeding this tight budget.
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/apache_sort/**/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS url_wildcard_max_directories_to_read=3"
        )
    )
    assert result.strip() == "18"


def test_url_wildcard_preserves_index_entry_query():
    result = node1.query(
        with_url_wildcard_setting("SELECT sum(x) FROM url('http://resolver:8087/data/query_override/part*.tsv?x=1', 'TSV', 'x UInt64')")
    )
    assert result.strip() == "6"


def test_url_wildcard_file_filter_uses_visible_file_name():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT _file, sum(x) FROM url('http://resolver:8087/data/query_override/part*.tsv?x=1', 'TSV', 'x UInt64') "
            "WHERE _file = 'part1.tsv' GROUP BY _file"
        )
    )
    assert result == "part1.tsv\t6\n"


def test_url_wildcard_path_filter_uses_visible_path():
    visible_path = node1.query(
        with_url_wildcard_setting(
            "SELECT DISTINCT _path FROM url('http://resolver:8087/data/query_override/part*.tsv?x=1', 'TSV', 'x UInt64')"
        )
    ).strip()
    assert visible_path != ""
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT _path, sum(x) FROM url('http://resolver:8087/data/query_override/part*.tsv?x=1', 'TSV', 'x UInt64') "
            f"WHERE _path = '{visible_path}' GROUP BY _path"
        )
    )
    assert result == f"{visible_path}\t6\n"


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


def test_url_wildcard_deduplicates_after_successful_failover_query_inheritance():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/source_query_duplicate/**/part*.tsv?token={fail|abc}', "
            "'TSV', 'x UInt64')"
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


def test_url_wildcard_failover_resets_credentials():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://{user:pass@resolver:8087|resolver:8087}/data/auth_failover/part*.tsv', "
            "'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "23"


def test_url_query_with_literal_double_colon():
    literal_query_url = "http://resolver:8087/data/api?x=::1"
    archive_query_url = "http://resolver:8087/data/simple_archive.zip?token=x::eod.csv"

    literal_table_functions = [
        f"url('{literal_query_url}', 'TSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{literal_query_url}', 'TSV', 'x UInt64')",
    ]
    for table_function in literal_table_functions:
        assert node1.query(
            f"SELECT sum(x) FROM {table_function}",
            settings={"allow_archive_path_syntax": 1},
        ).strip() == "5"

    archive_table_functions = [
        f"url('{archive_query_url}', 'CSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{archive_query_url}', 'CSV', 'x UInt64')",
    ]
    for table_function in archive_table_functions:
        assert node1.query(
            f"SELECT sum(x) FROM {table_function}",
            settings={"allow_archive_path_syntax": 1},
        ).strip() == "3"


def test_url_writes_to_archive_paths():
    archive_url = "http://resolver:8087/data/simple_archive.zip :: eod.csv"
    for archive_path_syntax, expected_error in [
        (1, "Write into archive is not supported"),
        (0, "Bad URI syntax: URI contains invalid characters"),
    ]:
        error = node1.query_and_get_error(
            f"INSERT INTO FUNCTION url('{archive_url}', 'CSV', 'x UInt64') VALUES (1)",
            settings={"allow_archive_path_syntax": archive_path_syntax},
        )
        assert expected_error in error

    targets = [
        ("http://resolver:8087/data/simple_archive.zip::eod.csv", 1, {"allow_archive_path_syntax": 0}),
        ("http://resolver:8087/data/file.tar", 2, {}),
        ("http://resolver:8087/data/file.csv", 3, {}),
    ]
    for target_url, value, settings in targets:
        node1.query(
            f"INSERT INTO FUNCTION url('{target_url}', 'CSV', 'x UInt64') VALUES ({value})",
            settings=settings,
        )
        assert node1.query(
            f"SELECT sum(x) FROM url('{target_url}', 'CSV', 'x UInt64')",
            settings=settings,
        ).strip() == str(value)


def test_url_archive_path_braces_are_expanded_without_index_listing():
    settings = {
        "allow_experimental_url_wildcard_from_index_pages": 0,
        "glob_expansion_max_elements": 2,
    }
    source = "http://resolver:8087/data/archive_braces/{a,b}.zip :: value.tsv"

    assert node1.query(
        f"SELECT sum(x) FROM url('{source}', 'TSV', 'x UInt64')",
        settings=settings,
    ).strip() == "33"
    assert node1.query(
        f"SELECT sum(x) FROM urlCluster('test_cluster_two_shards', '{source}', 'TSV', 'x UInt64')",
        settings=settings,
    ).strip() == "33"

    error = node1.query_and_get_error(
        "SELECT sum(x) FROM url("
        "'http://resolver:8087/data/archive_braces/{a,missing}.zip :: value.tsv', "
        "'TSV', 'x UInt64')",
        settings=settings,
    )
    assert "No such file: data/archive_braces/missing.zip" in error

    oversized_source = "http://resolver:8087/data/archive_braces/archive{000000..999999}.zip :: value.tsv"
    queries = [
        f"SELECT * FROM url('{oversized_source}', 'TSV', 'x UInt64')",
        f"SELECT * FROM urlCluster('test_cluster_two_shards', '{oversized_source}', 'TSV', 'x UInt64')",
    ]
    for query in queries:
        error = node1.query_and_get_error(query, settings=settings)
        assert "first argument generates too many result addresses" in error


def test_url_archive_path_failover():
    settings = {
        "allow_experimental_url_wildcard_from_index_pages": 0,
        "glob_expansion_max_elements": 4,
    }
    single_source = "http://resolver:8087/data/archive_failover/archive{missing|good}.zip :: value.tsv"
    union_source = "http://resolver:8087/data/archive_failover/archive{0,1}{missing|good}.zip :: value.tsv"
    shard_and_failover_source = (
        "http://resolver:8087/data/archive_failover/archive{missing|good}.zip?shard={0,1} :: value.tsv"
    )

    table_functions = [
        (f"url('{single_source}', 'TSV', 'x UInt64')", "17"),
        (f"urlCluster('test_cluster_two_shards', '{single_source}', 'TSV', 'x UInt64')", "17"),
        (f"url('{union_source}', 'TSV', 'x UInt64')", "30"),
        (f"urlCluster('test_cluster_two_shards', '{union_source}', 'TSV', 'x UInt64')", "30"),
        (f"url('{shard_and_failover_source}', 'TSV', 'x UInt64')", "34"),
        (f"urlCluster('test_cluster_two_shards', '{shard_and_failover_source}', 'TSV', 'x UInt64')", "34"),
    ]
    for table_function, expected_sum in table_functions:
        assert node1.query(f"SELECT sum(x) FROM {table_function}", settings=settings).strip() == expected_sum

    for table_function, _ in table_functions[:2]:
        visible_path = node1.query(f"SELECT DISTINCT _path FROM {table_function}", settings=settings).strip()
        assert "archivegood.zip::value.tsv" in visible_path
        assert "archivemissing.zip" not in visible_path
        assert node1.query(
            f"SELECT sum(x) FROM {table_function} WHERE _path = '{visible_path}'", settings=settings
        ).strip() == "17"
        missing_path = visible_path.replace("archivegood.zip", "archivemissing.zip")
        assert node1.query(
            f"SELECT count() FROM {table_function} WHERE _path = '{missing_path}'", settings=settings
        ).strip() == "0"

    pinned_source = "http://resolver:8087/data/archive_pin/archive{primary|mirror}.zip :: value.tsv"
    pinned_table_functions = [
        f"url('{pinned_source}', 'TSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{pinned_source}', 'TSV', 'x UInt64')",
    ]
    for table_function in pinned_table_functions:
        reset_index_page_server_stats()
        result = node1.query(
            f"SELECT _path, sum(x) FROM {table_function} GROUP BY _path", settings=settings
        ).strip()
        visible_path, total = result.split("\t")
        assert "archivemirror.zip::value.tsv" in visible_path
        assert "archiveprimary.zip" not in visible_path
        assert total == "7"

    limited_settings = dict(settings)
    limited_settings["glob_expansion_max_elements"] = 3
    combined_queries = [
        f"SELECT * FROM url('{shard_and_failover_source}', 'TSV', 'x UInt64')",
        f"SELECT * FROM urlCluster('test_cluster_two_shards', '{shard_and_failover_source}', 'TSV', 'x UInt64')",
    ]
    for query in combined_queries:
        reset_index_page_server_stats()
        error = node1.query_and_get_error(query, settings=limited_settings)
        assert "first argument generates too many result addresses" in error
        assert get_index_page_server_stats() == {}

    listing_source = "http://resolver:8087/data/archive_failover/archive{missing|good}*.zip :: value.tsv"
    queries = [
        f"SELECT * FROM url('{listing_source}', 'TSV', 'x UInt64')",
        f"SELECT * FROM urlCluster('test_cluster_two_shards', '{listing_source}', 'TSV', 'x UInt64')",
    ]
    listing_settings = dict(settings)
    listing_settings["allow_experimental_url_wildcard_from_index_pages"] = 1
    for query in queries:
        error = node1.query_and_get_error(query, settings=listing_settings)
        assert "Failover patterns ('|') in the path are not supported" in error
        assert "index listing" in error


def test_url_archive_combined_expansion_limit():
    settings = {
        "allow_experimental_url_wildcard_from_index_pages": 0,
        "glob_expansion_max_elements": 2,
    }
    two_shards_one_path = (
        "http://resolver:8087/data/archive_identity/archive.zip?shard={0,1} :: value.tsv"
    )
    table_functions = [
        f"url('{two_shards_one_path}', 'TSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{two_shards_one_path}', 'TSV', 'x UInt64')",
    ]
    for table_function in table_functions:
        assert node1.query(f"SELECT sum(x) FROM {table_function}", settings=settings).strip() == "303"

    two_shards_two_paths = (
        "http://resolver:8087/data/archive_identity/archive{0,1}.zip?shard={0,1} :: value.tsv"
    )
    queries = [
        f"SELECT * FROM url('{two_shards_two_paths}', 'TSV', 'x UInt64')",
        f"SELECT * FROM urlCluster('test_cluster_two_shards', '{two_shards_two_paths}', 'TSV', 'x UInt64')",
    ]
    for query in queries:
        reset_index_page_server_stats()
        error = node1.query_and_get_error(query, settings=settings)
        assert "first argument generates too many result addresses" in error
        assert get_index_page_server_stats() == {}


def test_url_cluster_rejects_bucket_granularity_for_archives():
    error = node1.query_and_get_error(
        "SELECT sum(x) FROM urlCluster("
        "'test_cluster_two_shards', "
        "'http://resolver:8087/data/simple_archive.zip :: eod.csv', "
        "'CSV', 'x UInt64') "
        "SETTINGS cluster_table_function_split_granularity='bucket'"
    )
    assert "is not supported for reading archives" in error
    assert "cluster_table_function_split_granularity" in error


def test_url_archive_brace_paths_are_filtered_before_metadata_probe():
    settings = {
        "allow_experimental_url_wildcard_from_index_pages": 0,
        "glob_expansion_max_elements": 2,
    }
    valid_source = "http://resolver:8087/data/archive_braces/a.zip :: value.tsv"
    source_with_missing = "http://resolver:8087/data/archive_braces/{a,missing}.zip :: value.tsv"
    visible_path = node1.query(
        f"SELECT DISTINCT _path FROM url('{valid_source}', 'TSV', 'x UInt64')",
        settings=settings,
    ).strip()

    table_functions = [
        f"url('{source_with_missing}', 'TSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{source_with_missing}', 'TSV', 'x UInt64')",
    ]
    for table_function in table_functions:
        reset_index_page_server_stats()
        assert node1.query(
            f"SELECT sum(x) FROM {table_function} WHERE _path = '{visible_path}'", settings=settings
        ).strip() == "11"
        stats = get_index_page_server_stats()
        assert all("missing.zip" not in request for request in stats)

    for table_function in table_functions:
        reset_index_page_server_stats()
        assert node1.query(
            f"SELECT count() FROM {table_function} WHERE _file = 'other.tsv'", settings=settings
        ).strip() == "0"
        stats = get_index_page_server_stats()
        assert all("archive_braces" not in request for request in stats)


def test_url_archive_member_glob_infers_compression_from_matched_file():
    source = "http://resolver:8087/data/simple_archive.zip :: compressed*"
    table_functions = [
        f"url('{source}', 'CSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{source}', 'CSV', 'x UInt64')",
    ]
    for table_function in table_functions:
        assert node1.query(f"SELECT sum(x) FROM {table_function}").strip() == "9"


def test_url_cluster_archive_processing_modes_do_not_duplicate_members():
    source = "http://resolver:8087/data/multi_member_archive.zip :: *.tsv"
    for process_on_multiple_nodes in (0, 1):
        result = node1.query(
            "SELECT count(), sum(x), uniqExact(_path) FROM urlCluster("
            f"'test_cluster_two_shards', '{source}', 'TSV', 'x UInt64') "
            f"SETTINGS cluster_function_process_archive_on_multiple_nodes={process_on_multiple_nodes}"
        )
        assert result.strip() == "2\t3\t2"


def test_url_archive_without_url_wildcards():
    for archive_name in ("simple_archive.zip", "simple_archive.tar", "simple_archive.tar.gz"):
        archive_url = f"http://resolver:8087/data/{archive_name} :: eod.csv"
        assert node1.query(f"SELECT sum(c1) FROM url('{archive_url}')").strip() == "3"

    archive_url = "http://resolver:8087/data/simple_archive.zip :: eod.csv"
    assert node1.query(
        "SELECT sum(value) FROM url("
        "'http://resolver:8087/data/simple_archive.zip :: *.csv', "
        "'CSV', 'value UInt64')"
    ).strip() == "3"
    assert node1.query(
        "SELECT sum(id) FROM url("
        "'http://resolver:8087/data/simple_archive.7z :: example*.csv', "
        "'CSV', 'id UInt32, data String')"
    ).strip() == "10"
    for process_on_multiple_nodes in (0, 1):
        assert node1.query(
            "SELECT sum(value) FROM urlCluster("
            "'test_cluster_two_shards', "
            "'http://resolver:8087/data/simple_archive.zip :: *.csv', "
            "'CSV', 'value UInt64') "
            f"SETTINGS cluster_function_process_archive_on_multiple_nodes={process_on_multiple_nodes}"
        ).strip() == "3"

    assert node1.query(
        "SELECT sum(value) FROM urlCluster("
        "'test_cluster_two_shards', "
        "'http://resolver:8087/data/simple_archive.tar :: *.csv', "
        "'CSV', 'value UInt64')"
    ).strip() == "3"
    assert node1.query(
        "SELECT sum(id) FROM urlCluster("
        "'test_cluster_two_shards', "
        "'http://resolver:8087/data/simple_archive.7z :: example*.csv', "
        "'CSV', 'id UInt32, data String')"
    ).strip() == "10"
    assert node1.query(
        "SELECT sum(c1) FROM urlCluster("
        "'test_cluster_two_shards', "
        "'http://resolver:8087/data/simple_archive.zip :: eod.csv')"
    ).strip() == "3"
    assert node1.query(
        "SELECT sum(value) FROM urlCluster("
        "'test_cluster_two_shards', "
        "'http://resolver:8087/data/header_archive.zip :: eod.csv', "
        "'CSV', 'value UInt64', headers('X-Test-Header'='1'))"
    ).strip() == "3"

    table_name = "url_archive_empty_as"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    try:
        node1.query(
            f"CREATE TABLE {table_name} ORDER BY () EMPTY AS "
            f"SELECT * FROM url('{archive_url}')"
        )
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "0"
        assert node1.query(f"DESCRIBE TABLE {table_name}") == TSV([["c1", "Nullable(Int64)"]])
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_url_wildcard_archive_metadata_uses_shard_identity():
    result = node1.query(
        "SELECT sum(x) FROM url('"
        "http://resolver:8087/data/archive_identity/archive.zip?shard={0,1} :: value.tsv', "
        "'TSV', 'x UInt64')"
    )
    assert result.strip() == "303"

    result = node1.query(
        "SELECT sum(x) FROM urlCluster('test_cluster_two_shards', '"
        "http://resolver:8087/data/archive_identity/archive.zip?shard={0,1} :: value.tsv', "
        "'TSV', 'x UInt64')"
    )
    assert result.strip() == "303"

    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('"
            "http://resolver:8087/data/archive_identity/archive*.zip?shard={0,1} :: value.tsv', "
            "'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "303"

    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM urlCluster('test_cluster_two_shards', '"
            "http://resolver:8087/data/archive_identity/archive*.zip?shard={0,1} :: value*.tsv', "
            "'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "303"


def test_url_wildcard_rejects_unknown_size_archive():
    error = node1.query_and_get_error(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('"
            "http://resolver:8087/data/unknown_size_archive/archive*.zip :: value.tsv', "
            "'TSV', 'x UInt64')"
        )
    )
    assert "Cannot read archive" in error
    assert "size is unknown" in error


def test_url_wildcard_ignores_failed_failover_option_after_success():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/source_query/**/part*.tsv?token={fail|abc}', 'TSV', 'x UInt64')"
        )
    )
    assert result.strip() == "13"


def test_url_wildcard_does_not_ignore_missing_shard():
    error = node1.query_and_get_error(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('"
            "http://resolver:8087/data/source_query/**/part*.tsv?token={bad,abc}', "
            "'TSV', 'x UInt64')"
        )
    )
    assert "There is no path" in error


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


def test_url_wildcard_rejects_cross_origin_index_redirect():
    reset_index_page_server_stats()
    reset_index_page_server_stats(8088)

    error = node1.query_and_get_error(
        with_url_wildcard_setting(
            "SELECT count() FROM url("
            "'http://user:pass@resolver:8087/data/cross_origin_redirect/**/part*.tsv', "
            "'TSV', "
            "'x UInt64', "
            "headers('X-Test-Header'='1')) "
            "SETTINGS max_http_get_redirects=1"
        )
    )
    assert "redirected to a different origin" in error

    redirect_target_stats = get_index_page_server_stats(8088)
    assert "GET /data/cross_origin_target/" not in redirect_target_stats


def test_url_wildcard_reads_index_file_redirect():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/index_redirect/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS max_http_get_redirects=1"
        )
    )
    assert result.strip() == "41"


def test_url_wildcard_preserves_redirected_recursive_match_path():
    result = node1.query(
        with_url_wildcard_setting(
            "SELECT sum(x) FROM url('http://resolver:8087/data/recursive_redirect/**/part*.tsv', 'TSV', 'x UInt64') "
            "SETTINGS max_http_get_redirects=1"
        )
    )
    assert result.strip() == "43"


def test_url_engine_wildcard_redirect_uses_query_setting():
    table_name = "url_wildcard_redirect"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    try:
        node1.query(
            f"CREATE TABLE {table_name} (x UInt64) "
            "ENGINE = URL('http://resolver:8087/data/redirect/part*.tsv', 'TSV')",
            settings={"allow_experimental_url_wildcard_from_index_pages": 1},
        )

        error = node1.query_and_get_error(
            f"SELECT sum(x) FROM {table_name} "
            "SETTINGS allow_experimental_url_wildcard_from_index_pages=1, max_http_get_redirects=0"
        )
        assert "Too many redirects while trying to access" in error

        result = node1.query(
            f"SELECT sum(x) FROM {table_name} "
            "SETTINGS allow_experimental_url_wildcard_from_index_pages=1, max_http_get_redirects=1"
        )
        assert result.strip() == "19"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_url_wildcard_is_experimental():
    settings = {"allow_experimental_url_wildcard_from_index_pages": 0}

    error = node1.query_and_get_error(
        "SELECT sum(x) FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')",
        settings=settings,
    )
    assert "allow_experimental_url_wildcard_from_index_pages" in error

    error = node1.query_and_get_error(
        "SELECT sum(x) FROM urlCluster("
        "'test_cluster_two_shards', 'http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')",
        settings=settings,
    )
    assert "allow_experimental_url_wildcard_from_index_pages" in error


def test_table_function_url_access_rights():
    node1.query("CREATE USER OR REPLACE u1")

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        "SELECT * FROM url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    )

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        "SELECT * FROM url('http://nginx:80/test_1', 'TSV')", user="u1"
    )

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        "DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    )

    expected_error = "necessary to have the grant READ ON URL"
    assert expected_error in node1.query_and_get_error(
        "DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV')", user="u1"
    )

    node1.query("GRANT READ ON URL TO u1")
    assert node1.query(
        "DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV')",
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

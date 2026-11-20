import gzip
import pytest
import time
import uuid
import os
import json

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)
URL_WILDCARD_EXPERIMENTAL_SETTING = "allow_url_wildcard_from_index_pages=1"
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/conf.xml", "configs/named_collections.xml", "configs/query_log.xml"],
    user_configs=["configs/users.xml"],
    with_nginx=True,
    with_minio=True,
)
# The worker of `test_cluster_node2`; `forbidden_authorization` changes only node1's configuration.
node2 = cluster.add_instance("node2", main_configs=["configs/conf.xml"])

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


def server_part_of_error(error):
    # The client appends "(query: ...)" with the query text as sent, credentials included.
    return error.split("(query: ")[0]


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
        settings={"allow_url_wildcard_from_index_pages": 1},
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
        with_url_wildcard_setting("SELECT count() FROM url('http://user:pass@resolver:8087/data/oversize/**/part*.tsv', 'TSV', 'x UInt64')")
    )
    assert "exceeds max_http_index_page_size" in error
    server_error = server_part_of_error(error)
    assert "user:pass@" not in server_error
    assert "'http://[HIDDEN]@resolver:8087/data/oversize/'" in server_error


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


def test_url_path_with_literal_double_colon_requires_archive_syntax_off():
    literal_url = "http://resolver:8087/data/data.zip::v1"
    table_functions = [
        f"url('{literal_url}', 'TSV', 'x UInt64')",
        f"urlCluster('test_cluster_two_shards', '{literal_url}', 'TSV', 'x UInt64')",
    ]
    for table_function in table_functions:
        assert node1.query(
            f"SELECT sum(x) FROM {table_function}",
            settings={"allow_archive_path_syntax": 0},
        ).strip() == "7"

    # The other half of the contract: with the setting on, `data.zip::v1` is archive syntax even though
    # the member name has no extension. The mock serves the member as `9`, the literal path as `7`.
    for table_function in table_functions:
        assert node1.query(
            f"SELECT sum(x) FROM {table_function}",
            settings={"allow_archive_path_syntax": 1},
        ).strip() == "9"


def test_url_writes_to_archive_paths():
    # The POSTed paths are served before the archive data until the server is reset, so leave none behind
    # for the tests that run after this one, even when an assertion below fails.
    try:
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
    finally:
        reset_index_page_server_stats()


def test_url_archive_member_size_is_the_one_in_the_archive():
    # `_size` of a member is its size inside the archive: for `compressed.csv.gz` that is the gzip stream,
    # not the four bytes it expands to.
    assert node1.query(
        "SELECT any(_size) FROM url("
        "'http://resolver:8087/data/simple_archive.zip :: eod.csv', 'CSV', 'x UInt64')"
    ).strip() == "4"
    compressed_size = node1.query(
        "SELECT any(_size) FROM url("
        "'http://resolver:8087/data/simple_archive.zip :: compressed.csv.gz', 'CSV', 'x UInt64')"
    ).strip()
    # The gzip stream of `4\n5\n` is a fixed 24 bytes, longer than the four it expands to, so the exact value
    # pins that `_size` is the stored size rather than the expanded one.
    assert int(compressed_size) == len(gzip.compress(b"4\n5\n"))

    # A `DEFLATE` entry stores fewer bytes than it reports: `_size` is the uncompressed size, which is what
    # the reader has to hand out, not the size the container holds.
    deflated = node1.query(
        "SELECT any(_size) FROM url("
        "'http://resolver:8087/data/deflated_archive.zip :: value.tsv', 'CSV', 'x UInt64')"
    ).strip()
    assert int(deflated) == 1000


def test_url_archive_combined_expansion_limit():
    settings = {
        "allow_url_wildcard_from_index_pages": 0,
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

    # One shard too many for the limit: the product of the URL shards and the literal path is bounded
    # before anything is reserved, and no index page is listed for a literal path.
    too_many_shards = (
        "http://resolver:8087/data/archive_identity/archive.zip?shard={0,1,2} :: value.tsv"
    )
    queries = [
        f"SELECT * FROM url('{too_many_shards}', 'TSV', 'x UInt64')",
        f"SELECT * FROM urlCluster('test_cluster_two_shards', '{too_many_shards}', 'TSV', 'x UInt64')",
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
    split_query_id = "url_cluster_archive_split_across_shards"
    for process_on_multiple_nodes in (0, 1):
        result = node1.query(
            "SELECT count(), sum(x), uniqExact(_path) FROM urlCluster("
            f"'test_cluster_two_shards', '{source}', 'TSV', 'x UInt64') "
            f"SETTINGS cluster_function_process_archive_on_multiple_nodes={process_on_multiple_nodes}",
            query_id=split_query_id if process_on_multiple_nodes else None,
        )
        assert result.strip() == "2\t3\t2"

    # The tasks really reach the workers: with the archive split across the shards, each of them runs a
    # secondary query for it. The assertion is bound to the `=1` query, because the `=0` round sends the
    # whole archive and would satisfy a plain "some secondary query mentions the archive" check as well.
    node1.query("SYSTEM FLUSH LOGS")
    forwarded = int(
        node1.query(
            "SELECT count() FROM system.query_log WHERE is_initial_query = 0 "
            "AND event_date >= today() - 1 AND type = 'QueryFinish' "
            f"AND initial_query_id = '{split_query_id}'"
        ).strip()
    )
    assert forwarded > 0, "no secondary query for the archive was dispatched"


def test_url_archive_failure_modes():
    # A member that is not in the archive and a member glob that matches nothing are both an empty read,
    # like a glob over paths that matches nothing and like `file`/`s3` with the same source.
    assert node1.query(
        "SELECT count() FROM url('http://resolver:8087/data/simple_archive.zip :: missing.csv', 'CSV', 'c1 String')"
    ).strip() == "0"
    assert node1.query(
        "SELECT count() FROM url('http://resolver:8087/data/simple_archive.zip :: *.none', 'CSV', 'c1 String')"
    ).strip() == "0"

    # A path that ends with an archive suffix but does not hold an archive, and a truncated one, both have
    # to be reported as an unreadable archive rather than be read as the table format.

    error = node1.query_and_get_error(
        "SELECT * FROM url('http://resolver:8087/data/not_an_archive.zip :: value.tsv', 'CSV', 'c1 String')"
    )
    assert "CANNOT_UNPACK_ARCHIVE" in error, error
    assert "not_an_archive.zip" in error, error

    error = node1.query_and_get_error(
        "SELECT * FROM url('http://resolver:8087/data/truncated_archive.zip :: value.tsv', 'CSV', 'c1 String')"
    )
    assert "CANNOT_UNPACK_ARCHIVE" in error, error
    assert "truncated_archive.zip" in error, error


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
    # A fixed member in whole-archive mode: the initiator sends one task per archive and drops the ones that
    # do not hold the member, so the member is read exactly once and a non-matching archive is skipped.
    for process_on_multiple_nodes in (0, 1):
        assert node1.query(
            "SELECT sum(c1) FROM urlCluster("
            "'test_cluster_two_shards', "
            "'http://resolver:8087/data/simple_archive.zip :: eod.csv', 'CSV') "
            f"SETTINGS cluster_function_process_archive_on_multiple_nodes={process_on_multiple_nodes}"
        ).strip() == "3"

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
            settings={"allow_url_wildcard_from_index_pages": 1},
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
            settings={"allow_url_wildcard_from_index_pages": 1},
        )

        error = node1.query_and_get_error(
            f"SELECT count() FROM {table_name} "
            "SETTINGS allow_url_wildcard_from_index_pages=1, url_wildcard_max_directories_to_read=3"
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
    server_error = server_part_of_error(error)
    assert "user:pass@" not in server_error
    assert "redirect_pass" not in server_error
    assert "'http://[HIDDEN]@resolver:8087/data/cross_origin_redirect/'" in server_error
    assert "'http://[HIDDEN]@resolver:8088/data/cross_origin_target/'" in server_error

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
            settings={"allow_url_wildcard_from_index_pages": 1},
        )

        error = node1.query_and_get_error(
            f"SELECT sum(x) FROM {table_name} "
            "SETTINGS allow_url_wildcard_from_index_pages=1, max_http_get_redirects=0"
        )
        assert "Too many redirects while trying to access" in error

        result = node1.query(
            f"SELECT sum(x) FROM {table_name} "
            "SETTINGS allow_url_wildcard_from_index_pages=1, max_http_get_redirects=1"
        )
        assert result.strip() == "19"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_url_wildcard_is_experimental():
    settings = {"allow_url_wildcard_from_index_pages": 0}

    error = node1.query_and_get_error(
        "SELECT sum(x) FROM url('http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')",
        settings=settings,
    )
    assert "allow_url_wildcard_from_index_pages" in error

    error = node1.query_and_get_error(
        "SELECT sum(x) FROM urlCluster("
        "'test_cluster_two_shards', 'http://resolver:8087/data/**/part*.tsv', 'TSV', 'x UInt64')",
        settings=settings,
    )
    assert "allow_url_wildcard_from_index_pages" in error


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

    # Like `SELECT`, `DESCRIBE` of a table function also needs `CREATE TEMPORARY TABLE`.
    node1.query("GRANT READ ON URL, CREATE TEMPORARY TABLE ON *.* TO u1")
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


def test_partitioned_write_filtered_grant():
    """
     A filtered `WRITE` grant is checked against the URI template of the table function, but the partition id
     substituted into `{_partition_id}` comes from the data. A partition id with `../` must not let the write
     leave the granted prefix, for `url` (Nginx resolves the dot segments) as well as for `s3`.
     Example:
        INSERT INTO FUNCTION url('http://nginx/allowed/{_partition_id}.tsv', 'TSV', 'p String, v UInt32')
        PARTITION BY p
        VALUES ('a', 1), ('../protected/owned', 2);
    """

    suffix = uuid.uuid4().hex  # suffix used for all random names
    user = f"partition_writer_{suffix}"
    table = f"partitioned_s3_table_{suffix}"
    function_table = f"s3_function_table_{suffix}"
    user_function_table = f"s3_function_table_by_user_{suffix}"
    allowed = f"allowed_{suffix}"
    protected = f"protected_{suffix}"
    minio_url = f"http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}"
    columns = "column1 UInt32, column2 UInt32, column3 UInt32"

    # The path is a template: ClickHouse replaces `{_partition_id}` in it with the value of `PARTITION BY` for each row.
    partition_template = f"{allowed}/{{_partition_id}}.tsv"
    # The only row is `(1, 2, 3)`, so `column1` is 1.
    safe_partition = "PARTITION BY toString(column1)"  # -> "1", written to `allowed_.../1.tsv`
    # partition id that traverses the protected prefix and should be refused
    traversal_partition = f"PARTITION BY concat('../{protected}/owned_', toString(column1))"  # -> "../protected_.../owned_1"

    def insert_url_error(path, partition_by=""):
        """Insert the row into `http://nginx:80/<path>` as the user; return the error text, empty on success."""
        _, error = node1.query_and_get_answer_with_error(
            f"INSERT INTO FUNCTION url(url_file, url = 'http://nginx:80/{path}', format = 'TSV') "
            f"{partition_by} VALUES (1, 2, 3)",
            user=user,
        )
        return error

    def insert_s3_error(path, partition_by="", settings=None):
        """Insert the row into `<bucket>/<path>` on MinIO as the user; return the error text, empty on success."""
        _, error = node1.query_and_get_answer_with_error(
            f"INSERT INTO FUNCTION s3('{minio_url}/{path}', 'minio', '{cluster.minio_secret_key}', 'TSV', '{columns}') "
            f"{partition_by} VALUES (1, 2, 3)",
            settings=settings,
            user=user,
        )
        return error

    def read_url_data(path):
        """Read `http://nginx:80/<path>` as `default`; return its rows as TSV, or `None` if it does not exist."""
        data, error = node1.query_and_get_answer_with_error(
            f"SELECT * FROM url('http://nginx:80/{path}', 'TSV', '{columns}')"
        )
        return None if error else data

    def s3_keys(prefix, bucket=cluster.minio_bucket):
        """List the keys under `prefix` in a MinIO bucket."""
        return [obj.object_name for obj in cluster.minio_client.list_objects(bucket, prefix=prefix, recursive=True)]

    node1.query(f"CREATE USER {user}")
    node1.query(f"GRANT CREATE TEMPORARY TABLE ON *.*, NAMED COLLECTION ON url_file TO {user}")
    # The filter is matched against the normalized URI, which omits the default port of the scheme.
    node1.query(f"GRANT WRITE ON URL('^http://nginx/{allowed}/.*$') TO {user}")
    node1.query(f"GRANT WRITE ON S3('^{minio_url}/{allowed}/.*$') TO {user}")
    # Keys starting with `allowed_<suffix>-`, so that `{_partition_id}` can be in the first segment of the key.
    node1.query(f"GRANT WRITE ON S3('^{minio_url}/{allowed}-.*$') TO {user}")
    # A grant that allows the typed file name `{_schema_hash}.tsv`, because it contains `schema`, but not the name it expands
    # to, which has only hex digits, so that a write is allowed only if the typed name is the one checked.
    schema_dir = f"schema_{suffix}"
    node1.query(f"GRANT WRITE ON S3('^{minio_url}/{schema_dir}/[^/]*schema[^/]*\\\\.tsv$') TO {user}")
    # Only file names without a dot, such as the template `{_partition_id}.tsv` and `1.tsv`, in a directory with a dot,
    # `versioned.<suffix>/`.
    versioned = f"versioned.{suffix}"
    node1.query(f"GRANT WRITE ON S3('^{minio_url}/versioned\\\\.{suffix}/[^./]+\\\\.tsv$') TO {user}")

    try:
        # Happy path: `url`: a partition inside the granted prefix is written.
        assert insert_url_error(partition_template, safe_partition) == ""
        assert read_url_data(f"{allowed}/1.tsv") == "1\t2\t3\n"

        # Reject: a path outside the grant is refused.
        assert "Not enough privileges" in insert_url_error(f"{protected}/direct.tsv")

        # Reject: a partition id that leaves the granted prefix is refused too, and nothing is written there.
        assert "Not enough privileges" in insert_url_error(partition_template, traversal_partition)
        assert read_url_data(f"{protected}/owned_1.tsv") is None

        # Happy path: `s3`: the same three cases.
        assert insert_s3_error(partition_template, safe_partition) == ""
        assert s3_keys(f"{allowed}/") == [f"{allowed}/1.tsv"]

        # Reject: a path outside the grant is refused.
        assert "Not enough privileges" in insert_s3_error(f"{protected}/direct.tsv")

        # Reject: a partition id that leaves the granted prefix is refused too, and nothing is written there.
        assert "Not enough privileges" in insert_s3_error(partition_template, traversal_partition)
        assert s3_keys(f"{protected}/") == []

        # Reject: the same, with `{_partition_id}` percent-encoded in the URI. `s3` decodes the key, so the encoded
        # placeholder is still replaced with the partition id when writing.
        encoded_template = f"{allowed}/%7B_partition_id%7D.tsv"
        assert "Not enough privileges" in insert_s3_error(encoded_template, traversal_partition)
        assert s3_keys(f"{protected}/") == []

        # Reject: an existing object is not overwritten but renamed by `s3_create_new_file_on_insert`, and the new key
        # is outside the grant. Depending on the version, `.1` is inserted before the first dot of the key or of the
        # file name, so `versioned.<suffix>/1.tsv` becomes `versioned.1.<suffix>/1.tsv` or `versioned.<suffix>/1.1.tsv`.
        versioned_template = f"{versioned}/{{_partition_id}}.tsv"
        assert insert_s3_error(versioned_template, safe_partition) == ""
        assert s3_keys(f"{versioned}/") == [f"{versioned}/1.tsv"]
        assert "Not enough privileges" in insert_s3_error(
            versioned_template, safe_partition, settings={"s3_create_new_file_on_insert": 1}
        )
        assert s3_keys(f"{versioned}/") == [f"{versioned}/1.tsv"]
        assert s3_keys(f"versioned.1.{suffix}/") == []

        # Reject: the same rename for an unpartitioned write, whose typed key `versioned.<suffix>/1.tsv` is inside the grant.
        assert "Not enough privileges" in insert_s3_error(f"{versioned}/1.tsv", settings={"s3_create_new_file_on_insert": 1})
        assert s3_keys(f"{versioned}/") == [f"{versioned}/1.tsv"]
        assert s3_keys(f"versioned.1.{suffix}/") == []

        # Reject: an unpartitioned write whose key is expanded after the source access check: the typed
        # `schema_<suffix>/{_schema_hash}.tsv` is inside the grant, but `schema_<suffix>/<hash>.tsv`, which is written, is not.
        assert "Not enough privileges" in insert_s3_error(f"{schema_dir}/{{_schema_hash}}.tsv")
        assert s3_keys(f"{schema_dir}/") == []

        # Reject: a partition id that makes the key fully qualified, `allowed_<suffix>-s3://root2/owned_<suffix>_1.tsv`,
        # which the object storage would write as `owned_<suffix>_1.tsv` into the bucket `root2`, while the checked URI
        # is in the granted bucket. A plain partition id with the same template is written.
        qualified_template = f"{allowed}-{{_partition_id}}.tsv"
        assert insert_s3_error(qualified_template, safe_partition) == ""
        assert s3_keys(f"{allowed}-") == [f"{allowed}-1.tsv"]
        other_bucket_partition = f"PARTITION BY concat('s3://{cluster.minio_bucket_2}/owned_{suffix}_', toString(column1))"
        assert "refers to another bucket" in insert_s3_error(qualified_template, other_bucket_partition)
        assert s3_keys(f"owned_{suffix}_", bucket=cluster.minio_bucket_2) == []

        # Reject: the same fully qualified key typed in an unpartitioned write, also percent-encoded. The typed URI
        # matches the grant `^<bucket>/allowed_<suffix>-.*$` as a string, but the key would go to the bucket `root2`.
        # Such a key always contains `//`, which the parser of S3 URIs refuses after decoding, whatever the grants,
        # so only a partition id can make a key fully qualified.
        for separator in ("://", "%3A%2F%2F"):
            qualified_key = f"{allowed}-s3{separator}{cluster.minio_bucket_2}/owned_{suffix}_direct.tsv"
            assert "Invalid S3 key" in insert_s3_error(qualified_key)
            assert s3_keys(f"owned_{suffix}_", bucket=cluster.minio_bucket_2) == []

        # A partitioned table with the `S3` engine is not checked against the source grants of the user who
        # inserts into it, so it keeps working with a destination outside the prefix the user could write to.
        # (The `URL` engine does not support `PARTITION BY`.)
        node1.query(
            f"CREATE TABLE {table} ({columns}) "
            f"ENGINE = S3('{minio_url}/{protected}/{{_partition_id}}.tsv', 'minio', '{cluster.minio_secret_key}', 'TSV') "
            f"PARTITION BY toString(column1)"
        )
        node1.query(f"GRANT INSERT ON {table} TO {user}")
        node1.query(f"INSERT INTO {table} VALUES (1, 2, 3)", user=user)
        assert s3_keys(f"{protected}/") == [f"{protected}/1.tsv"]

        # The same for a table created by an administrator as `s3(...)`: the user needs only `INSERT` on the table.
        node1.query(
            f"CREATE TABLE {function_table} ({columns}) "
            f"AS s3('{minio_url}/{protected}/function_table.tsv', 'minio', '{cluster.minio_secret_key}', 'TSV')"
        )
        node1.query(f"GRANT INSERT ON {function_table} TO {user}")
        node1.query(f"INSERT INTO {function_table} VALUES (1, 2, 3)", user=user)
        assert s3_keys(f"{protected}/function_table") == [f"{protected}/function_table.tsv"]

        # Reject: creating a table as `s3(...)` needs the `TABLE ENGINE` grant of `S3`, like `ENGINE = S3`, which
        # a filtered grant does not imply, even for a URI inside it: the table could then write outside the grant.
        node1.query(f"GRANT CREATE TABLE ON default.* TO {user}")
        _, error = node1.query_and_get_answer_with_error(
            f"CREATE TABLE {user_function_table} ({columns}) "
            f"AS s3('{minio_url}/{allowed}/function_table.tsv', 'minio', '{cluster.minio_secret_key}', 'TSV')",
            user=user,
        )
        assert "Not enough privileges" in error and "TABLE ENGINE" in error
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table}")
        node1.query(f"DROP TABLE IF EXISTS {function_table}")
        node1.query(f"DROP TABLE IF EXISTS {user_function_table}")
        node1.query(f"DROP USER IF EXISTS {user}")


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


CONF_PATH = "/etc/clickhouse-server/config.d/conf.xml"
FORBIDDEN_HEADER = "<header>X-Forbidden-Url-Wildcard</header>"


class forbidden_authorization:
    """Forbids the `Authorization` header in `http_forbid_headers` for the duration of the block."""

    def __enter__(self):
        node1.replace_in_config(
            CONF_PATH, FORBIDDEN_HEADER, FORBIDDEN_HEADER + "<header>Authorization</header>"
        )
        node1.query("SYSTEM RELOAD CONFIG")

    def __exit__(self, *args):
        node1.replace_in_config(CONF_PATH, "<header>Authorization</header>", "")
        node1.query("SYSTEM RELOAD CONFIG")


def authorization_requests(port=8087):
    return get_index_page_server_stats(port).get("Authorization", 0)


def assert_authorization_forbidden(error, secret="user:pass"):
    assert 'HTTP header "Authorization" is forbidden' in error, error
    assert secret not in server_part_of_error(error), error


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            "SELECT sum(x) FROM url('http://user:pass@resolver:8087/data/2025/part1.tsv', 'TSV', 'x UInt64')",
            id="userinfo",
        ),
        # The first option is unreachable, so the read moves on to the second one.
        pytest.param(
            "SELECT sum(x) FROM url("
            "'http://127.0.0.1:1/data/2025/part1.tsv|http://user:pass@resolver:8087/data/2025/part1.tsv', "
            "'TSV', 'x UInt64') SETTINGS http_max_tries = 1",
            id="failover",
        ),
        # The second option is usable, so the read must not move on to it.
        pytest.param(
            "SELECT sum(x) FROM url("
            "'http://user:pass@resolver:8087/data/2025/part1.tsv|http://resolver:8087/data/2025/part1.tsv', "
            "'TSV', 'x UInt64') SETTINGS http_max_tries = 1",
            id="failover_first",
        ),
        pytest.param(
            "SELECT sum(x) FROM urlCluster('test_cluster_two_shards', "
            "'http://user:pass@resolver:8087/data/2025/part1.tsv', 'TSV', 'x UInt64')",
            id="url_cluster",
        ),
        pytest.param(
            with_url_wildcard_setting(
                "SELECT sum(x) FROM url('http://user:pass@resolver:8087/data/2025/part*.tsv', 'TSV', 'x UInt64')"
            ),
            id="wildcard",
        ),
        # Requests with the `Authorization` header get 403 there, so the read moves on to the second option.
        pytest.param(
            with_url_wildcard_setting(
                "SELECT sum(x) FROM url('http://{user:pass@resolver:8087|resolver:8087}/data/auth_failover/part*.tsv', "
                "'TSV', 'x UInt64')"
            ),
            id="wildcard_failover_first",
        ),
    ],
)
def test_url_userinfo_with_forbidden_authorization(query):
    # The userinfo is sent as Basic credentials in the `Authorization` header.
    reset_index_page_server_stats()
    node1.query(query)
    assert authorization_requests() > 0

    reset_index_page_server_stats()
    with forbidden_authorization():
        assert_authorization_forbidden(node1.query_and_get_error(query))
    assert authorization_requests() == 0


def test_url_cluster_userinfo_refused_by_initiator():
    # node1 refuses the URL before node2, which does not forbid the header, gets it. The userinfo is in the second
    # failover option, so the URL as a whole has none.
    query = (
        "SELECT sum(x) FROM urlCluster('test_cluster_node2', "
        "'http://127.0.0.1:1/data/2025/part1.tsv|http://user:pass@resolver:8087/data/2025/part1.tsv', "
        "'TSV', 'x UInt64') SETTINGS http_max_tries = 1"
    )
    reset_index_page_server_stats()
    assert node1.query(query) == "3\n"
    assert authorization_requests() > 0

    reset_index_page_server_stats()
    with forbidden_authorization():
        assert_authorization_forbidden(node1.query_and_get_error(query))
    assert authorization_requests() == 0


def test_url_userinfo_without_username_with_forbidden_authorization():
    # Credentials without a username are not sent, so they are not refused.
    reset_index_page_server_stats()
    with forbidden_authorization():
        assert (
            node1.query("SELECT sum(x) FROM url('http://:pass@resolver:8087/data/2025/part1.tsv', 'TSV', 'x UInt64')")
            == "3\n"
        )
    assert authorization_requests() == 0


def test_stored_url_wildcard_table_with_forbidden_authorization():
    # A stored table loads after the header is forbidden, but its reads do not send the userinfo.
    table_name = "url_wildcard_userinfo"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    node1.query(
        f"CREATE TABLE {table_name} (x UInt64) "
        "ENGINE = URL('http://user:pass@resolver:8087/data/2025/part*.tsv', 'TSV')",
        settings={"allow_url_wildcard_from_index_pages": 1},
    )
    try:
        reset_index_page_server_stats()
        assert node1.query(f"SELECT sum(x) FROM {table_name}") == "12\n"
        assert authorization_requests() > 0

        with forbidden_authorization():
            node1.query(f"DETACH TABLE {table_name}")
            node1.query(f"ATTACH TABLE {table_name}")
            reset_index_page_server_stats()
            assert_authorization_forbidden(node1.query_and_get_error(f"SELECT sum(x) FROM {table_name}"))
            assert authorization_requests() == 0
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_web_index_disk_userinfo_with_forbidden_authorization():
    # Creating a table on a `web_index` disk probes `format_version.txt` with a HEAD request, before any listing.
    table_name = "web_index_userinfo"
    query = (
        f"CREATE TABLE {table_name} (x UInt64) ENGINE = MergeTree ORDER BY x "
        "SETTINGS disk = disk(type = web, endpoint = 'http://user:pass@resolver:8087/data/web_index_userinfo/', "
        "metadata_type = web_index)"
    )
    try:
        reset_index_page_server_stats()
        node1.query(query)
        assert authorization_requests() > 0
        node1.query(f"DROP TABLE {table_name} SYNC")

        reset_index_page_server_stats()
        with forbidden_authorization():
            assert_authorization_forbidden(node1.query_and_get_error(query))
        assert authorization_requests() == 0
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_url_redirect_userinfo_is_not_sent_with_forbidden_authorization():
    # The redirect has a userinfo in its `Location`, which does not become the credentials of the request.
    reset_index_page_server_stats(8088)
    with forbidden_authorization():
        node1.query(
            "SELECT * FROM url('http://resolver:8087/data/cross_origin_redirect/', 'LineAsString') "
            "SETTINGS max_http_get_redirects = 1"
        )
    stats = get_index_page_server_stats(8088)
    assert stats["GET /data/cross_origin_target/"] > 0
    assert "Authorization" not in stats


def test_http_dictionary_credentials_with_forbidden_authorization():
    def create_dictionary(name):
        node1.query(
            f"CREATE DICTIONARY {name} (id UInt64, name String) PRIMARY KEY id "
            "SOURCE(HTTP(url 'http://resolver:8087/data/dictionary/data.tsv' format 'TabSeparated' "
            "credentials(user 'user' password 'dict_secret_pw'))) "
            "LIFETIME(0) LAYOUT(FLAT())"
        )

    allowed, forbidden = "http_credentials_allowed", "http_credentials_forbidden"
    try:
        create_dictionary(allowed)
        reset_index_page_server_stats()
        assert node1.query(f"SELECT dictGet('{allowed}', 'name', toUInt64(1))") == "one\n"
        assert authorization_requests() > 0

        with forbidden_authorization():
            create_dictionary(forbidden)
            reset_index_page_server_stats()
            assert_authorization_forbidden(
                node1.query_and_get_error(f"SELECT dictGet('{forbidden}', 'name', toUInt64(1))"),
                secret="dict_secret_pw",
            )
            assert authorization_requests() == 0
    finally:
        node1.query(f"DROP DICTIONARY IF EXISTS {allowed}")
        node1.query(f"DROP DICTIONARY IF EXISTS {forbidden}")


def test_loaded_http_dictionary_credentials_with_forbidden_authorization():
    # A loaded dictionary keeps its source across its updates, so the credentials are checked per request.
    name = "http_credentials_loaded"
    node1.query(
        f"CREATE DICTIONARY {name} (id UInt64, name String) PRIMARY KEY id "
        "SOURCE(HTTP(url 'http://resolver:8087/data/dictionary/data.tsv' format 'TabSeparated' "
        "credentials(user 'user' password 'dict_secret_pw'))) "
        "LIFETIME(MIN 1 MAX 1) LAYOUT(FLAT())"
    )
    try:
        reset_index_page_server_stats()
        assert node1.query(f"SELECT dictGet('{name}', 'name', toUInt64(1))") == "one\n"
        assert authorization_requests() > 0

        with forbidden_authorization():
            assert_eq_with_retry(
                node1,
                f"SELECT last_exception LIKE '%Authorization%is forbidden%' FROM system.dictionaries WHERE name = '{name}'",
                "1",
                retry_count=60,
                sleep_time=0.5,
            )
    finally:
        node1.query(f"DROP DICTIONARY IF EXISTS {name}")


@pytest.mark.parametrize(
    "registry_url",
    [
        pytest.param("http://user:pass@resolver:8087/", id="userinfo"),
        # Without an authority the URL has no userinfo, but the schema is requested from
        # `http://user:pass@resolver:8087/schemas/ids/1`, which its path resolves to.
        pytest.param("http:http://user:pass@resolver:8087/", id="without_authority"),
    ],
)
def test_avro_schema_registry_userinfo_with_forbidden_authorization(registry_url):
    # The data is the magic byte and the schema id 1, so the schema is requested. The server has no
    # schema registry, so the request fails anyway: only the header it was sent with matters.
    query = (
        "SELECT * FROM format(AvroConfluent, 'a UInt8', '\\x00\\x00\\x00\\x00\\x01') "
        f"SETTINGS format_avro_schema_registry_url = '{registry_url}'"
    )
    reset_index_page_server_stats()
    node1.query_and_get_error(query)
    assert authorization_requests() > 0

    reset_index_page_server_stats()
    with forbidden_authorization():
        assert_authorization_forbidden(node1.query_and_get_error(query))
    assert authorization_requests() == 0


def test_avro_schema_registry_output_userinfo_with_forbidden_authorization():
    # Writing `AvroConfluent` registers the schema, with the userinfo of the registry URL as Basic credentials.
    # Over HTTP the server writes the output, `clickhouse client` would write it itself.
    query = (
        "SELECT toInt64(1) AS a FORMAT AvroConfluent "
        "SETTINGS format_avro_schema_registry_url = 'http://user:pass@resolver:8087/', "
        "output_format_avro_confluent_subject = 'subject'"
    )
    with forbidden_authorization():
        assert_authorization_forbidden(node1.http_query_and_get_error(query))

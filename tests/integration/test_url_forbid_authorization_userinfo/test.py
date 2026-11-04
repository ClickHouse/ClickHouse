import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# `configs/config.xml` forbids the `Authorization` header in `http_forbid_headers`.
node = cluster.add_instance("node", main_configs=["configs/config.xml"])
CONFIG_PATH = "/etc/clickhouse-server/config.d/config.xml"

USER = "url_userinfo_user"
PASSWORD = "url_userinfo_password"
# The server reads from its own HTTP interface, so a request sent with the credentials
# would run the query as `USER`.
QUERY_URL = "127.0.0.1:8123/?query=SELECT+currentUser()"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query(f"CREATE USER {USER} IDENTIFIED BY '{PASSWORD}'")
        yield cluster
    finally:
        cluster.shutdown()


def assert_forbidden_without_requests(query):
    error = node.query_and_get_error(query)
    assert "BAD_ARGUMENTS" in error, error
    assert 'HTTP header "Authorization" is forbidden' in error, error
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        node.query(f"SELECT count() FROM system.query_log WHERE user = '{USER}'")
        == "0\n"
    )
    return error


def test_url_without_credentials():
    assert (
        node.query(f"SELECT * FROM url('http://{QUERY_URL}', LineAsString)")
        == "default\n"
    )
    # The request to the server's own HTTP interface is logged, so the absence of the
    # queries of `USER` in `system.query_log` means that no request with the credentials
    # reached the server.
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.query_log "
                "WHERE user = 'default' AND query LIKE 'SELECT currentUser()%'"
            )
        )
        > 0
    )


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            f"SELECT * FROM url('http://{QUERY_URL}', LineAsString, "
            "headers('Authorization' = 'Basic dXNlcjpwYXNzd29yZA=='))",
            id="explicit_header",
        ),
        pytest.param(
            f"SELECT * FROM url('http://{USER}:{PASSWORD}@{QUERY_URL}', LineAsString)",
            id="userinfo",
        ),
        pytest.param(
            f"SELECT * FROM url('http://{USER}@{QUERY_URL}', LineAsString)",
            id="userinfo_without_password",
        ),
        # The first option is unreachable, so the read moves on to the second one.
        pytest.param(
            "SELECT * FROM url("
            f"'http://127.0.0.1:1/|http://{USER}:{PASSWORD}@{QUERY_URL}', LineAsString) "
            "SETTINGS http_max_tries = 1",
            id="failover",
        ),
        pytest.param(
            "SELECT * FROM urlCluster("
            f"'localhost_cluster', 'http://{USER}:{PASSWORD}@{QUERY_URL}', LineAsString)",
            id="url_cluster",
        ),
        pytest.param(
            f"SELECT * FROM url('http://{USER}:{PASSWORD}@127.0.0.1:8123/*.tsv', "
            "LineAsString) SETTINGS allow_url_wildcard_from_index_pages = 1",
            id="url_wildcard_from_index_pages",
        ),
        pytest.param(
            f"SELECT * FROM url('http://{QUERY_URL}', LineAsString) SETTINGS "
            f"format_avro_schema_registry_url = 'http://{USER}:{PASSWORD}@127.0.0.1:8123/'",
            id="avro_schema_registry",
        ),
        pytest.param(
            "CREATE TABLE url_userinfo_web_disk (x UInt64) ENGINE = MergeTree ORDER BY x "
            "SETTINGS disk = disk(type = web, "
            f"endpoint = 'http://{USER}:{PASSWORD}@127.0.0.1:8123/')",
            id="web_disk",
        ),
    ],
)
def test_url_credentials_forbidden(query):
    assert_forbidden_without_requests(query)


def test_avro_schema_registry_url_without_authority():
    # The URL has no authority, so it has neither a userinfo nor a host, but the schema
    # with id 1 is requested from `http://USER:PASSWORD@127.0.0.1:8123/schemas/ids/1`,
    # which its path resolves to. The data is the magic byte and the schema id 1.
    error = assert_forbidden_without_requests(
        "SELECT * FROM format(AvroConfluent, 'a UInt8', '\\x00\\x00\\x00\\x00\\x01') "
        "SETTINGS format_avro_schema_registry_url = "
        f"'http:http://{USER}:{PASSWORD}@127.0.0.1:8123/'"
    )
    assert "while fetching schema id = 1" in error, error


def replace_forbidden_header(old, new):
    node.replace_in_config(
        CONFIG_PATH, f"<header>{old}</header>", f"<header>{new}</header>"
    )
    node.query("SYSTEM RELOAD CONFIG")


def web_disk(disk_name):
    return (
        f"disk(name = '{disk_name}', type = web, "
        f"endpoint = 'http://{USER}:{PASSWORD}@127.0.0.1:8123/')"
    )


def create_web_disk_while_authorization_is_allowed(disk_name):
    replace_forbidden_header("Authorization", "X-Not-Forbidden")
    try:
        # `disk` and `storage_policy` together are rejected after the disk is created, so
        # the disk is created without a table, and no request is sent to its endpoint.
        error = node.query_and_get_error(
            f"CREATE TABLE {disk_name} (x UInt64) ENGINE = MergeTree ORDER BY x "
            f"SETTINGS disk = {web_disk(disk_name)}, storage_policy = 'default'"
        )
        assert "cannot be specified at the same time" in error, error
    finally:
        replace_forbidden_header("X-Not-Forbidden", "Authorization")

    assert (
        node.query(f"SELECT count() FROM system.disks WHERE name = '{disk_name}'")
        == "1\n"
    )


def test_web_disk_reused_after_authorization_is_forbidden():
    # A disk created while `Authorization` is allowed is reused, without creating it
    # again, for a later definition equal to its own.
    disk_name = "url_userinfo_reused_web_disk"
    create_web_disk_while_authorization_is_allowed(disk_name)
    assert_forbidden_without_requests(
        f"CREATE TABLE {disk_name} (x UInt64) ENGINE = MergeTree ORDER BY x "
        f"SETTINGS disk = {web_disk(disk_name)}"
    )


def test_cache_disk_over_web_disk_after_authorization_is_forbidden():
    # A new `cache` disk wraps the object storage of the existing disk it names, so it
    # would send the userinfo of the endpoint of that disk.
    web_disk_name = "url_userinfo_cached_web_disk"
    cache_disk_name = "url_userinfo_web_disk_cache"
    create_web_disk_while_authorization_is_allowed(web_disk_name)

    assert_forbidden_without_requests(
        f"CREATE TABLE {cache_disk_name} (x UInt64) ENGINE = MergeTree ORDER BY x "
        f"SETTINGS disk = disk(name = '{cache_disk_name}', type = cache, "
        f"disk = '{web_disk_name}', path = '{cache_disk_name}', max_size = 1073741824)"
    )
    # A request of a disk is not a query, so `system.query_log` does not necessarily show
    # it. The definition is refused before the `cache` disk, through which the table would
    # send the requests, is created.
    assert (
        node.query(f"SELECT count() FROM system.disks WHERE name = '{cache_disk_name}'")
        == "0\n"
    )
    assert node.query(f"EXISTS TABLE {cache_disk_name}") == "0\n"


def test_url_engine_credentials_forbidden():
    node.query(
        "CREATE TABLE url_userinfo_engine (line String) "
        f"ENGINE = URL('http://{USER}:{PASSWORD}@{QUERY_URL}', LineAsString)"
    )
    try:
        assert_forbidden_without_requests("SELECT * FROM url_userinfo_engine")
    finally:
        node.query("DROP TABLE url_userinfo_engine")


def test_http_dictionary_credentials_forbidden():
    node.query(
        "CREATE DICTIONARY url_userinfo_dictionary (id UInt64, name String) "
        "PRIMARY KEY id "
        "SOURCE(HTTP(url 'http://127.0.0.1:8123/?query=SELECT+1,currentUser()' "
        f"format 'TabSeparated' credentials(user '{USER}' password '{PASSWORD}'))) "
        "LIFETIME(0) LAYOUT(FLAT())"
    )
    try:
        assert_forbidden_without_requests(
            "SYSTEM RELOAD DICTIONARY url_userinfo_dictionary"
        )
        assert_forbidden_without_requests(
            "SELECT dictGet('url_userinfo_dictionary', 'name', toUInt64(1))"
        )
    finally:
        node.query("DROP DICTIONARY url_userinfo_dictionary")

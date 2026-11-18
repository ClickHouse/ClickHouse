import pytest
import time
import uuid
import os

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/conf.xml", "configs/named_collections.xml", "configs/query_log.xml"],
    user_configs=["configs/users.xml"],
    with_nginx=True,
)
# The worker of `test_cluster_node2`; `forbidden_authorization` changes only node1's configuration.
node2 = cluster.add_instance("node2", main_configs=["configs/conf.xml"])

@pytest.fixture(scope="module", autouse=True)
def setup_node():
    try:
        cluster.start()
        node1.query(
            "insert into table function url(url1) partition by column3 values (1, 2, 3), (3, 2, 1), (1, 3, 2)"
        )
        yield
    finally:
        cluster.shutdown()


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

    # Like `SELECT`, `DESCRIBE` of a table function also needs `CREATE TEMPORARY TABLE`.
    node1.query("GRANT READ ON URL, CREATE TEMPORARY TABLE ON *.* TO u1")
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


CONF_PATH = "/etc/clickhouse-server/config.d/conf.xml"
FORBIDDEN_HEADER = "<header>X-Forbidden-Placeholder</header>"


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


def server_part_of_error(error):
    # The client appends "(query: ...)" with the query text as sent, credentials included.
    return error.split("(query: ")[0]


def assert_authorization_forbidden(error, secret="user:pass"):
    assert 'HTTP header "Authorization" is forbidden' in error, error
    assert secret not in server_part_of_error(error), error


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            "SELECT sum(column1) FROM url('http://user:pass@nginx:80/test_1', 'TSV', "
            "'column1 UInt32, column2 UInt32, column3 UInt32')",
            id="userinfo",
        ),
        # The first option is unreachable, so the read moves on to the second one.
        pytest.param(
            "SELECT sum(column1) FROM url("
            "'http://127.0.0.1:1/test_1|http://user:pass@nginx:80/test_1', "
            "'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') SETTINGS http_max_tries = 1",
            id="failover",
        ),
        # The second option is usable, so the read must not move on to it.
        pytest.param(
            "SELECT sum(column1) FROM url("
            "'http://user:pass@nginx:80/test_1|http://nginx:80/test_1', "
            "'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') SETTINGS http_max_tries = 1",
            id="failover_first",
        ),
        pytest.param(
            "SELECT sum(column1) FROM urlCluster('test_cluster_two_shards', "
            "'http://user:pass@nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
            id="url_cluster",
        ),
    ],
)
def test_url_userinfo_with_forbidden_authorization(query):
    # The userinfo is sent as Basic credentials in the `Authorization` header.
    assert node1.query(query) == "3\n"

    with forbidden_authorization():
        assert_authorization_forbidden(node1.query_and_get_error(query))


def test_url_cluster_userinfo_refused_by_initiator():
    # node1 refuses the URL before node2, which does not forbid the header, gets it. The userinfo is in the second
    # failover option, so the URL as a whole has none.
    query = (
        "SELECT sum(column1) FROM urlCluster('test_cluster_node2', "
        "'http://127.0.0.1:1/test_1|http://user:pass@nginx:80/test_1', "
        "'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') SETTINGS http_max_tries = 1"
    )
    assert node1.query(query) == "3\n"

    with forbidden_authorization():
        assert_authorization_forbidden(node1.query_and_get_error(query))


def test_url_userinfo_without_username_with_forbidden_authorization():
    # Credentials without a username are not sent, so they are not refused.
    with forbidden_authorization():
        assert (
            node1.query(
                "SELECT sum(column1) FROM url('http://:pass@nginx:80/test_1', 'TSV', "
                "'column1 UInt32, column2 UInt32, column3 UInt32')"
            )
            == "3\n"
        )


def test_stored_url_table_with_forbidden_authorization():
    # A stored table loads after the header is forbidden, but its reads do not send the userinfo.
    table_name = "url_userinfo"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    node1.query(
        f"CREATE TABLE {table_name} (column1 UInt32, column2 UInt32, column3 UInt32) "
        "ENGINE = URL('http://user:pass@nginx:80/test_1', 'TSV')"
    )
    try:
        assert node1.query(f"SELECT sum(column1) FROM {table_name}") == "3\n"

        with forbidden_authorization():
            node1.query(f"DETACH TABLE {table_name}")
            node1.query(f"ATTACH TABLE {table_name}")
            assert_authorization_forbidden(node1.query_and_get_error(f"SELECT sum(column1) FROM {table_name}"))
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name}")


def test_http_dictionary_credentials_with_forbidden_authorization():
    def create_dictionary(name):
        node1.query(
            f"CREATE DICTIONARY {name} (column1 UInt64, column2 UInt32, column3 UInt32) PRIMARY KEY column1 "
            "SOURCE(HTTP(url 'http://nginx:80/test_1' format 'TabSeparated' "
            "credentials(user 'user' password 'dict_secret_pw'))) "
            "LIFETIME(0) LAYOUT(FLAT())"
        )

    allowed, forbidden = "http_credentials_allowed", "http_credentials_forbidden"
    try:
        create_dictionary(allowed)
        assert node1.query(f"SELECT dictGet('{allowed}', 'column2', toUInt64(3))") == "2\n"

        with forbidden_authorization():
            create_dictionary(forbidden)
            assert_authorization_forbidden(
                node1.query_and_get_error(f"SELECT dictGet('{forbidden}', 'column2', toUInt64(3))"),
                secret="dict_secret_pw",
            )
    finally:
        node1.query(f"DROP DICTIONARY IF EXISTS {allowed}")
        node1.query(f"DROP DICTIONARY IF EXISTS {forbidden}")


def test_loaded_http_dictionary_credentials_with_forbidden_authorization():
    # A loaded dictionary keeps its source across its updates, so the credentials are checked per request.
    name = "http_credentials_loaded"
    node1.query(
        f"CREATE DICTIONARY {name} (column1 UInt64, column2 UInt32, column3 UInt32) PRIMARY KEY column1 "
        "SOURCE(HTTP(url 'http://nginx:80/test_1' format 'TabSeparated' "
        "credentials(user 'user' password 'dict_secret_pw'))) "
        "LIFETIME(MIN 1 MAX 1) LAYOUT(FLAT())"
    )
    try:
        assert node1.query(f"SELECT dictGet('{name}', 'column2', toUInt64(3))") == "2\n"

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
        pytest.param("http://user:pass@nginx:80/", id="userinfo"),
        # Without an authority the URL has no userinfo, but the schema is requested from
        # `http://user:pass@nginx:80/schemas/ids/1`, which its path resolves to.
        pytest.param("http:http://user:pass@nginx:80/", id="without_authority"),
    ],
)
def test_avro_schema_registry_userinfo_with_forbidden_authorization(registry_url):
    # The data is the magic byte and the schema id 1, so the schema is requested. The server has no
    # schema registry, so the request fails anyway: only whether it is refused matters.
    query = (
        "SELECT * FROM format(AvroConfluent, 'a UInt8', '\\x00\\x00\\x00\\x00\\x01') "
        f"SETTINGS format_avro_schema_registry_url = '{registry_url}'"
    )
    error = node1.query_and_get_error(query)
    assert 'HTTP header "Authorization" is forbidden' not in error, error

    with forbidden_authorization():
        assert_authorization_forbidden(node1.query_and_get_error(query))

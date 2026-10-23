import datetime
import json

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import elasticsearch_pass, elasticsearch_user
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_elasticsearch=True,
)
node_without_flag = cluster.add_instance(
    "node_without_flag",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users_without_elasticsearch_flag.xml"],
    stay_alive=True,
)

# ClickHouse runs inside the Docker network and reaches Elasticsearch by its compose service name.
ELASTICSEARCH_URL_FOR_CLICKHOUSE = "http://elasticsearch1:9200"
BASIC_AUTH_ARGUMENTS = f"user = '{elasticsearch_user}', password = '{elasticsearch_pass}'"
ELASTICSEARCH_CREDENTIALS = (elasticsearch_user, elasticsearch_pass)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def elasticsearch_url_for_test_process():
    # The test process runs on the host and reaches Elasticsearch through the published port.
    return f"http://localhost:{cluster.elasticsearch_port}"


def elasticsearch_request(
    method,
    path,
    json_body=None,
    raw_body=None,
    headers=None,
    credentials=ELASTICSEARCH_CREDENTIALS,
):
    response = requests.request(
        method,
        elasticsearch_url_for_test_process() + path,
        json=json_body,
        data=raw_body,
        headers=headers,
        auth=credentials,
    )
    response.raise_for_status()
    return response.json()


def delete_index_if_exists(index_name):
    requests.delete(
        elasticsearch_url_for_test_process() + "/" + index_name,
        auth=ELASTICSEARCH_CREDENTIALS,
    )


def create_index_with_documents(index_name, documents, mapping=None, number_of_shards=3):
    delete_index_if_exists(index_name)

    index_definition = {
        "settings": {"number_of_shards": number_of_shards, "number_of_replicas": 0}
    }
    if mapping:
        index_definition["mappings"] = mapping
    elasticsearch_request("PUT", f"/{index_name}", json_body=index_definition)
    if not documents:
        return
    bulk_load_documents(
        index_name,
        [
            ({"index": {"_id": str(position + 1)}}, document)
            for position, document in enumerate(documents)
        ],
    )


def bulk_load_documents(target_name, actions_and_documents):
    # The bulk API takes newline-delimited JSON: an action line, then the document, per document.
    bulk_lines = []
    for action, document in actions_and_documents:
        bulk_lines.append(json.dumps(action))
        bulk_lines.append(json.dumps(document))
    bulk_result = elasticsearch_request(
        "POST",
        f"/{target_name}/_bulk?refresh=true",
        raw_body="\n".join(bulk_lines) + "\n",
        headers={"Content-Type": "application/x-ndjson"},
    )
    assert not bulk_result["errors"], bulk_result


def elasticsearch_arguments(index_name, credentials=BASIC_AUTH_ARGUMENTS, extra_arguments=""):
    arguments = f"'{ELASTICSEARCH_URL_FOR_CLICKHOUSE}', '{index_name}'"
    if credentials:
        arguments += ", " + credentials
    if extra_arguments:
        arguments += ", " + extra_arguments
    return arguments


def elasticsearch_table_function(index_name, credentials=BASIC_AUTH_ARGUMENTS, extra_arguments=""):
    return f"elasticsearch({elasticsearch_arguments(index_name, credentials, extra_arguments)})"


def elasticsearch_engine(index_name, credentials=BASIC_AUTH_ARGUMENTS, extra_arguments=""):
    return f"ENGINE = Elasticsearch({elasticsearch_arguments(index_name, credentials, extra_arguments)})"


def test_select_default_column():
    create_index_with_documents(
        "simple", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    )
    assert (
        node.query(
            f"SELECT document.id, document.name FROM {elasticsearch_table_function('simple')} "
            "ORDER BY document.id::Int64 FORMAT TSV"
        )
        == "1\ta\n2\tb\n"
    )


def test_virtual_columns():
    create_index_with_documents("virtuals", [{"id": 1}, {"id": 2}, {"id": 3}])
    table_function = elasticsearch_table_function("virtuals")

    assert (
        node.query(
            f"SELECT _id, _index, document.id FROM {table_function} ORDER BY document.id::Int64 FORMAT TSV"
        )
        == "1\tvirtuals\t1\n2\tvirtuals\t2\n3\tvirtuals\t3\n"
    )

    # Only virtual columns are read: the source is not fetched, the row count and ids must still be right.
    assert (
        node.query(f"SELECT _id, _index FROM {table_function} ORDER BY _id FORMAT TSV")
        == "1\tvirtuals\n2\tvirtuals\n3\tvirtuals\n"
    )

    described_columns = [
        line.split("\t")[:2]
        for line in node.query(f"DESCRIBE TABLE {table_function}").strip().split("\n")
    ]
    assert described_columns == [["document", "JSON"]]


def test_declared_json_column():
    create_index_with_documents(
        "declared",
        [{"id": 1, "name": "a", "secret": "s1"}, {"id": 2, "name": "b", "secret": "s2"}],
    )
    declared_column = "doc JSON(id Int64, SKIP secret)"
    expected = 'Int64\t1\t{"id":1,"name":"a"}\nInt64\t2\t{"id":2,"name":"b"}\n'

    node.query("DROP TABLE IF EXISTS es_declared")
    node.query(f"CREATE TABLE es_declared ({declared_column}) {elasticsearch_engine('declared')}")
    assert (
        node.query("SELECT toTypeName(doc.id), doc.id, doc FROM es_declared ORDER BY doc.id FORMAT TSV")
        == expected
    )
    node.query("DROP TABLE es_declared")

    table_function = elasticsearch_table_function(
        "declared", extra_arguments=f"structure = '{declared_column}'"
    )
    assert (
        node.query(f"SELECT toTypeName(doc.id), doc.id, doc FROM {table_function} ORDER BY doc.id FORMAT TSV")
        == expected
    )


def test_empty_index():
    create_index_with_documents("empty", [])
    table_function = elasticsearch_table_function("empty")
    assert node.query(f"SELECT count() FROM {table_function}") == "0\n"
    assert node.query(f"SELECT _id, document FROM {table_function}") == ""


def test_source_disabled():
    create_index_with_documents(
        "no_source",
        [{"id": 1}, {"id": 2}],
        mapping={"_source": {"enabled": False}},
    )
    table_function = elasticsearch_table_function("no_source")
    assert (
        node.query(f"SELECT _id, _index FROM {table_function} ORDER BY _id FORMAT TSV")
        == "1\tno_source\n2\tno_source\n"
    )


def test_query_composition():
    statuses = ["new", "paid", "shipped"]
    orders = [
        {"status": statuses[i % 3], "amount": i * 1.5 + 0.5, "user_id": i % 3 + 1}
        for i in range(10)
    ]
    create_index_with_documents("orders", orders)
    table_function = elasticsearch_table_function("orders")

    assert (
        node.query(
            f"SELECT document.status::String AS status, count() FROM {table_function} "
            "GROUP BY status ORDER BY status FORMAT TSV"
        )
        == "new\t4\npaid\t3\nshipped\t3\n"
    )

    assert node.query(f"SELECT sum(document.amount::Float64) FROM {table_function}") == "72.5\n"

    node.query("DROP TABLE IF EXISTS es_users")
    node.query("CREATE TABLE es_users (id UInt64, name String) ENGINE = MergeTree ORDER BY id")
    node.query("INSERT INTO es_users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
    assert (
        node.query(
            f"SELECT u.name, count() FROM {table_function} AS e "
            "JOIN es_users AS u ON e.document.user_id::UInt64 = u.id "
            "GROUP BY u.name ORDER BY u.name FORMAT TSV"
        )
        == "alice\t4\nbob\t3\ncarol\t3\n"
    )
    node.query("DROP TABLE es_users")


def test_index_patterns():
    create_index_with_documents("idxform-a", [{"id": 1}, {"id": 2}])
    create_index_with_documents("idxform-b", [{"id": 3}, {"id": 4}, {"id": 5}])
    create_index_with_documents("idxform-old", [{"id": 6}])
    create_index_with_documents("other-nginx", [{"id": 7}])

    def count_documents(index_pattern):
        return node.query(f"SELECT count() FROM {elasticsearch_table_function(index_pattern)}")

    def index_names(index_pattern):
        return node.query(
            f"SELECT arraySort(groupUniqArray(_index)) FROM {elasticsearch_table_function(index_pattern)}"
        )

    assert count_documents("idxform-a") == "2\n"
    assert count_documents("idxform-*") == "6\n"
    assert index_names("idxform-*") == "['idxform-a','idxform-b','idxform-old']\n"
    assert count_documents("*-nginx") == "1\n"
    assert count_documents("idxform-a,idxform-b") == "5\n"
    assert count_documents("idxform-*,-idxform-old") == "5\n"
    assert index_names("idxform-*,-idxform-old") == "['idxform-a','idxform-b']\n"

    # Other tests leave indices behind, so `*` and `_all` are checked for a lower bound and for membership.
    for everything in ["*", "_all"]:
        assert (
            node.query(
                "SELECT count() >= 7, hasAll(groupUniqArray(_index), "
                "['idxform-a', 'idxform-b', 'idxform-old', 'other-nginx']) "
                f"FROM {elasticsearch_table_function(everything)}"
            )
            == "1\t1\n"
        )

    error = node.query_and_get_error(
        f"SELECT count() FROM {elasticsearch_table_function('nothing-*')}"
    )
    assert "index_not_found_exception" in error


def test_alias():
    create_index_with_documents("idxform-a", [{"id": 1}, {"id": 2}])
    create_index_with_documents("idxform-b", [{"id": 3}, {"id": 4}, {"id": 5}])
    elasticsearch_request(
        "POST",
        "/_aliases",
        json_body={
            "actions": [
                {"add": {"index": "idxform-a", "alias": "idxform-alias"}},
                {"add": {"index": "idxform-b", "alias": "idxform-alias"}},
            ]
        },
    )
    table_function = elasticsearch_table_function("idxform-alias")

    assert node.query(f"SELECT count() FROM {table_function}") == "5\n"
    assert (
        node.query(f"SELECT arraySort(groupUniqArray(_index)) FROM {table_function}")
        == "['idxform-a','idxform-b']\n"
    )

    elasticsearch_request(
        "POST",
        "/_aliases",
        json_body={"actions": [{"remove": {"index": "idxform-b", "alias": "idxform-alias"}}]},
    )
    assert node.query(f"SELECT count() FROM {table_function}") == "2\n"


DATA_STREAM_TIMESTAMP = "2026-01-01T00:00:00Z"


def delete_data_stream_if_exists(stream_name):
    base_url = elasticsearch_url_for_test_process()
    requests.delete(f"{base_url}/_data_stream/{stream_name}", auth=ELASTICSEARCH_CREDENTIALS)
    requests.delete(
        f"{base_url}/_index_template/{stream_name}-template", auth=ELASTICSEARCH_CREDENTIALS
    )


def append_to_data_stream(stream_name, documents):
    # A data stream accepts only the `create` action, and every document needs `@timestamp`.
    bulk_load_documents(
        stream_name,
        [
            ({"create": {}}, {"@timestamp": DATA_STREAM_TIMESTAMP, **document})
            for document in documents
        ],
    )


def create_data_stream_with_documents(stream_name, documents):
    delete_data_stream_if_exists(stream_name)
    elasticsearch_request(
        "PUT",
        f"/_index_template/{stream_name}-template",
        json_body={"index_patterns": [stream_name + "*"], "data_stream": {}, "priority": 500},
    )
    elasticsearch_request("PUT", f"/_data_stream/{stream_name}")
    append_to_data_stream(stream_name, documents)


def test_data_stream():
    create_data_stream_with_documents("idxform-stream", [{"id": 1}, {"id": 2}])
    table_function = elasticsearch_table_function("idxform-stream")

    assert node.query(f"SELECT count() FROM {table_function}") == "2\n"
    assert (
        node.query(f"SELECT DISTINCT startsWith(_index, '.ds-idxform-stream-') FROM {table_function}")
        == "1\n"
    )

    # After a rollover the stream spans two backing indices and one read covers both.
    elasticsearch_request("POST", "/idxform-stream/_rollover")
    append_to_data_stream("idxform-stream", [{"id": 3}, {"id": 4}, {"id": 5}])
    assert node.query(f"SELECT count(), uniqExact(_index) FROM {table_function}") == "5\t2\n"

    delete_data_stream_if_exists("idxform-stream")


def test_index_date_math():
    # Elasticsearch resolves `<name-{now/d}>` to today's date in the format yyyy.MM.dd on its own clock.
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y.%m.%d")
    create_index_with_documents(f"idxform-{today}", [{"id": 1}, {"id": 2}, {"id": 3}])

    def count_documents(index_expression):
        return node.query(f"SELECT count() FROM {elasticsearch_table_function(index_expression)}")

    assert count_documents("<idxform-{now}>") == "3\n"

    assert count_documents("<idxform-{now/d}>") == "3\n"
    error = node.query_and_get_error(
        f"SELECT count() FROM {elasticsearch_table_function('<idxform-{now/d-1d}>')}"
    )
    assert "index_not_found_exception" in error


def test_paging():
    create_index_with_documents("paging", [{"id": i} for i in range(10)])

    def table_function_with_page_size(page_size):
        return elasticsearch_table_function(
            "paging", extra_arguments=f"SETTINGS page_size = {page_size}"
        )

    def read_all_ids(page_size):
        return node.query(
            f"SELECT document.id FROM {table_function_with_page_size(page_size)} "
            "ORDER BY document.id::Int64 FORMAT TSV"
        )

    expected_ids = "".join(f"{i}\n" for i in range(10))
    assert read_all_ids(3) == expected_ids
    assert read_all_ids(100) == expected_ids
    assert (
        node.query(
            f"SELECT count(), sum(document.id::Int64) FROM {table_function_with_page_size(3)}"
        )
        == "10\t45\n"
    )

    error = node.query_and_get_error(
        f"SELECT _id FROM {table_function_with_page_size(10001)}"
    )
    assert "max_result_window" in error


def test_settings():
    create_index_with_documents("settings", [{"id": i} for i in range(5)])

    node.query("DROP TABLE IF EXISTS es_settings")
    node.query(
        f"CREATE TABLE es_settings {elasticsearch_engine('settings')} SETTINGS page_size = 2, keep_alive = 30"
    )
    create_statement = node.query("SHOW CREATE TABLE es_settings")
    assert "page_size = 2" in create_statement
    assert "keep_alive = 30" in create_statement
    assert node.query("SELECT count() FROM es_settings") == "5\n"

    error = node.query_and_get_error("ALTER TABLE es_settings MODIFY SETTING page_size = 4")
    assert "not supported" in error
    error = node.query_and_get_error("ALTER TABLE es_settings RESET SETTING page_size")
    assert "not supported" in error
    assert "page_size = 2" in node.query("SHOW CREATE TABLE es_settings")
    error = node.query_and_get_error("ALTER TABLE es_settings ADD COLUMN extra Int32")
    assert "not supported" in error

    node.query("DROP TABLE es_settings")


def test_query_argument():
    create_index_with_documents("dsl", [{"id": i} for i in range(10)])

    def query_argument(query_body):
        return f"query('{json.dumps(query_body)}')"

    def read_ids(table_function, where=""):
        return node.query(
            f"SELECT document.id FROM {table_function} {where} ORDER BY document.id::Int64 FORMAT TSV"
        )

    term_query = query_argument({"term": {"id": 3}})
    assert (
        read_ids(elasticsearch_table_function("dsl", extra_arguments=term_query)) == "3\n"
    )

    range_query = query_argument({"range": {"id": {"gte": 5}}})
    assert read_ids(
        elasticsearch_table_function("dsl", extra_arguments=range_query)
    ) == read_ids(elasticsearch_table_function("dsl"), where="WHERE document.id::Int64 >= 5")

    unknown_query = query_argument({"no_such_query": {}})
    error = node.query_and_get_error(
        f"SELECT count() FROM {elasticsearch_table_function('dsl', extra_arguments=unknown_query)}"
    )
    assert "parsing_exception" in error

    node.query("DROP TABLE IF EXISTS es_dsl")
    node.query(f"CREATE TABLE es_dsl {elasticsearch_engine('dsl', extra_arguments=term_query)}")
    assert read_ids("es_dsl") == "3\n"
    node.query("DROP TABLE es_dsl")

    assert read_ids(f"elasticsearch(es_simple, index = 'dsl', {term_query})") == "3\n"


def test_query_numeric_tokens():
    create_index_with_documents(
        "numeric_tokens",
        [{"key": "1.0"}] * 5 + [{"key": "1"}] * 2,
        mapping={"properties": {"key": {"type": "keyword"}}},
    )
    term_query = "query('" + json.dumps({"term": {"key": 1.0}}) + "')"
    table_function = elasticsearch_table_function("numeric_tokens", extra_arguments=term_query)

    assert node.query(f"SELECT count(), uniqExact(_id) FROM {table_function}") == "5\t5\n"


def test_table_engine():
    create_index_with_documents("engine", [{"id": i} for i in range(3)])
    new_document = '{"id": 4}'

    node.query("DROP TABLE IF EXISTS es_engine")
    node.query(f"CREATE TABLE es_engine {elasticsearch_engine('engine')}")
    create_statement = node.query("SHOW CREATE TABLE es_engine")
    assert elasticsearch_pass not in create_statement
    assert "[HIDDEN]" in create_statement
    assert "`document` JSON" in create_statement
    assert node.query("SELECT count() FROM es_engine") == "3\n"

    node.query("DETACH TABLE es_engine")
    node.query("ATTACH TABLE es_engine")
    assert node.query("SELECT count() FROM es_engine") == "3\n"

    error = node.query_and_get_error(f"INSERT INTO es_engine VALUES ('{new_document}')")
    assert "not supported" in error
    node.query("DROP TABLE es_engine")

    node.query("DROP TABLE IF EXISTS es_engine_from_function")
    node.query(f"CREATE TABLE es_engine_from_function AS {elasticsearch_table_function('engine')}")
    create_statement = node.query("SHOW CREATE TABLE es_engine_from_function")
    assert elasticsearch_pass not in create_statement
    assert "[HIDDEN]" in create_statement
    assert node.query("SELECT count() FROM es_engine_from_function") == "3\n"

    node.query("DETACH TABLE es_engine_from_function")
    node.query("ATTACH TABLE es_engine_from_function")
    assert node.query("SELECT count() FROM es_engine_from_function") == "3\n"
    node.query("DROP TABLE es_engine_from_function")

    error = node.query_and_get_error(
        f"INSERT INTO FUNCTION {elasticsearch_table_function('engine')} VALUES ('{new_document}')"
    )
    assert "not supported" in error


def test_table_function_table_survives_restart():
    create_index_with_documents("function_settings", [{"id": i} for i in range(5)])
    elasticsearch_request(
        "PUT", "/function_settings/_settings", json_body={"index": {"max_result_window": 3}}
    )

    error = node_without_flag.query_and_get_error(
        f"SELECT count() FROM {elasticsearch_table_function('function_settings')}"
    )
    assert "SUPPORT_IS_DISABLED" in error

    table_function = elasticsearch_table_function(
        "function_settings", extra_arguments="SETTINGS page_size = 2, keep_alive = 30"
    )
    node_without_flag.query("DROP TABLE IF EXISTS es_function_settings")
    node_without_flag.query(
        f"CREATE TABLE es_function_settings AS {table_function}",
        settings={"enable_elasticsearch": 1},
    )

    def check_table():
        create_statement = node_without_flag.query("SHOW CREATE TABLE es_function_settings")
        assert "page_size = 2" in create_statement
        assert "keep_alive = 30" in create_statement
        assert node_without_flag.query("SELECT count() FROM es_function_settings") == "5\n"

    check_table()
    node_without_flag.restart_clickhouse()
    check_table()

    node_without_flag.query("DROP TABLE es_function_settings")


def test_insert_select_into_local():
    create_index_with_documents("to_copy", [{"id": i} for i in range(10)])
    table_function = elasticsearch_table_function("to_copy")
    expected_ids = "".join(f"{i}\n" for i in range(10))

    node.query("DROP TABLE IF EXISTS es_local")
    node.query("CREATE TABLE es_local (doc JSON) ENGINE = Memory")
    node.query("DROP TABLE IF EXISTS es_source")
    node.query(f"CREATE TABLE es_source {elasticsearch_engine('to_copy')}")

    node.query("INSERT INTO es_local SELECT document FROM es_source")
    assert node.query("SELECT doc.id FROM es_local ORDER BY doc.id::Int64 FORMAT TSV") == expected_ids

    node.query("TRUNCATE TABLE es_local")
    node.query(f"INSERT INTO es_local SELECT document FROM {table_function}")
    assert node.query("SELECT doc.id FROM es_local ORDER BY doc.id::Int64 FORMAT TSV") == expected_ids

    node.query("DROP TABLE IF EXISTS es_local_with_ids")
    node.query(
        f"CREATE TABLE es_local_with_ids ENGINE = Memory AS SELECT document, _id FROM {table_function}"
    )
    assert node.query("SELECT count(), uniqExact(_id) FROM es_local_with_ids") == "10\t10\n"

    node.query("DROP TABLE es_local_with_ids")
    node.query("DROP TABLE es_source")
    node.query("DROP TABLE es_local")


def test_named_collection():
    create_index_with_documents("simple", [{"id": 1}, {"id": 2}])
    create_index_with_documents("other", [{"id": 1}, {"id": 2}, {"id": 3}])

    assert node.query("SELECT count() FROM elasticsearch(es_simple)") == "2\n"
    assert node.query("SELECT count() FROM elasticsearch(es_simple, index = 'other')") == "3\n"

    error = node.query_and_get_error(
        "SELECT count() FROM elasticsearch(es_simple, index = 'simple', index = 'other')"
    )
    assert "more than once" in error

    node.query("DROP TABLE IF EXISTS es_from_collection")
    node.query("DROP NAMED COLLECTION IF EXISTS es_dependency")
    node.query(
        "CREATE NAMED COLLECTION es_dependency AS "
        f"url = '{ELASTICSEARCH_URL_FOR_CLICKHOUSE}', index = 'other', {BASIC_AUTH_ARGUMENTS}"
    )
    node.query("CREATE TABLE es_from_collection ENGINE = Elasticsearch(es_dependency)")
    assert node.query("SELECT count() FROM es_from_collection") == "3\n"

    error = node.query_and_get_error("DROP NAMED COLLECTION es_dependency")
    assert "is used by tables" in error
    assert "es_from_collection" in error

    node.query("DROP TABLE es_from_collection")
    node.query("DROP NAMED COLLECTION es_dependency")


def test_auth():
    create_index_with_documents("auth", [{"id": 1}, {"id": 2}])

    def count_with_credentials(credentials):
        return node.query(
            f"SELECT count() FROM {elasticsearch_table_function('auth', credentials=credentials)}"
        )

    def error_with_credentials(credentials):
        return node.query_and_get_error(
            f"SELECT count() FROM {elasticsearch_table_function('auth', credentials=credentials)}"
        )

    assert count_with_credentials(BASIC_AUTH_ARGUMENTS) == "2\n"
    assert "401" in error_with_credentials(f"user = '{elasticsearch_user}', password = 'wrong'")
    assert "401" in error_with_credentials("")

    api_key = elasticsearch_request(
        "POST", "/_security/api_key", json_body={"name": "clickhouse_test"}
    )["encoded"]
    assert count_with_credentials(f"api_key = '{api_key}'") == "2\n"
    assert "401" in error_with_credentials("api_key = 'not-a-key'")


def test_remote_errors():
    create_index_with_documents("errors", [{"id": 1}])

    error = node.query_and_get_error(
        f"SELECT count() FROM {elasticsearch_table_function('no_such_index')}"
    )
    assert "index_not_found_exception" in error

    unreachable = f"elasticsearch('http://elasticsearch1:1', 'errors', {BASIC_AUTH_ARGUMENTS})"
    error = node.query_and_get_error(
        f"SELECT count() FROM {unreachable}", settings={"http_max_tries": 1}
    )
    assert "Connection refused" in error

    error = node.query_and_get_error(
        f"SELECT _id FROM {elasticsearch_table_function('errors', extra_arguments='SETTINGS keep_alive = 90000')}"
    )
    assert "keep_alive" in error


def open_search_contexts():
    stats = elasticsearch_request("GET", "/_nodes/stats/indices/search")
    return sum(
        node_stats["indices"]["search"]["open_contexts"] for node_stats in stats["nodes"].values()
    )


def test_pit_closed_after_read():
    create_index_with_documents("pit", [{"id": i} for i in range(10)])
    contexts_before = open_search_contexts()

    assert (
        node.query(
            f"SELECT uniqExact(_id) FROM {elasticsearch_table_function('pit', extra_arguments='SETTINGS page_size = 3')}"
        )
        == "10\n"
    )

    # The point in time is closed by the client destructor, which runs after the query has returned.
    wait_condition(
        open_search_contexts,
        lambda contexts: contexts == contexts_before,
        max_attempts=50,
        delay=0.2,
    )
    assert (
        node.query(
            "SELECT value FROM system.events WHERE event = 'ElasticsearchPointInTimeCloseFailures'"
        )
        == ""
    )

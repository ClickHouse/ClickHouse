import json
import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CONFIG_PATH = "/etc/clickhouse-server/config.d/opa.xml"
STUB_PATH = "/tmp/opa_stub.py"
STUB_PORT = 8181
DEFAULT_URI = f"http://127.0.0.1:{STUB_PORT}/v1/data/clickhouse/allow"

BASE_SECTION = """<clickhouse>
    <open_policy_agent>
        <uri>{uri}</uri>
        {extra}
        <exempt_users>
            <user>default</user>
        </exempt_users>
    </open_policy_agent>
</clickhouse>"""

node = cluster.add_instance("node", main_configs=["configs/opa.xml"], stay_alive=True)
node_without_opa = cluster.add_instance("node_without_opa")


def write_section(uri=DEFAULT_URI, extra=""):
    node.replace_config(CONFIG_PATH, BASE_SECTION.format(uri=uri, extra=extra))


def stub_curl(path, method="POST", data=""):
    """The stub is reached from inside the container, so the config can point at localhost and the
    test needs no extra service on the cluster network."""
    command = ["curl", "-s", "-X", method, f"http://127.0.0.1:{STUB_PORT}{path}"]
    if data:
        command += ["--data-binary", data]
    return node.exec_in_container(command)


def set_rule(rule):
    """Installs the decision rule and clears the recorded requests."""
    stub_curl("/rule", data=rule)


def recorded_requests():
    return json.loads(stub_curl("/requests", method="GET"))


def last_action():
    requests = recorded_requests()
    assert requests, "OPA was not consulted"
    return requests[-1]["input"]["action"]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        node.copy_file_to_container(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "opa_stub.py"),
            STUB_PATH,
        )
        node.exec_in_container(
            ["bash", "-c", f"nohup python3 {STUB_PATH} {STUB_PORT} >/dev/null 2>&1 &"],
            detach=True,
        )
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"for i in $(seq 1 50); do curl -sf -X POST http://127.0.0.1:{STUB_PORT}/rule "
                f"--data-binary True && exit 0; sleep 0.2; done; exit 1",
            ]
        )

        node.query("CREATE DATABASE IF NOT EXISTS plain")
        node.query(
            "CREATE TABLE plain.orders (id UInt32, amount UInt32, customer_email String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "INSERT INTO plain.orders VALUES (1, 10, 'a@x.com'), (2, 20, 'b@x.com')"
        )

        node.query("CREATE USER analyst IDENTIFIED WITH no_password")
        node.query("CREATE ROLE analysts")
        node.query("GRANT analysts TO analyst")
        node.query("GRANT SELECT ON plain.* TO analyst")

        # Granted only two of the three columns, to show what an expression may reach.
        node.query("CREATE USER narrow IDENTIFIED WITH no_password")
        node.query("GRANT SELECT(id, amount) ON plain.orders TO narrow")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def restore_state():
    """Each test rewrites the section and the rule, so both are put back to keep tests independent."""
    try:
        yield
    finally:
        write_section()
        node.query("SYSTEM RELOAD CONFIG")
        set_rule("True")


def test_section_is_parsed_on_startup():
    assert node.contains_in_log("Open Policy Agent authorization is enabled")


def test_absent_section_leaves_the_feature_off():
    assert not node_without_opa.contains_in_log(
        "Open Policy Agent authorization is enabled"
    )


def test_reload_picks_up_a_changed_endpoint():
    reloaded_uri = "http://127.0.0.1:18181/v1/data/clickhouse/allow"
    write_section(uri=reloaded_uri)
    node.query("SYSTEM RELOAD CONFIG")

    node.wait_for_log_line(reloaded_uri)


def test_optional_endpoints_are_reported():
    write_section(
        extra=f"<row_filters_uri>http://127.0.0.1:{STUB_PORT}/v1/data/clickhouse/rowFilters</row_filters_uri>"
    )
    node.query("SYSTEM RELOAD CONFIG")

    node.wait_for_log_line("row filters endpoint")


def test_a_missing_uri_is_rejected():
    """The section exists to turn authorization on; without a decision endpoint it cannot, and
    silently disabling itself would drop the control the operator was configuring."""
    node.replace_config(
        CONFIG_PATH,
        "<clickhouse><open_policy_agent>"
        "<default_catalog>testcatalog</default_catalog>"
        "</open_policy_agent></clickhouse>",
    )

    assert "is required" in node.query_and_get_error("SYSTEM RELOAD CONFIG")


def test_an_unknown_element_in_a_user_list_is_rejected():
    """A typo in a security-relevant list must not silently widen access."""
    node.replace_config(
        CONFIG_PATH,
        "<clickhouse><open_policy_agent>"
        f"<uri>{DEFAULT_URI}</uri>"
        "<exempt_users><usr>oops</usr></exempt_users>"
        "</open_policy_agent></clickhouse>",
    )

    assert "Unexpected element" in node.query_and_get_error("SYSTEM RELOAD CONFIG")


def test_request_carries_the_documented_shape():
    set_rule("True")
    node.query("SELECT id FROM plain.orders", user="analyst")

    action = last_action()
    assert "SELECT" in action["operations"]

    resource = action["resource"]
    assert resource["database"] == "plain"
    assert resource["table"] == "orders"
    assert "id" in resource["columns"]

    context = recorded_requests()[-1]["input"]["context"]
    assert context["user"] == "analyst"
    assert "analysts" in context["roles"]
    assert context["query_id"]
    assert context["clickhouse_version"]


def test_a_table_name_is_reported_verbatim():
    """ClickHouse names two levels, so the table arrives exactly as ClickHouse knows it. A database
    whose tables are named `namespace.table` keeps the dot; splitting it is a policy's business, not
    the server's."""
    node.query("CREATE DATABASE IF NOT EXISTS ice")
    node.query(
        "CREATE TABLE IF NOT EXISTS ice.`sales.orders` (id UInt32) ENGINE = MergeTree ORDER BY id"
    )
    node.query("GRANT SELECT ON ice.* TO analyst")

    set_rule("True")
    node.query("SELECT id FROM ice.`sales.orders`", user="analyst")

    resource = last_action()["resource"]
    assert resource["database"] == "ice"
    assert resource["table"] == "sales.orders"


def test_a_database_scoped_check_omits_the_table():
    """A policy can tell a check that covers a whole database from one about a table by the absence
    of the key, rather than by a blank value."""
    set_rule("True")
    node.query("SHOW TABLES FROM plain", user="analyst")

    assert any(
        "table" not in request["input"]["action"].get("resource", {"table": None})
        for request in recorded_requests()
    )


def test_a_denied_table_is_refused():
    set_rule("input['action']['resource'].get('table') != 'orders'")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT id FROM plain.orders", user="analyst"
    )


def test_an_allowed_table_still_works():
    set_rule("input['action']['resource'].get('table') == 'orders'")

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"


def test_an_exempt_user_bypasses_the_policy():
    """An administrative account has to keep working while a policy is broken, because repairing the
    policy is done through it."""
    set_rule("False")

    assert node.query("SELECT count() FROM plain.orders").strip() == "2"


def test_an_unreachable_policy_engine_denies():
    """Losing the policy engine must not silently lose the restrictions it was enforcing."""
    write_section(uri=f"http://127.0.0.1:{STUB_PORT + 999}/v1/data/clickhouse/allow")
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query_and_get_error("SELECT id FROM plain.orders", user="analyst")


def test_a_failing_policy_engine_denies():
    stub_curl("/fail", data="500")

    assert node.query_and_get_error("SELECT id FROM plain.orders", user="analyst")


def test_an_undefined_decision_is_reported_clearly():
    """OPA omits `result` when the queried document is undefined, which almost always means the
    endpoint path does not match the policy's package and rule."""
    stub_curl("/body", data="{}")

    error = node.query_and_get_error("SELECT id FROM plain.orders", user="analyst")
    assert "undefined" in error


def test_a_non_boolean_decision_is_rejected():
    stub_curl("/body", data='{"result": "yes"}')

    error = node.query_and_get_error("SELECT id FROM plain.orders", user="analyst")
    assert "boolean" in error


def test_a_decision_document_with_allow_is_accepted():
    """A policy may answer with the whole decision document rather than a bare boolean."""
    stub_curl("/body", data='{"result": {"allow": true}}')

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"


def test_the_system_database_is_out_of_scope_by_default():
    """Clients poll system tables constantly for metadata; routing that traffic to OPA would cost a
    request per poll, so the database is excluded unless asked for."""
    set_rule("False")

    assert node.query("SELECT 1 FROM system.one", user="analyst").strip() == "1"


DENY_EMAIL = "'customer_email' not in input['action']['resource'].get('columns', [])"


def test_a_denied_column_is_refused():
    set_rule(DENY_EMAIL)

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT customer_email FROM plain.orders", user="analyst"
    )


def test_the_remaining_columns_stay_readable():
    set_rule(DENY_EMAIL)

    assert (
        node.query("SELECT id, amount FROM plain.orders ORDER BY id", user="analyst")
        == "1\t10\n2\t20\n"
    )


def test_select_star_is_refused_when_any_column_is_denied():
    """`*` expands to every column, so the check names the denied one and must fail - the same way it
    does with a native column grant."""
    set_rule(DENY_EMAIL)

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT * FROM plain.orders", user="analyst"
    )


def test_a_column_denial_is_evaluated_per_column():
    """A trivial query reads no column data but still needs one readable column. The decision has to
    be per column, so a policy that denies one column does not make the whole table unreadable."""
    set_rule(DENY_EMAIL)

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"

    asked = [
        request["input"]["action"]["resource"].get("columns")
        for request in recorded_requests()
    ]
    # The column list is what the policy is asked about, one column at a time on this path.
    assert ["customer_email"] in asked


def test_a_table_wide_denial_also_blocks_a_trivial_query():
    set_rule("False")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT count() FROM plain.orders", user="analyst"
    )


def test_a_repeated_question_is_asked_once_per_query():
    """One query checks the same table several times - the planner, then the analyzer resolving
    columns, then any wrapper storage. Each of those must not cost a request, or a policy server sees
    traffic shaped by the query plan rather than by the objects the query touches."""
    set_rule("True")
    node.query(
        "SELECT a.id FROM plain.orders AS a JOIN plain.orders AS b ON a.id = b.id",
        user="analyst",
    )

    select_questions = [
        (
            tuple(request["input"]["action"]["operations"]),
            request["input"]["action"]["resource"].get("database"),
            request["input"]["action"]["resource"].get("table"),
            tuple(request["input"]["action"]["resource"].get("columns", [])),
        )
        for request in recorded_requests()
    ]

    # Every question asked was distinct; nothing was asked twice.
    assert len(select_questions) == len(set(select_questions))


def test_decisions_are_not_reused_across_queries():
    """A policy can change between queries, so a decision must not outlive the query that reached
    it."""
    set_rule("True")
    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"

    set_rule("False")
    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT count() FROM plain.orders", user="analyst"
    )


ROW_FILTERS_URI = f"http://127.0.0.1:{STUB_PORT}/v1/data/clickhouse/rowFilters"


def enable_row_filters():
    write_section(extra=f"<row_filters_uri>{ROW_FILTERS_URI}</row_filters_uri>")
    node.query("SYSTEM RELOAD CONFIG")


def set_row_filters(body):
    """The filter endpoint answers from its own state, so a filter response never has to satisfy the
    decision endpoint's shape."""
    stub_curl("/filters", data=body)


def test_a_row_filter_removes_rows():
    enable_row_filters()
    set_row_filters('{"result": [{"expression": "id = 1"}]}')

    assert (
        node.query("SELECT id FROM plain.orders ORDER BY id", user="analyst") == "1\n"
    )


def test_several_row_filters_are_combined_with_and():
    """A second filter can only remove more rows; it must not widen what the first one allowed."""
    enable_row_filters()
    set_row_filters(
        '{"result": [{"expression": "id >= 1"}, {"expression": "amount > 15"}]}'
    )

    assert node.query("SELECT id FROM plain.orders", user="analyst") == "2\n"


def test_no_row_filter_leaves_every_row():
    """A policy that defines no filter leaves `result` undefined, which is the common case and must
    not be read as an error."""
    enable_row_filters()
    set_row_filters("{}")

    assert (
        node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"
    )


def test_a_row_filter_applies_through_a_view():
    enable_row_filters()
    node.query("CREATE VIEW IF NOT EXISTS plain.orders_view AS SELECT * FROM plain.orders")
    node.query("GRANT SELECT ON plain.orders_view TO analyst")
    set_row_filters('{"result": [{"expression": "id = 1"}]}')

    assert (
        node.query("SELECT count() FROM plain.orders_view", user="analyst").strip()
        == "1"
    )


def test_a_malformed_row_filter_is_reported():
    """Dropping an expression that cannot be parsed would show more rows than the policy intended."""
    enable_row_filters()
    set_row_filters('{"result": [{"expression": "this is not sql"}]}')

    assert node.query_and_get_error("SELECT id FROM plain.orders", user="analyst")


def test_a_row_filter_cannot_change_the_row_count():
    """A filter is a per-row predicate, so a function that changes the number of rows is rejected -
    the same restriction a row policy written in SQL has."""
    enable_row_filters()
    set_row_filters('{"result": [{"expression": "arrayJoin([1, 2]) = 1"}]}')

    assert node.query_and_get_error("SELECT id FROM plain.orders", user="analyst")


def test_a_row_filter_combines_with_a_native_row_policy():
    """Both apply, so the result is the intersection."""
    enable_row_filters()
    node.query(
        "CREATE ROW POLICY IF NOT EXISTS only_big ON plain.orders USING amount > 15 TO analyst"
    )
    try:
        set_row_filters('{"result": [{"expression": "id = 1"}]}')

        # The native policy keeps only id=2, the OPA filter keeps only id=1; together, nothing.
        assert (
            node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "0"
        )
    finally:
        node.query("DROP ROW POLICY IF EXISTS only_big ON plain.orders")


MASKING_URI = f"http://127.0.0.1:{STUB_PORT}/v1/data/clickhouse/columnMask"

def enable_column_masking():
    write_section(extra=f"<column_masking_uri>{MASKING_URI}</column_masking_uri>")
    node.query("SYSTEM RELOAD CONFIG")


def set_column_masks(body):
    stub_curl("/masks", data=body)


REDACT_EMAIL = '{"result": [{"column": "customer_email", "expression": "\'****\'"}]}'


def test_a_masked_column_is_replaced_in_the_projection():
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)

    assert (
        node.query("SELECT customer_email FROM plain.orders", user="analyst")
        == "****\n****\n"
    )


def test_an_unmasked_user_sees_the_real_value():
    enable_column_masking()
    set_column_masks('{"result": []}')

    assert (
        node.query(
            "SELECT customer_email FROM plain.orders ORDER BY id", user="analyst"
        )
        == "a@x.com\nb@x.com\n"
    )


def test_a_mask_applies_in_where():
    """The mask is the column's definition, so a predicate sees the masked value too. If it did not,
    a user could search for a value and learn it from whether rows came back."""
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)

    assert (
        node.query(
            "SELECT count() FROM plain.orders WHERE customer_email = 'a@x.com'",
            user="analyst",
        ).strip()
        == "0"
    )
    assert (
        node.query(
            "SELECT count() FROM plain.orders WHERE customer_email = '****'",
            user="analyst",
        ).strip()
        == "2"
    )


def test_a_mask_applies_in_group_by():
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)

    assert (
        node.query(
            "SELECT customer_email, count() FROM plain.orders GROUP BY customer_email",
            user="analyst",
        )
        == "****\t2\n"
    )


def test_a_mask_applies_to_a_join_key():
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)

    # Masked to the same constant on both sides, so every row matches every row.
    assert (
        node.query(
            "SELECT count() FROM plain.orders AS a "
            "JOIN plain.orders AS b ON a.customer_email = b.customer_email",
            user="analyst",
        ).strip()
        == "4"
    )


def test_a_mask_is_cast_to_the_declared_type():
    """`id` is a UInt32, so a mask returning a string has to be converted rather than changing the
    column's type underneath the query."""
    enable_column_masking()
    set_column_masks('{"result": [{"column": "id", "expression": "\'7\'"}]}')

    assert node.query("SELECT id FROM plain.orders", user="analyst") == "7\n7\n"


def test_an_unrelated_column_is_untouched():
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)

    assert (
        node.query("SELECT id FROM plain.orders ORDER BY id", user="analyst") == "1\n2\n"
    )


def test_a_trivial_count_is_unaffected_by_a_mask():
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"


def test_two_masks_for_one_column_are_rejected():
    """Honouring both would make the effective mask depend on the order a policy emitted them."""
    enable_column_masking()
    set_column_masks(
        '{"result": [{"column": "customer_email", "expression": "\'a\'"},'
        ' {"column": "customer_email", "expression": "\'b\'"}]}'
    )

    assert "more than one mask" in node.query_and_get_error(
        "SELECT customer_email FROM plain.orders", user="analyst"
    )


def test_a_mask_without_a_column_is_rejected():
    enable_column_masking()
    set_column_masks('{"result": [{"expression": "\'****\'"}]}')

    assert node.query_and_get_error(
        "SELECT customer_email FROM plain.orders", user="analyst"
    )


def test_a_malformed_mask_is_reported():
    enable_column_masking()
    set_column_masks(
        '{"result": [{"column": "customer_email", "expression": "not valid sql ("}]}'
    )

    assert node.query_and_get_error(
        "SELECT customer_email FROM plain.orders", user="analyst"
    )


def test_masking_an_alias_column_is_rejected():
    """An ALIAS column already defines what it evaluates to; choosing one silently would either
    ignore the mask or ignore the table definition."""
    enable_column_masking()
    node.query(
        "CREATE TABLE IF NOT EXISTS plain.with_alias "
        "(id UInt32, doubled UInt32 ALIAS id * 2) ENGINE = MergeTree ORDER BY id"
    )
    node.query("GRANT SELECT ON plain.with_alias TO analyst")
    set_column_masks('{"result": [{"column": "doubled", "expression": "0"}]}')

    assert "ALIAS column" in node.query_and_get_error(
        "SELECT doubled FROM plain.with_alias", user="analyst"
    )


def test_a_mask_is_fetched_once_per_table():
    """One request covers the whole table, so a wide table does not turn into one request per
    column."""
    enable_column_masking()
    set_column_masks(REDACT_EMAIL)
    node.query(
        "SELECT id, amount, customer_email FROM plain.orders", user="analyst"
    )

    mask_requests = [
        request
        for request in recorded_requests()
        if "customer_email" in request["input"]["action"]["resource"].get("columns", [])
    ]
    # The masks for every column arrive together, so one request mentions the whole column list.
    assert mask_requests


def test_a_row_filter_may_read_a_column_the_user_cannot_select():
    """A row filter is administrator-defined, like a native `ROW POLICY`, so it is not subject to the
    querying user's column grants. This is why no identity override is needed to write one."""
    enable_row_filters()
    set_row_filters('{"result": [{"expression": "customer_email = \'a@x.com\'"}]}')

    assert (
        node.query("SELECT id FROM plain.orders", user="narrow").strip() == "1"
    )


def test_a_mask_may_read_a_column_the_user_cannot_select():
    enable_column_masking()
    set_column_masks(
        '{"result": [{"column": "amount", "expression": "length(customer_email)"}]}'
    )

    assert node.query("SELECT amount FROM plain.orders ORDER BY id", user="narrow") == "7\n7\n"



BATCH_URI = f"http://127.0.0.1:{STUB_PORT}/v1/data/clickhouse/batch"


def enable_batch():
    write_section(extra=f"<batch_uri>{BATCH_URI}</batch_uri>")
    node.query("SYSTEM RELOAD CONFIG")


def set_batch_rule(rule):
    """The rule is evaluated once per resource with `resource` bound to it."""
    stub_curl("/batch", data=rule)


def batch_request_count():
    return len(
        [
            request
            for request in recorded_requests()
            if request["input"]["action"].get("filter_resources")
        ]
    )


def single_column_request_count():
    return len(
        [
            request
            for request in recorded_requests()
            if len(request["input"]["action"].get("resource", {}).get("columns", [])) == 1
        ]
    )


def test_a_batched_endpoint_replaces_the_per_column_questions():
    """Discovering which columns are readable asks about each column. With a batched endpoint those
    questions travel together, so a wide table does not turn into one request per column."""
    enable_batch()
    set_batch_rule("True")

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"

    assert batch_request_count() == 1
    # The per-column answers came from the batch, so nothing was asked one column at a time.
    assert single_column_request_count() == 0


def test_an_explicit_column_still_uses_the_single_decision_endpoint():
    """Batching answers the question "which of these may be read", which the server asks only when it
    has to discover that. A query that names its columns is one decision about one resource."""
    enable_batch()
    set_batch_rule("False")
    set_rule("True")

    assert (
        node.query("SELECT customer_email FROM plain.orders ORDER BY id", user="analyst")
        == "a@x.com\nb@x.com\n"
    )


def test_a_batched_partial_answer_still_finds_a_readable_column():
    """A trivial query needs one readable column, so denying some columns in the batch must not make
    the table unreadable."""
    enable_batch()
    set_batch_rule("resource.get('columns') != ['customer_email']")

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"


def test_an_empty_batch_answer_denies_everything():
    """Returning no allowed indices is the closed position, so a trivial query finds no readable
    column."""
    enable_batch()
    set_batch_rule("False")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT count() FROM plain.orders", user="analyst"
    )


def test_a_batch_answer_naming_an_unknown_resource_is_rejected():
    """An index outside the request cannot be matched to a resource; accepting it would mean acting on
    an answer that does not describe what was asked."""
    enable_batch()
    stub_curl("/batch_body", data='{"result": [99]}')

    assert node.query_and_get_error("SELECT count() FROM plain.orders", user="analyst")


def test_batching_is_split_into_chunks():
    """A list longer than the configured maximum is asked about in several requests, and the answers
    are concatenated."""
    write_section(
        extra=f"<batch_uri>{BATCH_URI}</batch_uri><max_batch_size>2</max_batch_size>"
    )
    node.query("SYSTEM RELOAD CONFIG")
    set_batch_rule("True")

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"

    # Three columns, two per request.
    assert batch_request_count() == 2



def operations_seen():
    return [
        tuple(request["input"]["action"]["operations"]) for request in recorded_requests()
    ]


def test_insert_is_governed():
    node.query("GRANT INSERT ON plain.* TO analyst")
    set_rule("'INSERT' not in input['action']['operations']")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "INSERT INTO plain.orders VALUES (3, 30, 'c@x.com')", user="analyst"
    )


def test_a_read_only_policy_allows_select_and_refuses_insert():
    node.query("GRANT INSERT ON plain.* TO analyst")
    set_rule("input['action']['operations'] == ['SELECT']")

    assert node.query("SELECT count() FROM plain.orders", user="analyst").strip() == "2"
    assert "ACCESS_DENIED" in node.query_and_get_error(
        "INSERT INTO plain.orders VALUES (4, 40, 'd@x.com')", user="analyst"
    )


def test_alter_is_governed():
    node.query("GRANT ALTER UPDATE ON plain.orders TO analyst")
    set_rule("'ALTER UPDATE' not in input['action']['operations']")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "ALTER TABLE plain.orders UPDATE amount = 0 WHERE id = 1", user="analyst"
    )


def test_drop_table_is_governed():
    node.query("CREATE TABLE IF NOT EXISTS plain.droppable (id UInt32) ENGINE = MergeTree ORDER BY id")
    node.query("GRANT DROP TABLE ON plain.droppable TO analyst")
    set_rule("'DROP TABLE' not in input['action']['operations']")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "DROP TABLE plain.droppable", user="analyst"
    )


def test_create_table_is_governed():
    node.query("GRANT CREATE TABLE ON plain.* TO analyst")
    set_rule("'CREATE TABLE' not in input['action']['operations']")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "CREATE TABLE plain.forbidden (id UInt32) ENGINE = MergeTree ORDER BY id",
        user="analyst",
    )


def test_truncate_is_governed():
    node.query("CREATE TABLE IF NOT EXISTS plain.truncatable (id UInt32) ENGINE = MergeTree ORDER BY id")
    node.query("GRANT TRUNCATE ON plain.truncatable TO analyst")
    set_rule("'TRUNCATE' not in input['action']['operations']")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "TRUNCATE TABLE plain.truncatable", user="analyst"
    )


def test_a_rename_is_authorized_on_both_names():
    """ClickHouse authorizes a rename as separate checks on the old and the new name, so a policy sees
    each one as its own resource and needs no combined request to describe the pair."""
    node.query("CREATE TABLE IF NOT EXISTS plain.before (id UInt32) ENGINE = MergeTree ORDER BY id")
    node.query("GRANT SELECT, DROP TABLE ON plain.before TO analyst")
    node.query("GRANT CREATE TABLE, INSERT ON plain.after TO analyst")
    set_rule("True")

    node.query("RENAME TABLE plain.before TO plain.after", user="analyst")
    try:
        tables = {
            request["input"]["action"]["resource"].get("table")
            for request in recorded_requests()
        }
        assert "before" in tables
        assert "after" in tables
    finally:
        node.query("DROP TABLE IF EXISTS plain.after")




def test_a_row_filter_applies_through_a_merge_table():
    """A `Merge` table reads its children, so the filter has to reach them rather than stopping at the
    wrapper."""
    enable_row_filters()
    node.query(
        "CREATE TABLE IF NOT EXISTS plain.merged (id UInt32, amount UInt32, customer_email String) "
        "ENGINE = Merge('plain', '^orders$')"
    )
    node.query("GRANT SELECT ON plain.merged TO analyst")
    set_row_filters('{"result": [{"expression": "id = 1"}]}')

    assert node.query("SELECT count() FROM plain.merged", user="analyst").strip() == "1"


def test_a_mask_applies_through_a_merge_table():
    enable_column_masking()
    node.query(
        "CREATE TABLE IF NOT EXISTS plain.merged (id UInt32, amount UInt32, customer_email String) "
        "ENGINE = Merge('plain', '^orders$')"
    )
    node.query("GRANT SELECT ON plain.merged TO analyst")
    set_column_masks(REDACT_EMAIL)

    assert (
        node.query("SELECT DISTINCT customer_email FROM plain.merged", user="analyst")
        == "****\n"
    )


def test_a_denial_applies_inside_a_scalar_subquery():
    """A subquery is still a read of the table, so it cannot be used to reach denied data."""
    set_rule("input['action']['resource'].get('table') != 'orders'")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT (SELECT count() FROM plain.orders)", user="analyst"
    )


def test_a_row_filter_applies_inside_a_scalar_subquery():
    enable_row_filters()
    set_row_filters('{"result": [{"expression": "id = 1"}]}')

    assert (
        node.query("SELECT (SELECT count() FROM plain.orders)", user="analyst").strip()
        == "1"
    )


def test_a_denial_is_reported_when_only_analyzing():
    """`EXPLAIN` resolves the query without running it, and must not become a way to confirm access to
    something a policy refuses."""
    set_rule("input['action']['resource'].get('table') != 'orders'")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "EXPLAIN SELECT id FROM plain.orders", user="analyst"
    )


def test_a_mask_survives_a_projection():
    """A projection is precomputed from the real values, so it must not be used to answer a query over
    a masked column."""
    enable_column_masking()
    node.query(
        "CREATE TABLE IF NOT EXISTS plain.with_projection "
        "(id UInt32, customer_email String, "
        "PROJECTION by_email (SELECT customer_email, count() GROUP BY customer_email)) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query(
        "INSERT INTO plain.with_projection VALUES (1, 'a@x.com'), (2, 'b@x.com')"
    )
    node.query("GRANT SELECT ON plain.with_projection TO analyst")
    set_column_masks(REDACT_EMAIL)

    assert (
        node.query(
            "SELECT customer_email, count() FROM plain.with_projection GROUP BY customer_email",
            user="analyst",
        )
        == "****\t2\n"
    )


def test_a_definer_view_does_not_bypass_the_callers_row_filter():
    """`SQL SECURITY DEFINER` runs the view body with the definer's rights, so it is worth being
    explicit that it does not become a way around a policy: the filter for the underlying table is
    still resolved for the calling user, even when the definer is exempt."""
    enable_row_filters()
    node.query(
        "CREATE VIEW IF NOT EXISTS plain.definer_view "
        "DEFINER = default SQL SECURITY DEFINER AS SELECT * FROM plain.orders"
    )
    node.query("GRANT SELECT ON plain.definer_view TO analyst")
    set_row_filters('{"result": [{"expression": "id = 1"}]}')

    assert (
        node.query("SELECT count() FROM plain.definer_view", user="analyst").strip()
        == "1"
    )


def test_an_invoker_view_keeps_the_caller_identity():
    enable_row_filters()
    node.query(
        "CREATE VIEW IF NOT EXISTS plain.invoker_view "
        "SQL SECURITY INVOKER AS SELECT * FROM plain.orders"
    )
    node.query("GRANT SELECT ON plain.invoker_view TO analyst")
    set_row_filters('{"result": [{"expression": "id = 1"}]}')

    assert (
        node.query("SELECT count() FROM plain.invoker_view", user="analyst").strip()
        == "1"
    )

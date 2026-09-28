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


def test_both_column_masking_endpoints_are_rejected():
    """The two endpoints answer the same question, so accepting both would make the effective mask
    depend on which one the server happened to consult first."""
    write_section(
        extra=f"<column_masking_uri>http://127.0.0.1:{STUB_PORT}/a</column_masking_uri>"
        f"<batch_column_masking_uri>http://127.0.0.1:{STUB_PORT}/b</batch_column_masking_uri>"
    )

    assert "mutually exclusive" in node.query_and_get_error("SYSTEM RELOAD CONFIG")


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
    write_section(
        extra="<allowed_expression_identities><usr>oops</usr></allowed_expression_identities>"
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

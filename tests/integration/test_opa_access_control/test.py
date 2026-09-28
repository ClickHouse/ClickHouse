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
        <default_catalog>testcatalog</default_catalog>
        {extra}
        <exempt_users>
            <user>default</user>
        </exempt_users>
        <mapping>
            <database name="ice">
                <catalog>lakekeeper</catalog>
                <split_dotted_table_name>true</split_dotted_table_name>
            </database>
        </mapping>
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

    table = action["resource"]["table"]
    assert table["catalogName"] == "testcatalog"
    assert table["schemaName"] == "plain"
    assert table["tableName"] == "orders"
    assert "id" in table["columns"]

    context = recorded_requests()[-1]["input"]["context"]
    assert context["identity"]["user"] == "analyst"
    # A ClickHouse role is reported as a group, which is what lets one rule text match both engines.
    assert "analysts" in context["identity"]["groups"]
    assert context["queryId"]
    assert context["softwareStack"]["clickhouseVersion"]


def test_a_dotted_table_name_is_split_into_schema_and_table():
    """A data lake database names a table `namespace.table`; the namespace has to arrive as the
    schema so the name matches what another engine reports for the same table."""
    node.query("CREATE DATABASE IF NOT EXISTS ice")
    node.query(
        "CREATE TABLE ice.`sales.orders` (id UInt32) ENGINE = MergeTree ORDER BY id"
    )
    node.query("GRANT SELECT ON ice.* TO analyst")

    set_rule("True")
    node.query("SELECT id FROM ice.`sales.orders`", user="analyst")

    table = last_action()["resource"]["table"]
    assert table["catalogName"] == "lakekeeper"
    assert table["schemaName"] == "sales"
    assert table["tableName"] == "orders"


def test_a_denied_table_is_refused():
    set_rule("input['action']['resource']['table']['tableName'] != 'orders'")

    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT id FROM plain.orders", user="analyst"
    )


def test_an_allowed_table_still_works():
    set_rule("input['action']['resource']['table']['tableName'] == 'orders'")

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

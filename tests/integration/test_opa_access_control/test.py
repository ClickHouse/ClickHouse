import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CONFIG_PATH = "/etc/clickhouse-server/config.d/opa.xml"

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

DEFAULT_URI = "http://opa:8181/v1/data/clickhouse/allow"

node = cluster.add_instance("node", main_configs=["configs/opa.xml"])
node_without_opa = cluster.add_instance("node_without_opa")


def write_section(uri=DEFAULT_URI, extra=""):
    node.replace_config(CONFIG_PATH, BASE_SECTION.format(uri=uri, extra=extra))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def restore_config():
    """Each test rewrites the whole section, so put the original one back to keep tests order independent."""
    try:
        yield
    finally:
        write_section()
        node.query("SYSTEM RELOAD CONFIG")


def test_section_is_parsed_on_startup():
    assert node.contains_in_log("Open Policy Agent authorization is enabled")
    assert node.contains_in_log(DEFAULT_URI)


def test_absent_section_leaves_the_feature_off():
    assert not node_without_opa.contains_in_log(
        "Open Policy Agent authorization is enabled"
    )


def test_reload_picks_up_a_changed_endpoint():
    reloaded_uri = "http://opa-reloaded:8181/v1/data/clickhouse/allow"
    write_section(uri=reloaded_uri)
    node.query("SYSTEM RELOAD CONFIG")

    node.wait_for_log_line(reloaded_uri)


def test_optional_endpoints_are_reported():
    write_section(
        extra="<row_filters_uri>http://opa:8181/v1/data/clickhouse/rowFilters</row_filters_uri>"
    )
    node.query("SYSTEM RELOAD CONFIG")

    node.wait_for_log_line("row filters endpoint")


def test_both_column_masking_endpoints_are_rejected():
    """The two endpoints answer the same question, so accepting both would make the effective mask
    depend on which one the server happened to consult first."""
    write_section(
        extra="<column_masking_uri>http://opa:8181/v1/data/clickhouse/columnMask</column_masking_uri>"
        "<batch_column_masking_uri>http://opa:8181/v1/data/clickhouse/batchColumnMasks</batch_column_masking_uri>"
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

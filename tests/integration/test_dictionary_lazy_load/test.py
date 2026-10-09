import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node_lazy = cluster.add_instance("node_lazy", stay_alive=True)
node_eager = cluster.add_instance("node_eager", main_configs=["configs/eager.xml"], stay_alive=True)
node_xml = cluster.add_instance("node_xml", dictionaries=["configs/dictionaries/xml_ok.xml"], stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        for node in (node_lazy, node_eager):
            node.query("CREATE TABLE src (id UInt64, val String) ENGINE = MergeTree ORDER BY tuple()")
            node.query("INSERT INTO src VALUES (1, 'a')")
        yield cluster
    finally:
        cluster.shutdown()


def create_dictionary(node, clause, table):
    node.query("DROP DICTIONARY IF EXISTS dict")
    node.query(
        f"""
        CREATE DICTIONARY dict (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE '{table}' DB 'default'))
        LAYOUT(HASHED())
        LIFETIME(0)
        {clause}
        """
    )


def get_status(node, name):
    return node.query(
        f"SELECT status FROM system.dictionaries WHERE name = '{name}'"
    ).strip()


def get_is_lazy(node, name):
    return node.query(
        f"SELECT is_lazy FROM system.dictionaries WHERE name = '{name}'"
    ).strip() == "true"


# lazy = the effective decision: the dictionary_lazy_load setting if not 'auto', else the server's default.
@pytest.mark.parametrize("node, clause, lazy", [
    pytest.param(node_lazy, "", True),
    pytest.param(node_lazy, "SETTINGS(dictionary_lazy_load = 'auto')", True),
    pytest.param(node_lazy, "SETTINGS(dictionary_lazy_load = 1)", True),
    pytest.param(node_lazy, "SETTINGS(dictionary_lazy_load = 0)", False),
    pytest.param(node_eager, "", False),
    pytest.param(node_eager, "SETTINGS(dictionary_lazy_load = 'auto')", False),
    pytest.param(node_eager, "SETTINGS(dictionary_lazy_load = 1)", True),
    pytest.param(node_eager, "SETTINGS(dictionary_lazy_load = 0)", False),
])
def test_dictionary_lazy_load(started_cluster, node, clause, lazy):
    if lazy:
        create_dictionary(node, clause, "no_such_table")
    else:
        with pytest.raises(QueryRuntimeException):
            create_dictionary(node, clause, "no_such_table")

    create_dictionary(node, clause, "src")

    assert get_status(node, "dict") == ("NOT_LOADED" if lazy else "LOADED")
    assert get_is_lazy(node, "dict") == lazy
    assert node.query("SELECT dictGetString('dict', 'val', toUInt64(1))").strip() == "a"
    assert get_status(node, "dict") == "LOADED"
    assert get_is_lazy(node, "dict") == lazy

    node.restart_clickhouse()
    assert_eq_with_retry(node, "SELECT status FROM system.dictionaries WHERE name = 'dict'", "NOT_LOADED" if lazy else "LOADED")
    assert get_is_lazy(node, "dict") == lazy

    node.query("DROP DICTIONARY dict")


def test_invalid_dictionary_lazy_load_in_xml(started_cluster):
    query = "SELECT name, status, is_lazy, last_exception LIKE '%CANNOT_PARSE_BOOL%' FROM system.dictionaries WHERE name IN ('xml_ok', 'xml_bad') ORDER BY name"
    expected = "xml_bad\tFAILED\tfalse\t1\nxml_ok\tNOT_LOADED\ttrue\t0\n"
    node_xml.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/xml_bad.xml"), "/etc/clickhouse-server/dictionaries/xml_bad.xml")
    try:
        assert_eq_with_retry(node_xml, query, expected, retry_count=60)
        node_xml.restart_clickhouse()
        assert_eq_with_retry(node_xml, query, expected, retry_count=60)
    finally:
        node_xml.exec_in_container(["rm", "-f", "/etc/clickhouse-server/dictionaries/xml_bad.xml"])

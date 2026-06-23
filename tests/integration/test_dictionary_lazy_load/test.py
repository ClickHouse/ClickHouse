import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node_lazy = cluster.add_instance("node_lazy")
node_eager = cluster.add_instance("node_eager", main_configs=["configs/eager.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        for node in (node_lazy, node_eager):
            node.query("CREATE TABLE src (id UInt64, val String) ENGINE = Memory")
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


# lazy = the effective decision: the LAZY_LOAD clause if present, else the server's default.
@pytest.mark.parametrize("node, clause, lazy", [
    pytest.param(node_lazy, "", True),
    pytest.param(node_lazy, "LAZY_LOAD(1)", True),
    pytest.param(node_lazy, "LAZY_LOAD(0)", False),
    pytest.param(node_eager, "", False),
    pytest.param(node_eager, "LAZY_LOAD(1)", True),
    pytest.param(node_eager, "LAZY_LOAD(0)", False),
])
def test_dictionary_lazy_load(started_cluster, node, clause, lazy):
    if lazy:
        create_dictionary(node, clause, "no_such_table")
    else:
        with pytest.raises(QueryRuntimeException):
            create_dictionary(node, clause, "no_such_table")

    create_dictionary(node, clause, "src")

    assert get_status(node, "dict") == ("NOT_LOADED" if lazy else "LOADED")
    assert node.query("SELECT dictGetString('dict', 'val', toUInt64(1))").strip() == "a"
    assert get_status(node, "dict") == "LOADED"

    node.restart_clickhouse()
    assert_eq_with_retry(node, "SELECT status FROM system.dictionaries WHERE name = 'dict'", "NOT_LOADED" if lazy else "LOADED")

    node.query("DROP DICTIONARY dict")

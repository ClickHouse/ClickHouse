import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/factory_restrictions.xml"])

CONFIG_PATH = "/etc/clickhouse-server/config.d/factory_restrictions.xml"

RESTRICTIONS = """
<clickhouse>
    <factory_restrictions>
        <formats>
            <deny><CSV/></deny>
        </formats>
        <table_engines>
            <allow><MergeTree/><Memory/></allow>
        </table_engines>
    </factory_restrictions>
</clickhouse>
"""

INVALID_RESTRICTIONS = """
<clickhouse>
    <factory_restrictions>
        <formats>
            <allow><CSV/></allow>
            <deny><TSV/></deny>
        </formats>
    </factory_restrictions>
</clickhouse>
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_config(content):
    node.replace_config(CONFIG_PATH, content)
    node.query("SYSTEM RELOAD CONFIG")


def test_hot_reload(started_cluster):
    node.query("DROP TABLE IF EXISTS t_log")
    node.query("CREATE TABLE t_log (x UInt8) ENGINE = Log")
    assert node.query("SELECT 1 FORMAT CSV") == "1\n"

    set_config(RESTRICTIONS)

    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error("SELECT 1 FORMAT CSV")
    assert node.query("SELECT 1 FORMAT TSV") == "1\n"
    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error(
        "CREATE TABLE t_log2 (x UInt8) ENGINE = Log"
    )
    node.query("CREATE TABLE t_memory (x UInt8) ENGINE = Memory")
    node.query("DROP TABLE t_memory")

    # An invalid config is rejected, and the previous rules stay in place.
    node.replace_config(CONFIG_PATH, INVALID_RESTRICTIONS)
    assert "INVALID_CONFIG_PARAMETER" in node.query_and_get_error(
        "SYSTEM RELOAD CONFIG"
    )
    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error("SELECT 1 FORMAT CSV")

    # Without the section everything is allowed again.
    set_config("<clickhouse></clickhouse>")
    assert node.query("SELECT 1 FORMAT CSV") == "1\n"
    node.query("CREATE TABLE t_log2 (x UInt8) ENGINE = Log")
    node.query("DROP TABLE t_log2")
    node.query("DROP TABLE t_log")

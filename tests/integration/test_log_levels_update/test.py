import pytest
import re

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=False)

config = """<clickhouse>
    <logger>
        <level>information</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    </logger>
</clickhouse>"""


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_log(node):
    return node.exec_in_container(
        ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.log"]
    )


def test_log_levels_update(start_cluster):
    # Make sure that there are enough log messages for the test
    for i in range(5):
        node.query("SELECT 1")

    log = get_log(node)
    assert re.search("(<Trace>|<Debug>)", log)

    node.replace_config("/etc/clickhouse-server/config.d/log.xml", config)
    node.query("SYSTEM RELOAD CONFIG;")
    node.exec_in_container(
        ["bash", "-c", "> /var/log/clickhouse-server/clickhouse-server.log"]
    )

    for i in range(5):
        node.query("SELECT 1")

    log = get_log(node)
    assert len(log) > 0
    assert not re.search("(<Trace>|<Debug>)", log)

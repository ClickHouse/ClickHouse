import re

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", with_zookeeper=False, main_configs=["configs/log.xml"]
)

original_config = """
<clickhouse>
    <logger>
        <level>trace</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    </logger>
</clickhouse>
"""

updated_config = """
<clickhouse>
    <logger>
        <level>trace</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <message_regexp_negative>.*Loaded config.*</message_regexp_negative>
        <message_regexps>
            <logger>
                <name>executeQuery</name>
                <message_regexp>.*Read.*</message_regexp>
                <message_regexp_negative>.*from.*</message_regexp_negative>
            </logger>
        </message_regexps>
    </logger>
</clickhouse>
"""


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


def test_regexp_pattern_update(start_cluster):
    # Display config being used
    node.exec_in_container(["cat", "/etc/clickhouse-server/config.d/log.xml"])

    # Make sure that there are enough log messages for the test
    for _ in range(5):
        node.query("SYSTEM RELOAD CONFIG")
        node.query("SELECT 1")

    log = get_log(node)
    assert re.search(r".*Loaded config.*", log)
    assert re.search(r".*executeQuery.*Read.*", log)
    assert re.search(r".*executeQuery.*from.*", log)

    node.replace_config("/etc/clickhouse-server/config.d/log.xml", updated_config)
    node.query("SYSTEM RELOAD CONFIG;")
    node.exec_in_container(
        ["bash", "-c", "> /var/log/clickhouse-server/clickhouse-server.log"]
    )

    for _ in range(5):
        node.query("SYSTEM RELOAD CONFIG")
        node.query("SELECT 1")

    log = get_log(node)
    assert len(log) > 0

    assert not re.search(r".*Loaded config.*", log)
    assert re.search(r".*executeQuery.*Read.*", log)
    assert not re.search(r".*executeQuery.*from.*", log)

    node.replace_config("/etc/clickhouse-server/config.d/log.xml", original_config)

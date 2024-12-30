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


def test_regexp_pattern_update(start_cluster):
    # Display config being used
    node.exec_in_container(["cat", "/etc/clickhouse-server/config.d/log.xml"])

    # Make sure that there are enough log messages for the test
    for _ in range(5):
        node.query("SYSTEM RELOAD CONFIG")
        node.query("SELECT 1")

    assert node.contains_in_log(r".*Loaded config.*")
    assert node.contains_in_log(r".*executeQuery.*Read.*")
    assert node.contains_in_log(r".*executeQuery.*from.*")

    node.replace_config("/etc/clickhouse-server/config.d/log.xml", updated_config)
    node.query("SYSTEM RELOAD CONFIG;")
    node.rotate_logs()

    for _ in range(5):
        node.query("SYSTEM RELOAD CONFIG")
        node.query("SELECT 1")

    assert not node.contains_in_log(r".*Loaded config.*")
    assert node.contains_in_log(r".*executeQuery.*Read.*")
    assert not node.contains_in_log(r".*executeQuery.*from.*")

    node.replace_config("/etc/clickhouse-server/config.d/log.xml", original_config)

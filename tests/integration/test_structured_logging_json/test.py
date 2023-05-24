import pytest
from helpers.cluster import ClickHouseCluster
import json

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config_json.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def is_json(log_json):
    try:
        json.loads(log_json)
    except ValueError as e:
        return False
    return True


def test_structured_logging_json_format(start_cluster):
    node.query("SELECT 1")

    logs = node.grep_in_log("").split("\n")
    length = min(10, len(logs))
    for i in range(0, length):
        assert is_json(logs[i])

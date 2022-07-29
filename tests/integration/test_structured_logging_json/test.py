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


def get_log_array(logs):
    log_array = []
    temp_log = ""
    for i in range(0, len(logs)):
        temp_log += logs[i]
        if logs[i] == "}":
            log_array.append(temp_log)
            temp_log = ""
    return log_array


def is_json(log_json):
    try:
        json.loads(log_json)
    except ValueError as e:
        return False
    return True


def test_structured_logging_json_format(start_cluster):
    node.query("SELECT 1")

    logs = node.grep_in_log(" ")
    log_array = get_log_array(logs)
    result = True
    for i in range(0, len(log_array)):
        temporary_result = is_json(log_array[i])
        result &= temporary_result
        # we will test maximum 5 logs
        if i >= min(4, len(log_array) - 1):
            break
    assert result

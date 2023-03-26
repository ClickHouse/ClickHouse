import os
import time

import pytest
from helpers.cluster import ClickHouseCluster, get_instances_dir

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/max_table_size_to_drop.xml"])

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

CONFIG_PATH = os.path.join(
    SCRIPT_DIR,
    "./{}/node/configs/config.d/max_table_size_to_drop.xml".format(get_instances_dir()),
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query(
            "CREATE TABLE test(date Date, id UInt32) ENGINE = MergeTree() PARTITION BY date ORDER BY id"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_reload_max_table_size_to_drop(start_cluster):
    node.query("INSERT INTO test VALUES (now(), 0)")

    time.sleep(5)  # wait for data part commit

    drop = node.get_query_request("DROP TABLE test")
    out, err = drop.get_answer_and_error()
    assert out == ""
    assert err != ""

    config = open(CONFIG_PATH, "r")
    config_lines = config.readlines()
    config.close()
    config_lines = [
        line.replace("<max_table_size_to_drop>1", "<max_table_size_to_drop>1000000")
        for line in config_lines
    ]
    config = open(CONFIG_PATH, "w")
    config.writelines(config_lines)
    config.close()

    node.query("SYSTEM RELOAD CONFIG")

    drop = node.get_query_request("DROP TABLE test")
    out, err = drop.get_answer_and_error()
    assert out == ""
    assert err == ""

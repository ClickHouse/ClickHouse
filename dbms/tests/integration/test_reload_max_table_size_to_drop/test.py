import time
import pytest
import sys
import os

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', config_dir="configs")

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(SCRIPT_DIR, './_instances/node/configs')


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE chtest;")
        node.query(
            '''
            CREATE TABLE chtest.reload_max_table_size (date Date, id UInt32) 
            ENGINE = MergeTree(date, id, 8192)
            '''
        )
        node.query("INSERT INTO chtest.reload_max_table_size VALUES (now(), 0)")
        yield cluster
    finally:
        #cluster.shutdown()
        pass


def test_reload_max_table_size_to_drop(start_cluster):
    config = open(config_dir + '/config.xml', 'r')
    config_lines = config.readlines()
    config.close()

    error = node.get_query_request("DROP TABLE chtest.reload_max_table_size") # change to query_and_get_error after fix
    print >> sys.stderr, 'error: ' + error.get_answer()
    print >> sys.stderr, 'path: ' + config_dir
    print >> sys.stderr, 'config: '
    for line in config_lines:
        print >> sys.stderr, line

    assert error != "" # Crashes due to illigal config

    config_lines = map(lambda line: line.replace("<max_table_size_to_drop>1", "<max_table_size_to_drop>1000000"),
                       config_lines)
    config = open(config_dir + '/config.xml', 'w')
    config.writelines(config_lines)

    time.sleep(50000)
    error = node.query_and_get_error("DROP TABLE chtest.reload_max_table_size")
    assert error == ""

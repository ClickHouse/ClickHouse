import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', stay_alive=True, main_configs=[])


def copy_file_to_container(local_path, dist_path, container_id):
    os.system("docker cp {local} {cont_id}:{dist}".format(local=local_path, cont_id=container_id, dist=dist_path))

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        copy_file_to_container(os.path.join(SCRIPT_DIR, 'user_scripts/.'), '/var/lib/clickhouse/user_scripts', node.docker_id)
        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()

def test_executable_function_no_input(started_cluster):
    assert node.query("SELECT * FROM executable('test_no_input.sh', 'TabSeparated', 'value UInt64')") == '1\n'

def test_executable_function_input(started_cluster):
    assert node.query("SELECT * FROM executable('test_input.sh', 'TabSeparated', 'value String', 'SELECT 1')") == 'Key 1\n'

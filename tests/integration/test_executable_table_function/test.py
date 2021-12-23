import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', stay_alive=True, main_configs=[])


# Something like https://reviews.llvm.org/D33325
def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


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
    skip_test_msan(node)
    assert node.query("SELECT * FROM executable('test_no_input.sh', 'TabSeparated', 'value UInt64')") == '1\n'

def test_executable_function_input(started_cluster):
    skip_test_msan(node)
    assert node.query("SELECT * FROM executable('test_input.sh', 'TabSeparated', 'value String', (SELECT 1))") == 'Key 1\n'

def test_executable_function_input_multiple_pipes(started_cluster):
    skip_test_msan(node)
    actual = node.query("SELECT * FROM executable('test_input_multiple_pipes.sh', 'TabSeparated', 'value String', (SELECT 1), (SELECT 2), (SELECT 3))")
    expected = 'Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n'
    assert actual == expected

def test_executable_function_argument(started_cluster):
    skip_test_msan(node)
    assert node.query("SELECT * FROM executable('test_argument.sh 1', 'TabSeparated', 'value String')") == 'Key 1\n'

def test_executable_storage_no_input(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query("CREATE TABLE test_table (value UInt64) ENGINE=Executable('test_no_input.sh', 'TabSeparated')")
    assert node.query("SELECT * FROM test_table") == '1\n'
    node.query("DROP TABLE test_table")

def test_executable_storage_input(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query("CREATE TABLE test_table (value String) ENGINE=Executable('test_input.sh', 'TabSeparated', (SELECT 1))")
    assert node.query("SELECT * FROM test_table") == 'Key 1\n'
    node.query("DROP TABLE test_table")

def test_executable_storage_input_multiple_pipes(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query("CREATE TABLE test_table (value String) ENGINE=Executable('test_input_multiple_pipes.sh', 'TabSeparated', (SELECT 1), (SELECT 2), (SELECT 3))")
    actual = node.query("SELECT * FROM test_table")
    expected = 'Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n'
    assert actual == expected
    node.query("DROP TABLE test_table")

def test_executable_storage_argument(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query("CREATE TABLE test_table (value String) ENGINE=Executable('test_argument.sh 1', 'TabSeparated')")
    assert node.query("SELECT * FROM test_table") == 'Key 1\n'
    node.query("DROP TABLE test_table")

def test_executable_pool_storage(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query("CREATE TABLE test_table (value String) ENGINE=ExecutablePool('test_input_process_pool.sh', 'TabSeparated', (SELECT 1))")
    assert node.query("SELECT * FROM test_table") == 'Key 1\n'
    node.query("DROP TABLE test_table")

def test_executable_pool_storage_multiple_pipes(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query("CREATE TABLE test_table (value String) ENGINE=ExecutablePool('test_input_process_pool_multiple_pipes.sh', 'TabSeparated', (SELECT 1), (SELECT 2), (SELECT 3))")
    assert node.query("SELECT * FROM test_table") == 'Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n'
    node.query("DROP TABLE test_table")

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])


# Something like https://reviews.llvm.org/D33325
def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "user_scripts/."),
            "/var/lib/clickhouse/user_scripts",
            node.docker_id,
        )
        node.restart_clickhouse()

        node.query("CREATE TABLE test_data_table (id UInt64) ENGINE=TinyLog;")
        node.query("INSERT INTO test_data_table VALUES (0), (1), (2);")

        yield cluster

    finally:
        cluster.shutdown()


def test_executable_function_no_input_bash(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT * FROM executable('no_input.sh', 'TabSeparated', 'value String')"
        )
        == "Key 0\nKey 1\nKey 2\n"
    )


def test_executable_function_no_input_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT * FROM executable('no_input.py', 'TabSeparated', 'value String')"
        )
        == "Key 0\nKey 1\nKey 2\n"
    )


def test_executable_function_input_bash(started_cluster):
    skip_test_msan(node)

    query = (
        "SELECT * FROM executable('input.sh', 'TabSeparated', 'value String', {source})"
    )
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 0\nKey 1\nKey 2\n"
    )


def test_executable_function_input_python(started_cluster):
    skip_test_msan(node)

    query = (
        "SELECT * FROM executable('input.py', 'TabSeparated', 'value String', {source})"
    )
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 0\nKey 1\nKey 2\n"
    )

    query = "SELECT * FROM executable('input.py', 'TabSeparated', (SELECT 'value String'), {source})"
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 0\nKey 1\nKey 2\n"
    )

    node.query("CREATE FUNCTION test_function AS () -> 'value String';")
    query = "SELECT * FROM executable('input.py', 'TabSeparated', (SELECT test_function()), {source})"
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 0\nKey 1\nKey 2\n"
    )
    node.query("DROP FUNCTION test_function;")


def test_executable_function_arg_eval_input_python(started_cluster):
    skip_test_msan(node)

    query = (
        "SELECT * FROM executable('input.py', 'TabSeparated', 'value String', {source})"
    )
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 0\nKey 1\nKey 2\n"
    )

    node.query("CREATE FUNCTION test_function AS () -> 'input.py';")

    query = "SELECT * FROM executable(test_function(), 'TabSeparated', 'value String', {source})"
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 0\nKey 1\nKey 2\n"
    )

    node.query("DROP FUNCTION test_function;")


def test_executable_function_input_sum_python(started_cluster):
    skip_test_msan(node)

    query = "SELECT * FROM executable('input_sum.py', 'TabSeparated', 'value UInt64', {source})"
    assert node.query(query.format(source="(SELECT 1, 1)")) == "2\n"
    assert (
        node.query(query.format(source="(SELECT id, id FROM test_data_table)"))
        == "0\n2\n4\n"
    )


def test_executable_function_input_argument_python(started_cluster):
    skip_test_msan(node)

    query = "SELECT * FROM executable('input_argument.py 1', 'TabSeparated', 'value String', {source})"
    assert node.query(query.format(source="(SELECT 1)")) == "Key 1 1\n"
    assert (
        node.query(query.format(source="(SELECT id FROM test_data_table)"))
        == "Key 1 0\nKey 1 1\nKey 1 2\n"
    )


def test_executable_function_input_signalled_python(started_cluster):
    skip_test_msan(node)

    query = "SELECT * FROM executable('input_signalled.py', 'TabSeparated', 'value String', {source})"
    assert node.query(query.format(source="(SELECT 1)")) == ""
    assert node.query(query.format(source="(SELECT id FROM test_data_table)")) == ""


def test_executable_function_input_slow_python(started_cluster):
    skip_test_msan(node)

    query = "SELECT * FROM executable('input_slow.py', 'TabSeparated', 'value String', {source})"
    assert node.query_and_get_error(query.format(source="(SELECT 1)"))
    assert node.query_and_get_error(
        query.format(source="(SELECT id FROM test_data_table)")
    )


def test_executable_function_input_multiple_pipes_python(started_cluster):
    skip_test_msan(node)
    query = "SELECT * FROM executable('input_multiple_pipes.py', 'TabSeparated', 'value String', {source})"
    actual = node.query(query.format(source="(SELECT 1), (SELECT 2), (SELECT 3)"))
    expected = "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n"
    assert actual == expected

    actual = node.query(
        query.format(source="(SELECT id FROM test_data_table), (SELECT 2), (SELECT 3)")
    )
    expected = "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 0\nKey from 0 fd 1\nKey from 0 fd 2\n"
    assert actual == expected


def test_executable_function_input_slow_python_timeout_increased(started_cluster):
    skip_test_msan(node)
    query = "SELECT * FROM executable('input_slow.py', 'TabSeparated', 'value String', {source}, SETTINGS {settings})"
    settings = "command_termination_timeout = 26, command_read_timeout = 26000, command_write_timeout = 26000"
    assert node.query(query.format(source="(SELECT 1)", settings=settings)) == "Key 1\n"
    assert (
        node.query(
            query.format(source="(SELECT id FROM test_data_table)", settings=settings)
        )
        == "Key 0\nKey 1\nKey 2\n"
    )


def test_executable_storage_no_input_bash(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query(
        "CREATE TABLE test_table (value String) ENGINE=Executable('no_input.sh', 'TabSeparated')"
    )
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_no_input_python(started_cluster):
    skip_test_msan(node)
    node.query("DROP TABLE IF EXISTS test_table")
    node.query(
        "CREATE TABLE test_table (value String) ENGINE=Executable('no_input.py', 'TabSeparated')"
    )
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_input_bash(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input.sh', 'TabSeparated', {source})"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))
    assert node.query("SELECT * FROM test_table") == "Key 1\n"
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_input_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input.py', 'TabSeparated', {source})"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))
    assert node.query("SELECT * FROM test_table") == "Key 1\n"
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_input_send_chunk_header_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input_chunk_header.py', 'TabSeparated', {source}) SETTINGS send_chunk_header=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))
    assert node.query("SELECT * FROM test_table") == "Key 1\n"
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_input_sum_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value UInt64) ENGINE=Executable('input_sum.py', 'TabSeparated', {source})"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1, 1)"))
    assert node.query("SELECT * FROM test_table") == "2\n"
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id, id FROM test_data_table)"))
    assert node.query("SELECT * FROM test_table") == "0\n2\n4\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_input_argument_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input_argument.py 1', 'TabSeparated', {source})"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))
    assert node.query("SELECT * FROM test_table") == "Key 1 1\n"
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))
    assert node.query("SELECT * FROM test_table") == "Key 1 0\nKey 1 1\nKey 1 2\n"
    node.query("DROP TABLE test_table")


def test_executable_storage_input_signalled_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input_signalled.py', 'TabSeparated', {source})"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))
    assert node.query("SELECT * FROM test_table") == ""
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))
    assert node.query("SELECT * FROM test_table") == ""
    node.query("DROP TABLE test_table")


def test_executable_storage_input_slow_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input_slow.py', 'TabSeparated', {source}) SETTINGS command_read_timeout=2500"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))
    assert node.query_and_get_error("SELECT * FROM test_table")
    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))
    assert node.query_and_get_error("SELECT * FROM test_table")
    node.query("DROP TABLE test_table")


def test_executable_function_input_multiple_pipes_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=Executable('input_multiple_pipes.py', 'TabSeparated', {source})"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1), (SELECT 2), (SELECT 3)"))
    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n"
    )
    node.query("DROP TABLE test_table")

    node.query(
        query.format(source="(SELECT id FROM test_data_table), (SELECT 2), (SELECT 3)")
    )
    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 0\nKey from 0 fd 1\nKey from 0 fd 2\n"
    )
    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=ExecutablePool('input_pool.py', 'TabSeparated', {source}) SETTINGS send_chunk_header=1, pool_size=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))

    assert node.query("SELECT * FROM test_table") == "Key 1\n"
    assert node.query("SELECT * FROM test_table") == "Key 1\n"
    assert node.query("SELECT * FROM test_table") == "Key 1\n"

    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))

    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"
    assert node.query("SELECT * FROM test_table") == "Key 0\nKey 1\nKey 2\n"

    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_sum_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value UInt64) ENGINE=ExecutablePool('input_sum_pool.py', 'TabSeparated', {source}) SETTINGS send_chunk_header=1, pool_size=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1, 1)"))

    assert node.query("SELECT * FROM test_table") == "2\n"
    assert node.query("SELECT * FROM test_table") == "2\n"
    assert node.query("SELECT * FROM test_table") == "2\n"

    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id, id FROM test_data_table)"))

    assert node.query("SELECT * FROM test_table") == "0\n2\n4\n"
    assert node.query("SELECT * FROM test_table") == "0\n2\n4\n"
    assert node.query("SELECT * FROM test_table") == "0\n2\n4\n"

    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_argument_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=ExecutablePool('input_argument_pool.py 1', 'TabSeparated', {source}) SETTINGS send_chunk_header=1, pool_size=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))

    assert node.query("SELECT * FROM test_table") == "Key 1 1\n"
    assert node.query("SELECT * FROM test_table") == "Key 1 1\n"
    assert node.query("SELECT * FROM test_table") == "Key 1 1\n"

    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))

    assert node.query("SELECT * FROM test_table") == "Key 1 0\nKey 1 1\nKey 1 2\n"
    assert node.query("SELECT * FROM test_table") == "Key 1 0\nKey 1 1\nKey 1 2\n"
    assert node.query("SELECT * FROM test_table") == "Key 1 0\nKey 1 1\nKey 1 2\n"

    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_signalled_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=ExecutablePool('input_signalled_pool.py', 'TabSeparated', {source}) SETTINGS send_chunk_header=1, pool_size=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))

    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")

    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))

    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")

    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_slow_python(started_cluster):
    skip_test_msan(node)

    query = """CREATE TABLE test_table (value String)
        ENGINE=ExecutablePool('input_slow_pool.py', 'TabSeparated', {source})
        SETTINGS send_chunk_header=1, pool_size=1, command_read_timeout=2500"""

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))

    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")

    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT id FROM test_data_table)"))

    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")
    assert node.query_and_get_error("SELECT * FROM test_table")

    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_multiple_pipes_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=ExecutablePool('input_multiple_pipes_pool.py', 'TabSeparated', {source}) SETTINGS send_chunk_header=1, pool_size=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1), (SELECT 2), (SELECT 3)"))

    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n"
    )
    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n"
    )
    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 1\n"
    )

    node.query("DROP TABLE test_table")

    node.query(
        query.format(source="(SELECT id FROM test_data_table), (SELECT 2), (SELECT 3)")
    )

    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 0\nKey from 0 fd 1\nKey from 0 fd 2\n"
    )
    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 0\nKey from 0 fd 1\nKey from 0 fd 2\n"
    )
    assert (
        node.query("SELECT * FROM test_table")
        == "Key from 4 fd 3\nKey from 3 fd 2\nKey from 0 fd 0\nKey from 0 fd 1\nKey from 0 fd 2\n"
    )

    node.query("DROP TABLE test_table")


def test_executable_pool_storage_input_count_python(started_cluster):
    skip_test_msan(node)

    query = "CREATE TABLE test_table (value String) ENGINE=ExecutablePool('input_count_pool.py', 'TabSeparated', {source}) SETTINGS send_chunk_header=1, pool_size=1"

    node.query("DROP TABLE IF EXISTS test_table")
    node.query(query.format(source="(SELECT 1)"))

    assert node.query("SELECT * FROM test_table") == "1\n"
    assert node.query("SELECT * FROM test_table") == "1\n"
    assert node.query("SELECT * FROM test_table") == "1\n"

    node.query("DROP TABLE test_table")

    node.query(query.format(source="(SELECT number FROM system.numbers LIMIT 250000)"))

    assert node.query("SELECT * FROM test_table") == "250000\n"
    assert node.query("SELECT * FROM test_table") == "250000\n"
    assert node.query("SELECT * FROM test_table") == "250000\n"

    node.query("DROP TABLE test_table")

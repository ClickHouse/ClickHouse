import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])


def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


config = """<clickhouse>
    <user_defined_executable_functions_config>/etc/clickhouse-server/functions/test_function_config.xml</user_defined_executable_functions_config>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.replace_config(
            "/etc/clickhouse-server/config.d/executable_user_defined_functions_config.xml",
            config,
        )

        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "functions/."),
            "/etc/clickhouse-server/functions",
            node.docker_id,
        )
        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "user_scripts/."),
            "/var/lib/clickhouse/user_scripts",
            node.docker_id,
        )

        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


def test_executable_function_bash(started_cluster):
    skip_test_msan(node)
    assert node.query("SELECT test_function_bash(toUInt64(1))") == "Key 1\n"
    assert node.query("SELECT test_function_bash(1)") == "Key 1\n"

    assert node.query("SELECT test_function_pool_bash(toUInt64(1))") == "Key 1\n"
    assert node.query("SELECT test_function_pool_bash(1)") == "Key 1\n"


def test_executable_function_python(started_cluster):
    skip_test_msan(node)
    assert node.query("SELECT test_function_python(toUInt64(1))") == "Key 1\n"
    assert node.query("SELECT test_function_python(1)") == "Key 1\n"

    assert node.query("SELECT test_function_pool_python(toUInt64(1))") == "Key 1\n"
    assert node.query("SELECT test_function_pool_python(1)") == "Key 1\n"


def test_executable_function_send_chunk_header_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query("SELECT test_function_send_chunk_header_python(toUInt64(1))")
        == "Key 1\n"
    )
    assert node.query("SELECT test_function_send_chunk_header_python(1)") == "Key 1\n"

    assert (
        node.query("SELECT test_function_send_chunk_header_pool_python(toUInt64(1))")
        == "Key 1\n"
    )
    assert (
        node.query("SELECT test_function_send_chunk_header_pool_python(1)") == "Key 1\n"
    )


def test_executable_function_sum_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query("SELECT test_function_sum_python(toUInt64(1), toUInt64(1))") == "2\n"
    )
    assert node.query("SELECT test_function_sum_python(1, 1)") == "2\n"

    assert (
        node.query("SELECT test_function_sum_pool_python(toUInt64(1), toUInt64(1))")
        == "2\n"
    )
    assert node.query("SELECT test_function_sum_pool_python(1, 1)") == "2\n"


def test_executable_function_argument_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query("SELECT test_function_argument_python(toUInt64(1))") == "Key 1 1\n"
    )
    assert node.query("SELECT test_function_argument_python(1)") == "Key 1 1\n"

    assert (
        node.query("SELECT test_function_argument_pool_python(toUInt64(1))")
        == "Key 1 1\n"
    )
    assert node.query("SELECT test_function_argument_pool_python(1)") == "Key 1 1\n"


def test_executable_function_signalled_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error(
        "SELECT test_function_signalled_python(toUInt64(1))"
    )
    assert node.query_and_get_error("SELECT test_function_signalled_python(1)")

    assert node.query_and_get_error(
        "SELECT test_function_signalled_pool_python(toUInt64(1))"
    )
    assert node.query_and_get_error("SELECT test_function_signalled_pool_python(1)")


def test_executable_function_slow_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error("SELECT test_function_slow_python(toUInt64(1))")
    assert node.query_and_get_error("SELECT test_function_slow_python(1)")

    assert node.query_and_get_error(
        "SELECT test_function_slow_pool_python(toUInt64(1))"
    )
    assert node.query_and_get_error("SELECT test_function_slow_pool_python(1)")


def test_executable_function_non_direct_bash(started_cluster):
    skip_test_msan(node)
    assert node.query("SELECT test_function_non_direct_bash(toUInt64(1))") == "Key 1\n"
    assert node.query("SELECT test_function_non_direct_bash(1)") == "Key 1\n"

    assert (
        node.query("SELECT test_function_non_direct_pool_bash(toUInt64(1))")
        == "Key 1\n"
    )
    assert node.query("SELECT test_function_non_direct_pool_bash(1)") == "Key 1\n"


def test_executable_function_sum_json_python(started_cluster):
    skip_test_msan(node)

    node.query("CREATE TABLE test_table (lhs UInt64, rhs UInt64) ENGINE=TinyLog;")
    node.query("INSERT INTO test_table VALUES (0, 0), (1, 1), (2, 2);")

    assert (
        node.query("SELECT test_function_sum_json_unnamed_args_python(1, 2);") == "3\n"
    )
    assert (
        node.query(
            "SELECT test_function_sum_json_unnamed_args_python(lhs, rhs) FROM test_table;"
        )
        == "0\n2\n4\n"
    )

    assert (
        node.query("SELECT test_function_sum_json_partially_named_args_python(1, 2);")
        == "3\n"
    )
    assert (
        node.query(
            "SELECT test_function_sum_json_partially_named_args_python(lhs, rhs) FROM test_table;"
        )
        == "0\n2\n4\n"
    )

    assert node.query("SELECT test_function_sum_json_named_args_python(1, 2);") == "3\n"
    assert (
        node.query(
            "SELECT test_function_sum_json_named_args_python(lhs, rhs) FROM test_table;"
        )
        == "0\n2\n4\n"
    )

    assert (
        node.query("SELECT test_function_sum_json_unnamed_args_pool_python(1, 2);")
        == "3\n"
    )
    assert (
        node.query(
            "SELECT test_function_sum_json_unnamed_args_pool_python(lhs, rhs) FROM test_table;"
        )
        == "0\n2\n4\n"
    )

    assert (
        node.query("SELECT test_function_sum_json_partially_named_args_python(1, 2);")
        == "3\n"
    )
    assert (
        node.query(
            "SELECT test_function_sum_json_partially_named_args_python(lhs, rhs) FROM test_table;"
        )
        == "0\n2\n4\n"
    )

    assert (
        node.query("SELECT test_function_sum_json_named_args_pool_python(1, 2);")
        == "3\n"
    )
    assert (
        node.query(
            "SELECT test_function_sum_json_named_args_pool_python(lhs, rhs) FROM test_table;"
        )
        == "0\n2\n4\n"
    )

    node.query("DROP TABLE test_table;")

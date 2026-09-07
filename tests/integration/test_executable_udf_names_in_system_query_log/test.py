import os
import sys
import time
import uuid

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


def check_executable_udf_functions(functions):
    if not functions:
        assert False
    execute_udf = "SELECT {}" if len(functions) == 1 else "SELECT concat({})"
    execute_udf = execute_udf.format(','.join(map(lambda function : "{}(1)".format(function), functions)))
    query_id = uuid.uuid4().hex
    node.query(execute_udf, query_id=query_id)
    node.query("SYSTEM FLUSH LOGS")

    used_executable_user_defined_functions = set(map(repr, functions))
    query_result = node.query("SELECT used_executable_user_defined_functions FROM system.query_log "
                              "WHERE type = \'QueryFinish\' AND query_id = \'{}\'".format(query_id))
    query_result = set(query_result[1:len(query_result) - 2].split(','))
    assert query_result == used_executable_user_defined_functions


def test_executable_function_single(started_cluster):
    skip_test_msan(node)
    check_executable_udf_functions(["test_function_bash"])


def test_executable_function_multiple(started_cluster):
    skip_test_msan(node)
    check_executable_udf_functions(["test_function_bash", "test_function_python"])


def test_executable_function_multiple_pool(started_cluster):
    skip_test_msan(node)
    check_executable_udf_functions(["test_function_bash", "test_function_python", "test_function_pool_bash", "test_function_pool_python"])


def test_executable_function_duplicate(started_cluster):
    skip_test_msan(node)
    check_executable_udf_functions(["test_function_bash", "test_function_bash"])

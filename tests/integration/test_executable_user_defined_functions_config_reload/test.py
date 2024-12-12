import os
import sys
import time
import logging
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["config/executable_user_defined_functions_config.xml"],
)


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


config = """<clickhouse>
    <user_defined_executable_functions_config>/etc/clickhouse-server/functions/{user_defined_executable_functions_config}</user_defined_executable_functions_config>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

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


def change_config(user_defined_executable_functions_config):
    node.replace_config(
        "/etc/clickhouse-server/config.d/executable_user_defined_functions_config.xml",
        config.format(
            user_defined_executable_functions_config=user_defined_executable_functions_config
        ),
    )
    node.query("SYSTEM RELOAD CONFIG;")


def test(started_cluster):
    # Set config with the path to the first executable user defined function.
    change_config("test_function_config.xml")

    time.sleep(10)

    assert node.query("SELECT test_function_1(toUInt64(1));") == "Key_1 1\n"

    # Change path to the second executable user defined function in config.
    change_config("test_function_config2.xml")

    time.sleep(10)

    # Check that the new executable user defined function is loaded.
    assert node.query("SELECT test_function_2(toUInt64(1))") == "Key_2 1\n"

    # Check that the previous executable user defined function was unloaded.
    node.query_and_get_error("SELECT test_function_1(toUInt64(1));")

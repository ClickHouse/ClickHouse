import os
import sys

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

        yield cluster

    finally:
        cluster.shutdown()


def test_executable_mat_view_input_python(started_cluster):
    skip_test_msan(node)

    node.query("CREATE TABLE test_data_table (id UInt64) ENGINE=TinyLog;")
    node.query("CREATE TABLE input_result_ (value String) ENGINE=TinyLog;")

    node.query(
        "CREATE MATERIALIZED VIEW input_result TO input_result_ AS "
        "SELECT * FROM executable('input.py', 'TabSeparated', 'value String', (SELECT id FROM test_data_table))"
    )

    node.query("INSERT INTO test_data_table VALUES (0), (1), (2);")

    assert node.query("SELECT value FROM input_result") == "Key 0\nKey 1\nKey 2\n"

    node.query("DROP TABLE test_data_table")
    node.query("DROP TABLE input_result_")

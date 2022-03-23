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
    <dictionaries_config>/etc/clickhouse-server/dictionaries/*_dictionary.xml</dictionaries_config>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.replace_config(
            "/etc/clickhouse-server/config.d/dictionaries_config.xml", config
        )

        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "dictionaries/."),
            "/etc/clickhouse-server/dictionaries",
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


def test_executable_input_bash(started_cluster):
    skip_test_msan(node)
    assert (
        node.query("SELECT dictGet('executable_input_bash', 'result', toUInt64(1))")
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_pool_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_implicit_input_bash(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_pool_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_input_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query("SELECT dictGet('executable_input_python', 'result', toUInt64(1))")
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_pool_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_implicit_input_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_pool_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_input_send_chunk_header_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_input_send_chunk_header_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_send_chunk_header_pool_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_implicit_input_send_chunk_header_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_send_chunk_header_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_send_chunk_header_pool_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_input_sum_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_input_sum_python', 'result', tuple(toUInt64(1), toUInt64(1)))"
        )
        == "2\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_sum_pool_python', 'result', tuple(toUInt64(1), toUInt64(1)))"
        )
        == "2\n"
    )


def test_executable_implicit_input_sum_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_sum_python', 'result', tuple(toUInt64(1), toUInt64(1)))"
        )
        == "2\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_sum_pool_python', 'result', tuple(toUInt64(1), toUInt64(1)))"
        )
        == "2\n"
    )


def test_executable_input_argument_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_input_argument_python', 'result', toUInt64(1))"
        )
        == "Key 1 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_argument_pool_python', 'result', toUInt64(1))"
        )
        == "Key 1 1\n"
    )


def test_executable_implicit_input_argument_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_argument_python', 'result', toUInt64(1))"
        )
        == "Key 1 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_argument_pool_python', 'result', toUInt64(1))"
        )
        == "Key 1 1\n"
    )


def test_executable_input_signalled_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_input_signalled_python', 'result', toUInt64(1))"
        )
        == "Default result\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_signalled_pool_python', 'result', toUInt64(1))"
        )
        == "Default result\n"
    )


def test_executable_implicit_input_signalled_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_signalled_python', 'result', toUInt64(1))"
        )
        == "Default result\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_implicit_input_signalled_pool_python', 'result', toUInt64(1))"
        )
        == "Default result\n"
    )


def test_executable_input_slow_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error(
        "SELECT dictGet('executable_input_slow_python', 'result', toUInt64(1))"
    )
    assert node.query_and_get_error(
        "SELECT dictGet('executable_input_slow_pool_python', 'result', toUInt64(1))"
    )


def test_executable_implicit_input_slow_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error(
        "SELECT dictGet('executable_implicit_input_slow_python', 'result', toUInt64(1))"
    )
    assert node.query_and_get_error(
        "SELECT dictGet('executable_implicit_input_slow_pool_python', 'result', toUInt64(1))"
    )


def test_executable_input_slow_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error(
        "SELECT dictGet('executable_input_slow_python', 'result', toUInt64(1))"
    )
    assert node.query_and_get_error(
        "SELECT dictGet('executable_input_slow_pool_python', 'result', toUInt64(1))"
    )


def test_executable_implicit_input_slow_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error(
        "SELECT dictGet('executable_implicit_input_slow_python', 'result', toUInt64(1))"
    )
    assert node.query_and_get_error(
        "SELECT dictGet('executable_implicit_input_slow_pool_python', 'result', toUInt64(1))"
    )


def test_executable_non_direct_input_bash(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_input_non_direct_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_non_direct_pool_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_implicit_non_direct_input_bash(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT dictGet('executable_input_implicit_non_direct_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_input_implicit_non_direct_pool_bash', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_source_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_simple_key_python) ORDER BY input"
        )
        == "1\tValue 1\n2\tValue 2\n3\tValue 3\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_python', 'result', toUInt64(1))"
        )
        == "Value 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_python', 'result', toUInt64(2))"
        )
        == "Value 2\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_python', 'result', toUInt64(3))"
        )
        == "Value 3\n"
    )

    assert (
        node.query(
            "SELECT * FROM dictionary('executable_source_complex_key_python') ORDER BY input"
        )
        == "1\tValue 1\n2\tValue 2\n3\tValue 3\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_python', 'result', tuple(toUInt64(1)))"
        )
        == "Value 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_python', 'result', tuple(toUInt64(2)))"
        )
        == "Value 2\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_python', 'result', tuple(toUInt64(3)))"
        )
        == "Value 3\n"
    )


def test_executable_source_argument_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_simple_key_argument_python) ORDER BY input"
        )
        == "1\tValue 1 1\n2\tValue 1 2\n3\tValue 1 3\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_argument_python', 'result', toUInt64(1))"
        )
        == "Value 1 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_argument_python', 'result', toUInt64(2))"
        )
        == "Value 1 2\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_argument_python', 'result', toUInt64(3))"
        )
        == "Value 1 3\n"
    )

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_complex_key_argument_python) ORDER BY input"
        )
        == "1\tValue 1 1\n2\tValue 1 2\n3\tValue 1 3\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_argument_python', 'result', toUInt64(1))"
        )
        == "Value 1 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_argument_python', 'result', toUInt64(2))"
        )
        == "Value 1 2\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_argument_python', 'result', toUInt64(3))"
        )
        == "Value 1 3\n"
    )


def test_executable_source_updated_python(started_cluster):
    skip_test_msan(node)
    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_simple_key_update_python) ORDER BY input"
        )
        == "1\tValue 0 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_update_python', 'result', toUInt64(1))"
        )
        == "Value 0 1\n"
    )

    time.sleep(10)

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_simple_key_update_python) ORDER BY input"
        )
        == "1\tValue 1 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_simple_key_update_python', 'result', toUInt64(1))"
        )
        == "Value 1 1\n"
    )

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_complex_key_update_python) ORDER BY input"
        )
        == "1\tValue 0 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_update_python', 'result', toUInt64(1))"
        )
        == "Value 0 1\n"
    )

    time.sleep(10)

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_complex_key_update_python) ORDER BY input"
        )
        == "1\tValue 1 1\n"
    )
    assert (
        node.query(
            "SELECT dictGet('executable_source_complex_key_update_python', 'result', toUInt64(1))"
        )
        == "Value 1 1\n"
    )

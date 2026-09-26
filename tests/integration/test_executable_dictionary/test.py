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
    assert node.query_and_get_error(
        "SELECT dictGet('executable_input_signalled_python', 'result', toUInt64(1))"
    )
    assert node.query_and_get_error(
        "SELECT dictGet('executable_input_signalled_pool_python', 'result', toUInt64(1))"
    )


def test_executable_implicit_input_signalled_python(started_cluster):
    skip_test_msan(node)
    assert node.query_and_get_error(
        "SELECT dictGet('executable_implicit_input_signalled_python', 'result', toUInt64(1))"
    )
    assert node.query_and_get_error(
        "SELECT dictGet('executable_implicit_input_signalled_pool_python', 'result', toUInt64(1))"
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
    node.restart_clickhouse()
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


def test_executable_source_exit_code_check(started_cluster):
    skip_test_msan(node)
    assert "DB::Exception" in node.query_and_get_error(
        "SELECT * FROM dictionary(executable_input_missing_executable) ORDER BY input"
    )
    assert "DB::Exception" in node.query_and_get_error(
        "SELECT dictGet('executable_input_missing_executable', 'result', toUInt64(1))"
    )

    assert (
        node.query(
            "SELECT status FROM system.dictionaries WHERE name='executable_input_missing_executable'"
        )
        == "FAILED\n"
    )
    assert "DB::Exception" in node.query(
        "SELECT last_exception FROM system.dictionaries WHERE name='executable_input_missing_executable'"
    )


def test_executable_source_stderr_reaction(started_cluster):
    skip_test_msan(node)

    # The same source, which answers and complains on `stderr`, under the two ends of
    # `stderr_reaction`. Under `throw` the diagnostic fails the load, and it is quoted in the
    # exception so that the dictionary's `last_exception` says what the command said. Under `none`
    # it is read off the pipe and dropped, and the load sees only the rows.
    assert "the source complains" in node.query_and_get_error(
        "SELECT * FROM dictionary(executable_source_stderr_throw_python) ORDER BY input"
    )
    assert "Executable generates stderr" in node.query(
        "SELECT last_exception FROM system.dictionaries WHERE name='executable_source_stderr_throw_python'"
    )

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_stderr_none_python) ORDER BY input"
        )
        == "1\tValue 1\n2\tValue 2\n3\tValue 3\n"
    )


def test_executable_source_exit_code(started_cluster):
    skip_test_msan(node)

    # A source that produces every row and then exits with `3`. `check_exit_code` is on by default,
    # so that is a failed load however complete the rows were; turned off, the rows are all that
    # counts.
    assert "Child process was exited with return code 3" in node.query_and_get_error(
        "SELECT * FROM dictionary(executable_source_exit_code_checked_python) ORDER BY input"
    )
    assert (
        node.query(
            "SELECT status FROM system.dictionaries WHERE name='executable_source_exit_code_checked_python'"
        )
        == "FAILED\n"
    )

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_exit_code_ignored_python) ORDER BY input"
        )
        == "1\tValue 1\n2\tValue 2\n3\tValue 3\n"
    )


def test_executable_source_that_lingers_after_its_output(started_cluster):
    skip_test_msan(node)

    # A source that closes its stdout after the rows and stays alive. It is given
    # `command_termination_timeout` (one second here) to exit. With `check_exit_code` on, an exit
    # code that could not be read within that budget is not a passing one: the load fails and says
    # so, instead of waiting for the command indefinitely or waving it through. With it off, the
    # budget is spent, the command is signalled, and the rows are the result.
    error = node.query_and_get_error(
        "SELECT * FROM dictionary(executable_source_lingers_python) ORDER BY input"
    )
    assert "did not exit within command_termination_timeout (1 seconds)" in error, error

    assert (
        node.query(
            "SELECT * FROM dictionary(executable_source_lingers_unchecked_python) ORDER BY input"
        )
        == "1\tValue 1\n2\tValue 2\n3\tValue 3\n"
    )


def test_executable_pool_source_stderr_reaction(started_cluster):
    skip_test_msan(node)

    # The pooled source: a worker that answers a key and complains on `stderr` at the same time.
    # Under `throw` the request that caused the line fails; under `none` the line is dropped and
    # the answer is what comes back.
    assert "the command complains" in node.query_and_get_error(
        "SELECT dictGet('executable_pool_stderr_throw_python', 'result', toUInt64(1))"
    )

    assert (
        node.query(
            "SELECT dictGet('executable_pool_stderr_none_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_pool_source_that_lingers_after_its_output(started_cluster):
    skip_test_msan(node)

    # The pooled counterpart: the worker answers the key, closes its stdout and stays alive. A
    # worker without a stdout cannot answer anyone else, so it is discarded, and it has
    # `command_termination_timeout` (one second here) to exit. With `check_exit_code` on, an exit
    # code that could not be read within that budget fails the request; with it off, the budget
    # is spent, the worker is signalled, and the answer it gave is the result.
    error = node.query_and_get_error(
        "SELECT dictGet('executable_pool_lingers_python', 'result', toUInt64(1))"
    )
    assert "did not exit within command_termination_timeout (1 seconds)" in error, error

    assert (
        node.query(
            "SELECT dictGet('executable_pool_lingers_unchecked_python', 'result', toUInt64(1))"
        )
        == "Key 1\n"
    )


def test_executable_source_rejects_shared_memory_configuration(started_cluster):
    skip_test_msan(node)

    # The shared-memory transport exists only for executable user defined functions. A dictionary
    # that asks for it has to fail, because the alternative is that it loads and runs over the pipes
    # instead - a different transport from the one it was configured for, with nothing said about it.
    for name in [
        "executable_shared_memory_rejected_python",
        "executable_pool_shared_memory_rejected_python",
    ]:
        assert "DB::Exception" in node.query_and_get_error(
            f"SELECT dictGet('{name}', 'result', toUInt64(1))"
        )
        assert "shared-memory transport is available for executable" in node.query(
            f"SELECT last_exception FROM system.dictionaries WHERE name='{name}'"
        )

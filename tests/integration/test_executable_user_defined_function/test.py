import os
import sys
import time
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

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

    for function_name in [
        "test_function_send_chunk_header_python",
        "test_function_send_chunk_header_pool_python",
    ]:
        assert node.query(f"SELECT {function_name}(toUInt64(1))") == "Key 1\n"
        assert node.query(f"SELECT {function_name}(1)") == "Key 1\n"

        assert node.query(f"SELECT {function_name}(toUInt64(1))") == "Key 1\n"
        assert node.query(f"SELECT {function_name}(1)") == "Key 1\n"

        # Test specifically HTTP protocol
        # This ensures that http_write_exception_in_output_format works as expected
        assert node.http_query(
            f"SELECT {function_name}(number) FROM numbers(10)",
            params={"max_block_size": 3, "http_write_exception_in_output_format": True},
        ) == "".join(f"Key {i}\n" for i in range(10))


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

    node.query("DROP TABLE IF EXISTS test_table;")
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


def test_executable_function_input_nullable_python(started_cluster):
    skip_test_msan(node)

    node.query("DROP TABLE IF EXISTS test_table_nullable;")
    node.query(
        "CREATE TABLE test_table_nullable (value Nullable(UInt64)) ENGINE=TinyLog;"
    )
    node.query("INSERT INTO test_table_nullable VALUES (0), (NULL), (2);")

    assert (
        node.query(
            "SELECT test_function_nullable_python(1), test_function_nullable_python(NULL)"
        )
        == "Key 1\tKey Nullable\n"
    )
    assert (
        node.query(
            "SELECT test_function_nullable_python(value) FROM test_table_nullable;"
        )
        == "Key 0\nKey Nullable\nKey 2\n"
    )

    assert (
        node.query(
            "SELECT test_function_nullable_pool_python(1), test_function_nullable_pool_python(NULL)"
        )
        == "Key 1\tKey Nullable\n"
    )
    assert (
        node.query(
            "SELECT test_function_nullable_pool_python(value) FROM test_table_nullable;"
        )
        == "Key 0\nKey Nullable\nKey 2\n"
    )

    node.query("DROP TABLE test_table_nullable;")


def test_executable_function_parameter_python(started_cluster):
    skip_test_msan(node)

    assert node.query_and_get_error(
        "SELECT test_function_parameter_python(2,2)(toUInt64(1))"
    )
    assert node.query_and_get_error("SELECT test_function_parameter_python(2,2)(1)")
    assert node.query_and_get_error("SELECT test_function_parameter_python(1)")
    assert node.query_and_get_error(
        "SELECT test_function_parameter_python('test')(toUInt64(1))"
    )

    assert (
        node.query("SELECT test_function_parameter_python('2')(toUInt64(1))")
        == "Parameter 2 key 1\n"
    )
    assert (
        node.query("SELECT test_function_parameter_python(2)(toUInt64(1))")
        == "Parameter 2 key 1\n"
    )

    # Placeholders with invalid parameter names must not be registered as
    # command parameters, so each of these functions takes zero parameters and
    # passing one fails the parameter-count check with a specific error.
    for function_name in (
        "test_function_invalid_parameter_name_python",  # name with a space: {test parameter:UInt64}
        "test_function_invalid_empty_parameter_name_python",  # empty name: {:UInt64}
        "test_function_invalid_blank_parameter_name_python",  # blank name: { :UInt64}
    ):
        assert (
            "number of parameters does not match. Expected 0. Actual 1"
            in node.query_and_get_error(
                f"SELECT {function_name}(2)(toUInt64(1))"
            )
        )


def test_executable_function_always_error_python(started_cluster):
    skip_test_msan(node)
    try:
        node.query("SELECT test_function_always_error_throw_python(1)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        assert "DB::Exception: User defined function 'test_function_always_error_throw_python' failed" in str(ex)
        assert "DB::Exception: Executable generates stderr: Fake error" in str(ex)

    query_id = uuid.uuid4().hex
    assert (
        node.query("SELECT test_function_always_error_log_python(1)", query_id=query_id)
        == "Key 1\n"
    )
    assert node.contains_in_log(
        f"{{{query_id}}} <Warning> TimeoutReadBufferFromFileDescriptor: Executable generates stderr: Fake error"
    )

    query_id = uuid.uuid4().hex
    assert (
        node.query(
            "SELECT test_function_always_error_log_first_python(1)", query_id=query_id
        )
        == "Key 1\n"
    )
    assert node.contains_in_log(
        f"{{{query_id}}} <Warning> TimeoutReadBufferFromFileDescriptor: Executable generates stderr at the beginning:  {'a' * (3 * 1024)}{'b' * 1024}\n"
    )

    query_id = uuid.uuid4().hex
    assert (
        node.query(
            "SELECT test_function_always_error_log_last_python(1)", query_id=query_id
        )
        == "Key 1\n"
    )
    assert node.contains_in_log(
        f"{{{query_id}}} <Warning> TimeoutReadBufferFromFileDescriptor: Executable generates stderr at the end:  {'b' * 1024}{'c' * (3 * 1024)}\n"
    )

    assert node.query("SELECT test_function_exit_error_ignore_python(1)") == "Key 1\n"

    try:
        node.query("SELECT test_function_exit_error_fail_python(1)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        assert "DB::Exception: User defined function 'test_function_exit_error_fail_python' failed" in str(ex)
        assert "DB::Exception: Child process was exited with return code 1" in str(ex)


def test_executable_function_none_reaction_worker_flooding_after_a_quiet_gap(started_cluster):
    """`stderr_reaction = none` must keep a chatty pooled command from blocking, gap or no gap."""
    skip_test_msan(node)

    # The command answers, stays quiet for longer than the server spends draining its stderr when it
    # takes the worker back, and only then writes two pipefuls. A check that only looks at the pipe
    # at hand-back time sees nothing and can say nothing about what comes next; what keeps the
    # promise is that the read loop polls stderr alongside stdout, so the query waiting for a
    # response drains the command writing it.
    first = node.query("SELECT test_function_pool_stderr_flood_after_gap_python(0)").strip()

    pids = {first}
    for i in range(1, 3):
        time.sleep(0.5)
        started = time.monotonic()
        pids.add(node.query(f"SELECT test_function_pool_stderr_flood_after_gap_python({i})").strip())
        elapsed = time.monotonic() - started
        assert elapsed < 5, f"query {i} took {elapsed:.1f}s - the worker was left blocked on stderr"

    assert len(pids) == 1, f"the worker was not reused: {pids}"


def test_executable_function_previous_borrow_stderr_is_not_thrown_at_the_next_query(started_cluster):
    """A previous borrow's late stderr, however much of it, must not fail the query that borrows next."""
    skip_test_msan(node)

    # The same command shape as the quiet-gap test above, under `throw`: it answers, waits out the
    # hand-back probe, then writes two pipefuls to stderr and sits blocked in `write`. The next
    # borrow finds those bytes on the pipe. They are the earlier query's, and the earlier query has
    # already succeeded - the probe finished before they arrived, which is the documented boundary -
    # so the borrow has to take them off the pipe without putting them through its own reaction:
    # under `throw`, feeding them through would fail this query for a diagnostic it did not cause.
    # Two pipefuls, because the borrow-start report is capped at a few KiB and the rest goes through
    # a separate drain: only a flood proves that drain is reaction-free as well.
    first = node.query("SELECT test_function_pool_stderr_flood_after_gap_throw_python(0)").strip()

    pids = {first}
    for i in range(1, 3):
        time.sleep(0.5)
        pids.add(node.query(f"SELECT test_function_pool_stderr_flood_after_gap_throw_python({i})").strip())

    assert len(pids) == 1, f"the worker was not reused: {pids}"
    assert node.contains_in_log(
        "A pooled command had unread output on its stderr when it was borrowed"
    )


def test_executable_function_pooled_late_stderr_fails_the_query_that_caused_it(started_cluster):
    """`stderr_reaction = throw` must fail the pooled query whose command wrote the diagnostic."""
    skip_test_msan(node)

    # A pooled worker that satisfied its row count goes straight back to the pool without being
    # waited for, so this is the one path on which nothing looks at its stderr again: the probe that
    # refuses to pool a dirty worker runs after the query has already succeeded. Under `throw` that
    # would mean the setting silently costs a worker instead of failing the query that caused the
    # output - which is the only thing it promises to do.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_pool_stderr_after_rows_python(1)")

    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "complaining right after the rows" in str(exc.value), str(exc.value)


def test_executable_function_pooled_worker_that_exited_while_idle_is_replaced(started_cluster):
    """A worker that died in the pool is replaced before the next borrow is built on it."""
    skip_test_msan(node)

    # The command answers, goes quiet long enough to be handed back, and exits non-zero while it
    # sits in the pool. Nobody is waiting for it there, so the next query is the first to find out -
    # and it must not find out by failing its own first write to a closed stdin. The pool holds one
    # process, so a replacement is visible as a different pid.
    first = node.query("SELECT test_function_pool_late_exit_python(0)").strip()
    time.sleep(0.5)
    second = node.query("SELECT test_function_pool_late_exit_python(1)").strip()

    assert first != second, f"the dead worker was reused: {first}"
    assert node.contains_in_log("exited while it was idle in the pool")


def test_executable_function_late_stdout_cannot_be_parsed_as_the_next_query_result(started_cluster):
    """A borrow must not start on a worker that already has bytes waiting on its stdout."""
    skip_test_msan(node)

    # The command answers, goes quiet long enough to be handed back to the pool, and only then
    # writes an extra row. The hand-back probe finds an empty pipe and cannot say anything about
    # what comes next, so that row is waiting when the next query borrows the same process.
    #
    # The pipe transport has no framing that would let the next query tell a stale row from its own,
    # so it must refuse to start rather than answer with somebody else's data. The first query is
    # correct; the second must fail loudly, and must not return `999999`.
    first = node.query("SELECT test_function_pool_late_stdout_python(0)").strip()
    assert first != "999999", first

    time.sleep(0.5)

    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_pool_late_stdout_python(1)")

    assert "unread output on its stdout when it was borrowed" in str(exc.value), str(exc.value)

    # And the poisoned worker is gone: a fresh process answers the query after it.
    assert node.query("SELECT test_function_pool_late_stdout_python(2)").strip() != "999999"


def test_executable_function_pooled_worker_is_reused_and_absorbs_an_immediate_extra_byte(started_cluster):
    """A byte written straight after the rows is this query's problem, not the next one's."""
    skip_test_msan(node)

    # The command answers and immediately writes one byte too many. That byte is read into *this*
    # query's own buffer along with the rows - the reader reads ahead in blocks - and dies with it.
    # It never reaches the pipe the next borrower reads, so the worker is still at a usable
    # boundary and reusing it is correct.
    #
    # This is also the guard against the opposite mistake. Treating bytes a format reader is merely
    # holding as evidence of a dirty worker condemns every well-behaved pooled command too, and
    # quietly turns `executable_pool` into a process per call - which no test that only checks
    # results would notice. Hence the assertion on the pid: one worker, four calls.
    pids = set()
    for i in range(4):
        assert node.query(f"SELECT test_function_pool_chatty_python({i})").strip() != ""
        pids.add(node.query(f"SELECT test_function_pool_chatty_python({i})").strip())

    assert len(pids) == 1, f"a healthy pooled worker was not reused: {pids}"


def test_executable_function_stderr_written_on_the_way_out_still_throws(started_cluster):
    """`stderr_reaction` applies to output produced on the way out, with or without the exit check."""
    skip_test_msan(node)

    # The command answers correctly, closes its stdout, waits out the drain that follows - which
    # stops as soon as stderr goes quiet for a moment - and only then writes its line before
    # exiting. Those bytes are found by the bounded wait that reaps the command, the last stretch in
    # which a command can write at all.
    #
    # The second query is the point: `check_exit_code` and `stderr_reaction` are independent
    # settings, so reaching that output only when the exit status is also being checked would make
    # the reaction quietly conditional on something unrelated to it.
    for name in (
        "test_function_stderr_on_the_way_out_python",
        "test_function_stderr_on_the_way_out_no_exit_check_python",
    ):
        with pytest.raises(Exception) as exc:
            node.query(f"SELECT {name}(1)")

        assert "Executable generates stderr" in str(exc.value), str(exc.value)
        assert "complaining on the way out" in str(exc.value), str(exc.value)


def test_executable_function_unreadable_exit_code_fails_the_query(started_cluster):
    """A command whose exit status cannot be read must fail the query, not be waved through."""
    skip_test_msan(node)

    # This command answers correctly and then refuses to leave: instead of exiting when its stdin is
    # closed it sleeps far past its `command_termination_timeout`, and only much later exits
    # non-zero. The server will not wait for it indefinitely - the timeout is exactly how long it
    # waits before signalling - so the status is never read.
    #
    # `check_exit_code` is at its default, and a status that could not be read is not a passing one:
    # succeeding here would make the setting mean "checked, unless the command avoids being
    # checked", which is the one command it is there for.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_lingers_python(1)")
    elapsed = time.monotonic() - started

    assert "did not exit within command_termination_timeout" in str(exc.value), str(exc.value)
    assert elapsed < 60, f"the query took {elapsed:.1f}s to give up on the command"

    # And `check_exit_code = 0` is how such a command is configured - the setting the message above
    # points at, so it has to work: nothing is checked, and the same command answers normally.
    assert node.query("SELECT test_function_lingers_ignore_python(1)") == "Key 1\n"


def test_executable_function_query_cache(started_cluster):
    '''Test for issues #77553 and #59988: Users should be able to specify if externally-defined are non-deterministic, and the query cache should treat them correspondingly.'''
    '''Also see tests/0_stateless/test_query_cache_udf_sql.sql'''
    skip_test_msan(node)

    node.query("SYSTEM CLEAR QUERY CACHE");

    # we are each testing an UDF without explicit <deterministic> tag (to check the default behavior) and two queries with <deterministic> true respectively false </deterministic>.

    # query_cache_nondeterministic_function_handling = throw

    assert node.query_and_get_error("SELECT test_function_bash(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'")
    assert node.query("SELECT count(*) FROM system.query_cache") == "0\n"

    assert node.query("SELECT test_function_bash_deterministic(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "1\n"

    assert node.query_and_get_error("SELECT test_function_bash_nondeterministic(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'")
    assert node.query("SELECT count(*) FROM system.query_cache") == "1\n"

    node.query("SYSTEM CLEAR QUERY CACHE");

    # query_cache_nondeterministic_function_handling = save

    assert node.query("SELECT test_function_bash(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "1\n"

    assert node.query("SELECT test_function_bash_deterministic(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "2\n"

    assert node.query("SELECT test_function_bash_nondeterministic(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "3\n"

    node.query("SYSTEM CLEAR QUERY CACHE");

    # query_cache_nondeterministic_function_handling = ignore

    assert node.query("SELECT test_function_bash(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "0\n"

    assert node.query("SELECT test_function_bash_deterministic(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "1\n"

    assert node.query("SELECT test_function_bash_nondeterministic(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore'") == "Key 1\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "1\n"

    node.query("SYSTEM CLEAR QUERY CACHE");

def test_executable_function_python_exception_in_query_log(started_cluster):
    '''Test that Python exceptions with tracebacks appear in query_log when stderr_reaction is configured as throw'''
    skip_test_msan(node)

    # Clear query log
    node.query("SYSTEM FLUSH LOGS")

    # Generate a unique query_id for tracking
    query_id = uuid.uuid4().hex

    # Try to execute UDF that will raise Python exception
    try:
        node.query("SELECT test_function_python_exception_default(1)", query_id=query_id)
        assert False, "Exception should have been thrown"
    except Exception as ex:
        # Verify exception is thrown
        assert "DB::Exception" in str(ex)
        assert "Executable generates stderr" in str(ex)

    # Flush logs to ensure query_log is updated
    node.query("SYSTEM FLUSH LOGS")

    # Check query_log for the exception
    # Note: type is 'ExceptionBeforeStart' because exception occurs during prepare(), not during block processing
    result = node.query(f"""
        SELECT exception
        FROM system.query_log
        WHERE query_id = '{query_id}'
          AND type = 'ExceptionBeforeStart'
        FORMAT TabSeparated
    """)

    # Parse result with TSV to ensure proper formatting
    exception_text = TSV(result).lines[0]

    # Verify specific exception components are present
    # UDF stderr must contain complete Python traceback
    required_components = [
        "Executable generates stderr: Traceback (most recent call last):",
        "in process_data",
        "result = int(value) / 0",
        "ZeroDivisionError: division by zero",
    ]

    for component in required_components:
        assert component in exception_text, f"Missing required component: {component}"


@pytest.mark.parametrize("func_name", [
    "test_function_stderr_log_last_reaction",
    "test_function_stderr_log_first_reaction",
    "test_function_stderr_none_reaction",
])
def test_executable_function_stderr_no_throw_on_success(started_cluster, func_name):
    '''Test that UDFs writing to stderr succeed under log_last/log_first/none when exit code is 0'''
    skip_test_msan(node)

    assert node.query(f"SELECT {func_name}('abc')") == "Key abc\n"


@pytest.mark.parametrize("func_name,mode", [
    ("test_function_python_exception_log_last", "log_last"),
    ("test_function_python_exception_log_first", "log_first"),
])
def test_executable_function_stderr_in_exception_on_failure(started_cluster, func_name, mode):
    '''Test that stderr content appears in exception when exit code != 0 under log_last/log_first'''
    skip_test_msan(node)

    node.query("SYSTEM FLUSH LOGS")

    query_id = uuid.uuid4().hex

    try:
        node.query(f"SELECT {func_name}(1)", query_id=query_id)
        assert False, "Exception should have been thrown"
    except Exception as ex:
        assert "DB::Exception" in str(ex)
        assert "Child process was exited with return code 1" in str(ex)

    node.query("SYSTEM FLUSH LOGS")

    result = node.query(f"""
        SELECT exception
        FROM system.query_log
        WHERE query_id = '{query_id}'
          AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
        FORMAT TabSeparated
    """)

    exception_text = TSV(result).lines[0]

    required_components = [
        "Stderr:",
        "in process_data",
        "result = int(value) / 0",
        "ZeroDivisionError: division by zero",
    ]

    for component in required_components:
        assert component in exception_text, f"Missing required component in {mode}: {component}"

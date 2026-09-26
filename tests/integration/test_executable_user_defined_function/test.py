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



def wait_until_blocked_writing(pid, timeout=30):
    # Waits until the process is blocked in `write` - here, on a full stderr pipe nobody is reading.
    # The point is to make the next borrow start only once the flood is provably under way, on any
    # machine, instead of sleeping for "long enough" and hoping.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        wchan = node.exec_in_container(["bash", "-c", f"cat /proc/{pid}/wchan 2>/dev/null || true"]).strip()
        # `pipe_write` up to Linux 6.x, `anon_pipe_write` from 7.0 on.
        if wchan.endswith("pipe_write"):
            return
        time.sleep(0.05)
    raise AssertionError(f"process {pid} did not block writing to its stderr within {timeout}s (wchan={wchan!r})")


def wait_until_blocked_reading(pid, timeout=30):
    # Waits until the process is blocked in `read` on its stdin. The commands here write everything
    # they have to write for a request - including whatever they write late, after the answer -
    # before they come back for the next one, so this is the observable form of "the late output
    # has landed": on any machine, rather than after a sleep long enough to hope for it.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        wchan = node.exec_in_container(["bash", "-c", f"cat /proc/{pid}/wchan 2>/dev/null || true"]).strip()
        # `pipe_read` up to Linux 6.x, `anon_pipe_read` from 7.0 on.
        if wchan.endswith("pipe_read"):
            return
        time.sleep(0.05)
    raise AssertionError(f"process {pid} did not come back for its next request within {timeout}s (wchan={wchan!r})")


def wait_until_exited(pid, timeout=30):
    # Waits until the process has exited - left as a zombie for the server to reap, or gone. What
    # the tests need is "provably dead before the next borrow", on any machine, rather than a sleep.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stat = node.exec_in_container(["bash", "-c", f"cat /proc/{pid}/stat 2>/dev/null || true"]).strip()
        if not stat or stat[stat.rfind(")") + 2] == "Z":
            return
        time.sleep(0.05)
    raise AssertionError(f"process {pid} did not exit within {timeout}s")

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
        # The next borrow starts once the worker is provably blocked in `write` on its full stderr
        # pipe - the state this test is about - not after a sleep long enough to hope for it.
        wait_until_blocked_writing(first)
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
        # The command's gap is long (2 s) so that the query it answered is over - probe and all -
        # before the flood starts, on any machine; and the next borrow waits for the flood to be
        # under way, rather than for a fixed time.
        wait_until_blocked_writing(first)
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
    # output - which is the only thing it promises to do. The command puts the diagnostic on the
    # pipe before it flushes its rows (see the script), so the server finds it there every time it
    # has the rows - the check is deterministic, not a race against the command's scheduling.
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
    wait_until_exited(first)
    second = node.query("SELECT test_function_pool_late_exit_python(1)").strip()

    assert first != second, f"the dead worker was reused: {first}"
    assert node.contains_in_log("exited while it was idle in the pool")


def test_executable_function_idle_dead_worker_late_stderr_is_reported(started_cluster):
    """What a worker wrote to stderr before dying in the pool is logged, not dropped with it."""
    skip_test_msan(node)

    # The worker answers, waits out the hand-back probe, writes a diagnostic and exits. Nobody is
    # reading its pipes at that point. The next borrow finds it dead and starts a replacement; the
    # diagnostic has to be reported against the process before its pipes are closed with it, or
    # the one line that explains why the worker died is lost.
    first = node.query("SELECT test_function_pool_stderr_then_late_exit_python(0)").strip()
    wait_until_exited(first)
    second = node.query("SELECT test_function_pool_stderr_then_late_exit_python(1)").strip()

    assert first != second, f"the dead worker was reused: {first}"
    assert node.contains_in_log("exited while it was idle in the pool, after writing to its stderr")
    assert node.contains_in_log("last words of the worker")


def test_executable_function_pooled_overproduction_in_one_block_invalidates_the_worker(started_cluster):
    """Extra rows inside the same chunk fail the query and cost the worker, not the next query."""
    skip_test_msan(node)

    # A row format never hands over more than `max_block_size` rows at once, so overproduction over
    # the pipes usually shows up as bytes left in the pipe, which the hand-back probe catches. A
    # block format (`Native`) hands over the command's block whole: the extra row is inside the
    # chunk, the pipe is clean, and the row count is the only thing that can catch it. It has to be
    # caught in the source, or the worker goes back to the pool as if it had answered correctly and
    # only the outer function layer complains about the count.
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_pool_native_overproduce_python(number) FROM numbers(3) FORMAT Null")
        assert "produced more" in str(exc.value), str(exc.value)

    assert node.contains_in_log("wrong result, expected 3 row(s), but the command produced more")


def test_executable_function_late_stdout_cannot_be_parsed_as_the_next_query_result(started_cluster):
    """A borrow must not start on a worker that already has bytes waiting on its stdout."""
    skip_test_msan(node)

    # The command answers with its pid, goes quiet long enough to be handed back to the pool, and
    # only then writes an extra row. The hand-back probe finds an empty pipe and cannot say
    # anything about what comes next, so that row is waiting when the next query borrows the same
    # process.
    #
    # The pipe transport has no framing that would let the next query tell a stale row from its
    # own once it has started reading - so it must not start reading on such a worker. Before
    # anything is sent the row is provably not this query's: the worker is discarded, a fresh one
    # answers, and the answer is a different pid, never `999999`.
    first = node.query("SELECT test_function_pool_late_stdout_python(0)").strip()
    assert first != "999999", first

    # The stale row is written before the command comes back for its next request, so a worker
    # blocked reading its stdin is one whose row is already on the pipe.
    wait_until_blocked_reading(first)

    second = node.query("SELECT test_function_pool_late_stdout_python(1)").strip()
    assert second != "999999", second
    assert second != first, "the worker with stale output on its stdout was reused"
    assert node.contains_in_log("had unread output on its stdout when it was borrowed")


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


def test_executable_function_pooled_worker_with_unreadable_exit_code_fails_the_query(started_cluster):
    """A pooled worker that closed its stdout and lingers is not waved through under `check_exit_code`."""
    skip_test_msan(node)

    # The command answers correctly, closes its stdout and then sleeps far past its
    # `command_termination_timeout`. A pooled worker that closed its stdout cannot go back to the
    # pool, so this is the one moment its exit status is read - and there is none to read within
    # the budget. That is the same situation the plain `executable` test above is about, and it has
    # the same answer: a status that could not be read is not a passing one.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_pool_lingers_python(1)")
    elapsed = time.monotonic() - started

    assert "closed its stdout but did not exit within command_termination_timeout" in str(exc.value), str(exc.value)
    assert elapsed < 60, f"the query took {elapsed:.1f}s to give up on the worker"

    # With `check_exit_code = 0` nothing is checked and the same command answers normally. It is
    # still discarded - a worker without a stdout is of no use to the next borrow - and the pool
    # starts a fresh one for the next call, which answers just the same.
    assert node.query("SELECT test_function_pool_lingers_ignore_python(1)") == "Key 1\n"
    assert node.query("SELECT test_function_pool_lingers_ignore_python(2)") == "Key 2\n"


def test_executable_function_discarded_pooled_worker_sees_the_end_of_its_stdin(started_cluster):
    """A pooled worker that is not going back to the pool gets EOF on its stdin before its exit is waited for."""
    skip_test_msan(node)

    # The command answers one row of three, closes its stdout and reads its stdin to the end
    # before exiting - which is how a pooled command exits. It answered short, so it is not going
    # back to the pool and its exit status is read; a pooled worker's stdin is kept open across
    # borrows, and a server that waited for the exit with it still open would sit out the whole
    # `command_termination_timeout` (20 s here) on a command that is only waiting to be let go,
    # then fail the query for an exit code it never got to see. The stdin is closed first, the
    # command exits on the EOF within a moment, and the query fails for the short answer.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_pool_short_answer_python(number) FROM numbers(3)")
    elapsed = time.monotonic() - started

    assert "did not exit within command_termination_timeout" not in str(exc.value), str(exc.value)
    assert elapsed < 10, f"the query took {elapsed:.1f}s: the worker sat out its termination timeout"


def test_executable_function_pooled_worker_that_closed_stdout_after_answering_sees_the_end_of_its_stdin(started_cluster):
    """A pooled worker that answered in full and hung up its stdout gets EOF on its stdin before its exit code is read."""
    skip_test_msan(node)

    # The command answers, closes its stdout and reads its stdin to the end before exiting 0. A
    # worker without a stdout cannot go back to the pool, so under `check_exit_code` its exit code
    # is read right there; a pooled worker's stdin is kept open across borrows, and a server that
    # waited with it still open would sit out `command_termination_timeout` (20 s here) on a
    # command only waiting to be let go, then fail a query whose answer it already had. The stdin
    # is closed first, the command exits at once, the exit code is 0, and the query succeeds.
    started = time.monotonic()
    assert node.query("SELECT test_function_pool_answer_close_stdout_wait_stdin_python(1)") == "Key 1\n"
    elapsed = time.monotonic() - started
    assert elapsed < 10, f"the query took {elapsed:.1f}s: the worker sat out its termination timeout"

    # A fresh worker serves the next call the same way.
    assert node.query("SELECT test_function_pool_answer_close_stdout_wait_stdin_python(2)") == "Key 2\n"


def test_executable_function_pooled_worker_logging_after_its_rows_is_kept(started_cluster):
    """Under a `log*` reaction a line written after the rows is logged, and the worker is reused."""
    skip_test_msan(node)

    # The command answers with its pid and then writes a line to stderr - after the rows, so the
    # line is on the pipe when the worker is handed back. Under `throw` that line would be a
    # verdict on a query that has already succeeded, and the worker is discarded so that it is not
    # pinned on the next one. Under `log_last` it is a log line: it is taken off the pipe and
    # logged against the query that caused it, and the worker goes back to the pool - one process
    # serves every call. Discarding it would quietly turn `executable_pool` into a process per
    # call for every command that logs after its rows.
    pids = set()
    for i in range(4):
        pids.add(node.query(f"SELECT test_function_pool_pid_then_stderr_log_python({i})").strip())

    assert len(pids) == 1, f"a worker that only logged after its rows was not reused: {pids}"
    assert node.contains_in_log("logging right after the rows")


def test_executable_function_zero_termination_timeout_waits_for_the_exit_status(started_cluster):
    """`command_termination_timeout = 0` does not turn the wait for the exit status into a single probe."""
    skip_test_msan(node)

    # The command answers, closes its stdout and exits 300 ms later. With `check_exit_code` the
    # server waits for its exit status after the output ends; a zero termination timeout bounds
    # that wait at zero and would find the command not yet exited - and fail the query - or not,
    # depending on scheduling. Zero means "signal at once" for a command being discarded, and for
    # the wait for an exit status it means no bound, as a blocking wait had: the query succeeds
    # every time.
    for i in range(3):
        assert node.query(f"SELECT test_function_exit_after_a_moment_python({i})") == f"Key {i}\n"


def test_executable_function_stray_stdout_does_not_hide_late_stderr_without_exit_check(started_cluster):
    """A stray stdout write after the rows must not kill the command before its late stderr is seen."""
    skip_test_msan(node)

    # `check_exit_code = 0`, `stderr_reaction = throw`: the exit status is nobody's business, the
    # diagnostic is. The command answers, writes a stray line to stdout 300 ms later and only then
    # its diagnostic. A server that closed the command's stdout as soon as it had the rows would
    # have that stray write kill the command with SIGPIPE, diagnostic unwritten, and the query
    # would succeed; the stray line is read and discarded instead, and the diagnostic fails the
    # query.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_stray_stdout_then_stderr_python(1)")

    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "late complaint" in str(exc.value), str(exc.value)


def test_executable_function_lingering_command_with_no_grace_and_no_exit_check_is_not_waited_for(started_cluster):
    """`command_termination_timeout = 0` without `check_exit_code` signals a lingering command at once."""
    skip_test_msan(node)

    # The command answers, closes its stdout and never exits. With the exit code not checked the
    # server waits only for the command's last words on stderr (the default reaction logs them),
    # and a zero grace period means exactly that here: no wait, signal at once. Unbounded it is
    # only for the exit status - a wait that this configuration does not ask for - so the query
    # must not hang on a command that is never going to write anything.
    started = time.monotonic()
    assert node.query("SELECT test_function_lingers_ignore_no_grace_python(1)") == "Key 1\n"
    elapsed = time.monotonic() - started
    assert elapsed < 10, f"the query waited {elapsed:.1f}s for a command that never exits"


def test_executable_function_pooled_lingering_worker_with_no_grace_fails_the_query_at_once(started_cluster):
    """A pooled worker that closes its stdout and never exits does not hang the query under a zero grace period."""
    skip_test_msan(node)

    # The worker answers, closes its stdout and lingers; it cannot go back to the pool, so under
    # `check_exit_code` its exit status is read - and with `command_termination_timeout = 0` there
    # is no grace for reading it. A pooled worker being discarded was never waited for without a
    # bound, and must not be now: zero means zero, the status cannot be read, the query fails at
    # once and the worker is signalled - rather than the query, and the pool's only slot, hanging
    # forever with no way to cancel.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_pool_lingers_no_grace_python(1)")
    elapsed = time.monotonic() - started

    assert "did not exit within command_termination_timeout" in str(exc.value), str(exc.value)
    assert elapsed < 10, f"the query waited {elapsed:.1f}s for a pooled worker that never exits"

    # The slot is free: the next call is served by a fresh worker.
    with pytest.raises(Exception):
        node.query("SELECT test_function_pool_lingers_no_grace_python(2)")


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

def test_executable_function_deterministic_declaration_deduplicates(started_cluster):
    '''A function declared deterministic is entitled to run once per distinct value of a
    LowCardinality argument instead of once per row.'''
    skip_test_msan(node)

    node.query("DROP TABLE IF EXISTS low_cardinality_argument")
    node.query(
        "CREATE TABLE low_cardinality_argument (v LowCardinality(UInt64)) ENGINE = MergeTree ORDER BY tuple()",
        settings={"allow_suspicious_low_cardinality_types": 1},
    )
    node.query("INSERT INTO low_cardinality_argument SELECT number % 2 FROM numbers(100)")

    def run(function_name):
        query_id = uuid.uuid4().hex
        # `sum` over the result keeps the call from being pruned as an unused column.
        result = node.query(
            f"SELECT sum(length({function_name}(v))) FROM low_cardinality_argument",
            query_id=query_id,
        )
        node.query("SYSTEM FLUSH LOGS")
        input_bytes = node.query(
            f"""SELECT ProfileEvents['ExecutableUserDefinedFunctionInputBytes']
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'QueryFinish'"""
        )
        return result.strip(), int(input_bytes.strip())

    deterministic_result, deterministic_bytes = run("test_function_bash_deterministic")
    nondeterministic_result, nondeterministic_bytes = run("test_function_bash_nondeterministic")

    # The table holds 100 rows over 2 distinct values, and the argument is one line per row on
    # the child's stdin, so the declaration decides how much reaches the child.
    assert nondeterministic_bytes >= 100
    assert deterministic_bytes * 10 < nondeterministic_bytes
    # Deduplicating must not change the answer for a function that is deterministic in fact.
    assert deterministic_result == nondeterministic_result

    node.query("DROP TABLE low_cardinality_argument")

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

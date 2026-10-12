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


def test_executable_function_determinism_in_distributed_predicate_push_down(
    started_cluster,
):
    skip_test_msan(node)

    # `ReadFromRemote` pushes a predicate over a column of a distributed subquery into the `HAVING`
    # of the query sent to the shard, unless `hasNonRewritableFunction` finds a non-deterministic
    # function in the shard's `SELECT` list. `ExpressionInfoVisitor` reads the `deterministic` flag
    # of an executable UDF from its configuration: a parametric UDF must not be instantiated there
    # (an empty parameter list raises `BAD_ARGUMENTS`), and a UDF declared deterministic must still
    # be pushed down.
    node.query("DROP TABLE IF EXISTS test_table_distributed_predicate")
    node.query(
        "CREATE TABLE test_table_distributed_predicate (k UInt64) ENGINE = MergeTree ORDER BY k"
    )
    node.query(
        "INSERT INTO test_table_distributed_predicate SELECT number FROM numbers(4)"
    )

    settings = {
        "serialize_query_plan": 0,
        "allow_push_predicate_ast_for_distributed_subqueries": 1,
    }
    cases = {
        "test_function_parameter_python(2)(k)": False,
        "test_function_bash_nondeterministic(k)": False,
        "test_function_bash_deterministic(k)": True,
    }
    for expression, pushed_down in cases.items():
        log_comment = "distributed_predicate_" + expression.split("(")[0]
        assert (
            node.query(
                f"SELECT count() FROM (SELECT {expression} AS v"
                " FROM remote('127.0.0.2', default, test_table_distributed_predicate))"
                " WHERE v != ''",
                settings={**settings, "log_comment": log_comment},
            )
            == "4\n"
        )
        node.query("SYSTEM FLUSH LOGS query_log")
        shard_query = node.query(
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish'"
            f" AND is_initial_query = 0 AND log_comment = '{log_comment}'"
            " AND query LIKE '%test_table_distributed_predicate%'"
        )
        assert expression.split("(")[0] in shard_query
        assert ("HAVING" in shard_query) == pushed_down, shard_query

    node.query("DROP TABLE test_table_distributed_predicate")


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


def pooled_shared_memory_bytes():
    # Exactly the bytes of shared-memory region that pooled workers hold while idle. This metric is
    # added to and taken from in the same two places the server-wide memory charge is, so it is that
    # charge, told apart from everything else the server allocates. Reading the global memory tracker
    # instead would mean comparing deltas across a window in which anything at all may allocate or
    # free - a measurement with a tolerance, not an assertion.
    return int(
        node.query(
            "SELECT value FROM system.metrics "
            "WHERE metric = 'ExecutableUDFSharedMemoryPooledBytes'"
        ).strip()
    )


def pooled_shared_memory_baseline(region_size, timeout=30):
    # What pooled workers of *other* functions hold, which the assertions below are measured against.
    # The caller has reloaded the function it is about to measure, so the one contribution that must
    # not be in here is its own - which the absence of regions of its (unique to it) size proves.
    #
    # Waited for rather than sampled at once, on both counts. A reload drops the old function object
    # on a path nothing waits for, and so does the test before this one when its own worker goes
    # away: a baseline taken while either is still draining has a charge in it that is about to
    # disappear, and every assertion measured against it then reads "back to the number we started
    # from" when what it means to prove is "the idle charge was released". So: the regions of the
    # size under test have to be gone, and the metric has to have stopped moving - two equal reads
    # in a row, with the regions already gone at the first of them.
    deadline = time.monotonic() + timeout
    previous = None
    while True:
        regions_gone = region_size not in shm_region_sizes()
        value = pooled_shared_memory_bytes()
        if regions_gone and value == previous:
            return value

        assert time.monotonic() < deadline, (
            f"the pooled shared-memory charge did not settle within {timeout}s: "
            f"{value} bytes, regions of {region_size} bytes "
            f"{'gone' if regions_gone else 'still open'}"
        )
        previous = value if regions_gone else None
        time.sleep(0.2)


def wait_for_pooled_shared_memory_bytes(expected, description, timeout=30):
    # The charge is moved on paths the query does not wait for - the source's cleanup on the way out,
    # and, for a reload, the loader dropping the old function object - so give it a moment to land.
    # The value waited for is exact, so this either reaches it or the test has genuinely failed.
    deadline = time.monotonic() + timeout
    while True:
        value = pooled_shared_memory_bytes()
        if value == expected:
            return value
        assert (
            time.monotonic() < deadline
        ), f"{description}: expected {expected} bytes charged to the server, found {value}"
        time.sleep(0.2)


def profile_event_value(event):
    return int(
        node.query(
            f"SELECT ifNull(sum(value), 0) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def shm_regions():
    # The regions the server holds right now, as `(inode, size)` of its `memfd` descriptors. A region
    # has no name in any filesystem, so the server's descriptor is the only place it can be seen
    # from outside. The inode tells one region from another, and the size of the file behind the
    # descriptor is exactly the region's size: the file is sealed against shrinking and nobody but
    # the server ever grows it. The command's copy of the descriptor lives in the command's process,
    # so every region is counted once.
    pid = node.get_process_pid("clickhouse server")
    assert pid is not None
    listing = node.exec_in_container(
        [
            "bash",
            "-c",
            f"for fd in /proc/{pid}/fd/*; do "
            f'if [ "$(readlink "$fd")" = "/memfd:clickhouse_udf_shm (deleted)" ]; '
            f'then stat -L -c "%i %s" "$fd"; fi; done',
        ]
    ).splitlines()
    return sorted(tuple(int(field) for field in line.split()) for line in listing if line)


def shm_region_sizes():
    return sorted(size for _, size in shm_regions())


def wait_for_memory_worker_ticks(ticks=2, timeout=30):
    # The background memory worker replaces the server-wide tracker's value with a measurement on
    # every tick (`memory_worker_correct_memory_tracker`, on by default). Waits until it has done so
    # at least `ticks` times since the call, so that what is read afterwards is the corrected value.
    start = profile_event_value("MemoryWorkerRun")
    deadline = time.monotonic() + timeout
    while profile_event_value("MemoryWorkerRun") < start + ticks:
        assert time.monotonic() < deadline, f"the memory worker did not run {ticks} times within {timeout}s"
        time.sleep(0.1)


def tracked_server_memory():
    # The amount of the server-wide memory tracker - the number `max_server_memory_usage` is
    # checked against.
    return int(node.query("SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'").strip())


def container_available_memory():
    # What the container could still allocate: the host's `MemAvailable`, or what is left under
    # the cgroup's limit when there is one, whichever is smaller. Read from inside the container,
    # since that is where the regions are allocated.
    script = r"""
        avail_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
        avail=$((avail_kb * 1024))
        for f in /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory/memory.limit_in_bytes; do
            if [ -r "$f" ]; then
                limit=$(cat "$f")
                case "$limit" in max|9223372036854771712) ;; *)
                    for u in /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory/memory.usage_in_bytes; do
                        [ -r "$u" ] && usage=$(cat "$u") && left=$((limit - usage)) && [ "$left" -lt "$avail" ] && avail=$left
                    done ;;
                esac
            fi
        done
        echo "$avail"
    """
    return int(node.exec_in_container(["bash", "-c", script]).strip())


def resident_server_memory():
    # The tracker also refuses an allocation when the resident size it is told about plus the
    # allocation would pass the limit. Which figure it is told about depends on the environment -
    # the cgroup's usage where there is one, the allocator's resident size otherwise - so take the
    # largest of everything on offer: a limit set above that is above whichever one is in use.
    raw = node.query(
        "SELECT max(value) FROM system.asynchronous_metrics "
        "WHERE metric IN ('MemoryResident', 'CGroupMemoryUsed', 'jemalloc.resident')"
    ).strip()
    return int(float(raw))


def test_shared_memory_udf_idle_pooled_region_counts_against_the_server_limit(started_cluster):
    # The exact idle charge is checked by the stateless test
    # `05321_executable_udf_shared_memory_accounting` through the pooled-bytes metric; this is the
    # charge doing what a charge is for. An idle pooled worker holds a region nobody is querying
    # through, and the charge counts against `max_server_memory_usage`. Two things have to be true
    # for that, and both are checked here: the server-wide tracker's amount goes up by the region
    # while the worker is idle, and an allocation that would fit into the server's headroom is
    # refused while it does.
    #
    # Both are checked after the background memory worker has corrected the tracker with a
    # measurement (`memory_worker_correct_memory_tracker` is left at its default): none of the
    # measurements it uses sees the pages of a region, so the charge has to survive the correction
    # through `MemoryTrackingUnmeasured`, or the idle region would stop counting on the next tick.
    #
    # The allocation is a second region of the same size, for a second function: it is charged
    # before it is created, so nothing is actually allocated when it is refused, and it leaves the
    # resident size alone - the tracker checks that too, and a probe that touched hundreds of MiB
    # would move it. The limit leaves room for one region and a half above what the server uses
    # now: the first region fits (a server over its limit at rest refuses every query, the one
    # that would drop the pool included), the second does not. `max_server_memory_usage` is
    # applied on `SYSTEM RELOAD CONFIG`, so no second server is needed.
    #
    # The resident size can run ahead of the tracked amount (allocator retention, cgroup page
    # cache, sanitizer shadow), and the limit has to sit above it; the room the second region has
    # to overflow shrinks by that gap, and if it is gone the check cannot be made here.
    # Large enough for the assertions to stand clear of the tolerance and of the resident/tracked
    # gap, which on a debug or sanitizer build runs to hundreds of MiB (allocator retention,
    # shadow memory, the container's page cache): 768 MiB of committed pages per region, one
    # region at a time. That is a real amount on a shared machine, so the test first checks that
    # the container has it to spare, and skips otherwise rather than be the thing that runs the
    # machine out of memory.
    region_size = 768 * 1048576
    tolerance = 32 * 1048576
    first = "test_function_shm_server_limit_python"

    available = container_available_memory()
    if available < 2 * region_size + 1024 * 1048576:
        pytest.skip(f"only {available >> 20} MiB of memory available to the container; the test needs two regions of {region_size >> 20} MiB with room to spare")
    second = "test_function_shm_server_limit_second_python"

    node.query(f"SYSTEM RELOAD FUNCTION {first}")
    node.query(f"SYSTEM RELOAD FUNCTION {second}")
    pooled_before = pooled_shared_memory_baseline(region_size)
    wait_for_memory_worker_ticks()
    tracked_before = tracked_server_memory()
    base = max(tracked_before, resident_server_memory())
    gap = base - tracked_before
    if gap + tolerance >= region_size // 2:
        pytest.skip(f"resident memory exceeds tracked memory by {gap >> 20} MiB, leaving the second region no room to overflow")

    limit_config = "/etc/clickhouse-server/config.d/tight_server_memory_limit.xml"

    def set_server_limit(limit):
        if limit is None:
            node.exec_in_container(["rm", "-f", limit_config])
        else:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"printf '%s' '<clickhouse><max_server_memory_usage>{limit}"
                    f"</max_server_memory_usage></clickhouse>' > {limit_config}",
                ]
            )
        node.query("SYSTEM RELOAD CONFIG")

    try:
        # Inside the `try`, so that a limit that was written but whose reload failed - or a reload
        # that failed halfway - is still taken back below: nothing after this test may run under
        # a server limit it did not set.
        set_server_limit(base + region_size + region_size // 2)

        # An idle worker sits in the pool holding a region no query is charged for. It went under
        # the limit: its creation is charged to the query that made it, and one region fits.
        assert node.query(f"SELECT {first}(1)") == "Key 1\n"
        assert shm_region_sizes().count(region_size) == 1
        wait_for_pooled_shared_memory_bytes(
            pooled_before + region_size, "the idle region is not charged to the server"
        )

        # Directly on the tracker the limit is enforced against, not just on the metric: the whole
        # region, once, give or take what the server allocates and frees on its own meanwhile -
        # and still there after the memory worker has replaced the tracker's value.
        wait_for_memory_worker_ticks()
        tracked_idle = tracked_server_memory()
        assert abs((tracked_idle - tracked_before) - region_size) < tolerance, (tracked_before, tracked_idle)

        # The idle region has eaten into the headroom, so a second one is refused ...
        with pytest.raises(Exception) as exc:
            node.query(f"SELECT {second}(1) FORMAT Null")
        assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value), str(exc.value)
        assert "total" in str(exc.value), str(exc.value)
        # ... before it is created: refused is refused, not created and then rolled back.
        assert shm_region_sizes().count(region_size) == 1

        # Once the first pool is dropped, its region and charge go with it, and the second region
        # fits under the same limit: the only thing that changed is the idle region.
        node.query(f"SYSTEM RELOAD FUNCTION {first}")
        wait_for_pooled_shared_memory_bytes(
            pooled_before, "the idle region's charge was not released with the pool"
        )
        assert node.query(f"SELECT {second}(1)") == "Key 1\n"
        assert shm_region_sizes().count(region_size) == 1
    finally:
        # Both pools, whichever of them an assertion above left standing: an idle worker of either
        # holds 768 MiB the rest of this suite would otherwise run next to.
        set_server_limit(None)
        node.query(f"SYSTEM RELOAD FUNCTION {first}")
        node.query(f"SYSTEM RELOAD FUNCTION {second}")

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# The server puts its region files in a per-uid subdirectory of the configured path, so that a
# shared parent like `/dev/shm` can hold one of these per OS user. The tests do not care which uid
# the server runs as, so they look for any of them.
REGION_DIRECTORY_GLOB = "*/.clickhouse-udf-shared-memory-*"
LATE_UNLINK_MARKER = "/tmp/clickhouse_shm_udf_late_unlink_once"
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[],
    tmpfs=[
        "/shm_udf_tiny:size=1M",
        "/shm_udf_accounting:size=1M",
        "/shm_udf_pipeline_accounting:size=32M",
        "/shm_udf_discard:size=1M",
        "/shm_udf_trim:size=4M",
        "/shm_udf_shrink:size=1M",
        "/shm_udf_error:size=1M",
        "/shm_udf_die:size=1M",
        "/shm_udf_tamper:size=128M",
        "/shm_udf_idle_charge:size=256M",
        "/shm_udf_pipeline_trim:size=8M",
        "/shm_udf_pipeline_idle:size=192M",
    ],
)


def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


IDLE_CHARGE_REGION_SIZE = 64 * 1048576
# `shared_memory_pipeline` keeps two regions of this size per pooled worker.
PIPELINE_IDLE_REGION_SIZE = 32 * 1048576


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


def pooled_shared_memory_baseline(path):
    # What pooled workers of *other* functions hold, which the assertions below are measured against.
    # It is a fixed number rather than a settling one: the metric moves only when a shared-memory
    # UDF is borrowed, returned or discarded, the tests in this file run one at a time, and a worker
    # an earlier test left in its pool just sits there charged. The caller has reloaded the function
    # it is about to measure, so the one contribution that must not be in here is its own - which
    # the absence of its region files proves.
    assert shm_file_count(path) == 0
    return pooled_shared_memory_bytes()


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


def query_profile_event(query_id, event):
    # Per-query rather than the server-wide counter: this one has to be attributed to a specific
    # invocation. The row type is not pinned to QueryFinish because some of these queries are
    # expected to fail, and their counters live on the exception row.
    node.query("SYSTEM FLUSH LOGS")
    raw = node.query(
        f"SELECT ProfileEvents['{event}'] FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type != 'QueryStart' "
        "ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip()
    return int(raw) if raw else 0


def profile_event_value(event):
    return int(
        node.query(
            f"SELECT ifNull(sum(value), 0) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def shm_file_count(path):
    return int(
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"find {path} -mindepth 2 -maxdepth 2 -path '{REGION_DIRECTORY_GLOB}' "
                f"-name 'clickhouse_udf_shm_*' | wc -l",
            ]
        ).strip()
    )


def tiny_shm_file_count():
    return shm_file_count("/shm_udf_tiny")


def discard_shm_file_count():
    return shm_file_count("/shm_udf_discard")


def shm_file_names(path):
    listing = node.exec_in_container(
        [
            "bash",
            "-c",
            f"find {path} -mindepth 2 -maxdepth 2 -path '{REGION_DIRECTORY_GLOB}' "
            f"-name 'clickhouse_udf_shm_*' -printf '%f\\n'",
        ]
    ).split()
    return sorted(listing)


def shm_file_sizes(path):
    find = (
        f"find {path} -mindepth 2 -maxdepth 2 -path '{REGION_DIRECTORY_GLOB}' "
        f"-name 'clickhouse_udf_shm_*' -printf '%s\\n'"
    )
    listing = node.exec_in_container(["bash", "-c", find]).split()
    return sorted(int(size) for size in listing)


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


def test_shared_memory_udf_single(started_cluster):
    skip_test_msan(node)

    assert node.query("SELECT test_function_shm_python(1)") == "Key 1\n"
    assert (
        node.query("SELECT test_function_shm_python(number) FROM numbers(3)")
        == "Key 0\nKey 1\nKey 2\n"
    )


def test_shared_memory_udf_single_closes_stdin_before_wait(started_cluster):
    skip_test_msan(node)

    # `shm_udf.py` exits only after stdin EOF. A non-pooled shared-memory source must close stdin
    # before waiting for the child after producing the fixed number of result rows.
    assert (
        node.query("SELECT test_function_shm_python(1) SETTINGS max_execution_time=5")
        == "Key 1\n"
    )


def test_shared_memory_udf_pool(started_cluster):
    skip_test_msan(node)

    # Call several times to exercise reuse of the same shared-memory file across pool borrows.
    for i in range(5):
        assert node.query(f"SELECT test_function_shm_pool_python({i})") == f"Key {i}\n"

    assert (
        node.query("SELECT test_function_shm_pool_python(number) FROM numbers(4)")
        == "Key 0\nKey 1\nKey 2\nKey 3\n"
    )


def test_shared_memory_udf_pool_counts_allocated_bytes_once(started_cluster):
    skip_test_msan(node)

    event = "ExecutableUDFSharedMemoryAllocatedBytes"
    before = profile_event_value(event)

    for i in range(5):
        assert (
            node.query(f"SELECT test_function_shm_pool_profile_event_python({i})")
            == f"Key {i}\n"
        )

    after = profile_event_value(event)

    # `ExecutableUDFSharedMemoryAllocatedBytes` tracks actual region capacity, not per-query
    # memory charges, so pooled reuse must not count the same region on every borrow.
    assert after - before == 1048576


def test_shared_memory_udf_pool_region_is_charged_to_query(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pool_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=524288, max_untracked_memory=0"
        )

    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value)


def test_shared_memory_udf_pipeline_charges_both_regions_to_query(started_cluster):
    skip_test_msan(node)

    # Both functions use 8 MiB regions. The limit leaves almost 8 MiB for the one-region control's
    # query overhead, but it is still one byte short of the 16 MiB region charge incurred by
    # `shared_memory_pipeline`. The pipeline query can only pass if its second region is unaccounted.
    memory_limit = 16 * 1048576 - 1
    assert (
        node.query(
            "SELECT test_function_shm_accounting_control_python(1) "
            f"SETTINGS max_memory_usage={memory_limit}, max_untracked_memory=0"
        )
        == "Key 1\n"
    )

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pipeline_accounting_python(1) FORMAT Null "
            f"SETTINGS max_memory_usage={memory_limit}, max_untracked_memory=0"
        )

    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value)


def test_shared_memory_udf_pool_failed_first_borrow_drops_created_region(started_cluster):
    skip_test_msan(node)

    assert shm_file_count("/shm_udf_accounting") == 0

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pool_accounting_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=524288, max_untracked_memory=0"
        )

    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value)
    assert shm_file_count("/shm_udf_accounting") == 0

    successful_query = (
        "SELECT test_function_shm_pool_accounting_python(1) "
        "SETTINGS max_memory_usage=10485760, max_untracked_memory=0"
    )
    worker_pid = node.query(successful_query).strip()
    assert worker_pid.isdigit()

    # The region now exists, so this borrow fails while charging it in the source constructor.
    # No request has reached the worker and the pool must remain usable.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pool_accounting_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=524288, max_untracked_memory=0"
        )

    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value)
    # No request reached the worker, so the pool must hand back the very same process - a new pid
    # here would mean a healthy worker was discarded (and its region rebuilt) over a failure that
    # never touched it.
    assert node.query(successful_query).strip() == worker_pid


def test_shared_memory_udf_pool_short_result_does_not_hang(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_pool_short_python(number) FROM numbers(3)")

    assert "wrong result, expected 3 row(s), actual 1" in str(exc.value)


def test_shared_memory_udf_pool_overproduction_invalidates_worker(started_cluster):
    skip_test_msan(node)

    # The command returns more rows than requested. The server must fail with a "wrong result"
    # error (detected before the oversized chunk leaves the source) rather than silently returning
    # extra rows. Because the worker is invalidated, a repeated call must fail the same way — it
    # must not reuse the bad worker as valid nor return a stale/oversized result.
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query(
                "SELECT test_function_shm_pool_over_python(number) FROM numbers(3) FORMAT Null"
            )
        assert "wrong result, expected 3 row(s)" in str(exc.value)

    # A subsequent valid pooled shared-memory UDF still works (the pool is not corrupted).
    assert (
        node.query("SELECT test_function_shm_pool_python(number) FROM numbers(3)")
        == "Key 0\nKey 1\nKey 2\n"
    )


def test_shared_memory_udf_pool_discard_releases_region(started_cluster):
    skip_test_msan(node)

    assert discard_shm_file_count() == 0

    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query(
                "SELECT test_function_shm_pool_discard_over_python(number) "
                "FROM numbers(3) FORMAT Null"
            )
        assert "wrong result, expected 3 row(s)" in str(exc.value)
        assert discard_shm_file_count() == 0


def test_shared_memory_udf_pipeline_pool_overproduction_invalidates_worker(started_cluster):
    skip_test_msan(node)

    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query(
                "SELECT test_function_shm_pipeline_pool_over_python(number) FROM numbers(3) FORMAT Null"
            )
        assert "wrong result, expected 3 row(s)" in str(exc.value)


def test_shared_memory_udf_grows(started_cluster):
    skip_test_msan(node)

    # The region starts at 16 bytes but may grow up to 1 MiB. A chunk whose serialized input
    # exceeds the initial size forces the region to grow instead of failing. shm_udf_grow.py
    # echoes the input back, so the result equals the input.
    expected = "".join(f"{i}\n" for i in range(200))
    assert (
        node.query("SELECT test_function_shm_grow_python(number) FROM numbers(200)")
        == expected
    )


def test_shared_memory_udf_input_fills_the_region_exactly(started_cluster):
    skip_test_msan(node)

    # `numbers(3)` serializes to exactly the 6 bytes of the region, which may not grow
    # (`shared_memory_max_size` defaults to `shared_memory_size`). Filling the region to its very
    # last byte must not be mistaken for needing one byte more. shm_udf_grow.py echoes the input
    # back at offset 0, so the result fits exactly as well.
    assert (
        node.query("SELECT test_function_shm_exact_python(number) FROM numbers(3)")
        == "0\n1\n2\n"
    )


def test_shared_memory_udf_grows_with_room_for_the_result(started_cluster):
    skip_test_msan(node)

    # `shm_udf.py` writes its result right after the input, so the region the server grew to fit
    # the input alone leaves it no room: it asks for a larger region through the control protocol
    # and the server enlarges it and re-sends the request.
    expected = "".join(f"Key {i}\n" for i in range(200))
    assert (
        node.query(
            "SELECT test_function_shm_grow_after_input_python(number) FROM numbers(200)"
        )
        == expected
    )


def test_shared_memory_udf_grows_pool(started_cluster):
    skip_test_msan(node)

    # Same, but through the pool: every borrow of the reused worker grows the region again, because
    # a borrow gives back the space it grew (see test_shared_memory_udf_pool_trims_grown_region).
    expected = "".join(f"{i}\n" for i in range(200))
    for _ in range(3):
        assert (
            node.query("SELECT test_function_shm_grow_pool_python(number) FROM numbers(200)")
            == expected
        )


def test_shared_memory_udf_pool_trims_grown_region(started_cluster):
    skip_test_msan(node)

    # A pooled region that one chunk had to grow must not stay that large while the worker waits in
    # the pool: nothing would ever shrink it again, and its memory is charged server-wide, where no
    # query is blamed for it. The region file is therefore back at the configured
    # shared_memory_size (4096) once the query is over, and the next borrow grows it again.
    assert shm_file_sizes("/shm_udf_trim") == []

    expected = "".join(f"{i}\n" for i in range(2000))
    for _ in range(3):
        assert (
            node.query(
                "SELECT test_function_shm_grow_trim_pool_python(number) FROM numbers(2000)"
            )
            == expected
        )
        assert shm_file_sizes("/shm_udf_trim") == [4096]


def test_shared_memory_udf_pipeline(started_cluster):
    skip_test_msan(node)

    # Pipelined transport (two regions + background prefetch thread). A single-block query first.
    assert node.query("SELECT test_function_shm_pipeline_python(1)") == "1\n"

    # Many rows, so the function is invoked several times. Each invocation is a separate source
    # with its own pair of regions and its own producer thread, and passes exactly one block, so
    # nothing is actually prefetched -- this exercises the pipelined transport's setup and teardown
    # across repeated calls. shm_udf_grow.py echoes the input, so the result equals toString(number).
    expected = node.query("SELECT toString(number) FROM numbers(200000)")
    assert (
        node.query("SELECT test_function_shm_pipeline_python(number) FROM numbers(200000)")
        == expected
    )


def test_shared_memory_udf_pipeline_pool(started_cluster):
    skip_test_msan(node)

    # Same, but through the pool: two regions per process, reused across borrows.
    # As above, each borrow carries a single block, so this covers region reuse rather than prefetch.
    expected = node.query("SELECT toString(number) FROM numbers(200000)")
    for _ in range(3):
        assert (
            node.query(
                "SELECT test_function_shm_pipeline_pool_python(number) FROM numbers(200000)"
            )
            == expected
        )


def test_shared_memory_udf_pipeline_grows(started_cluster):
    skip_test_msan(node)

    expected = "".join(f"{i}\n" for i in range(200))
    assert (
        node.query(
            "SELECT test_function_shm_pipeline_grow_python(number) "
            "FROM numbers(200) SETTINGS max_block_size=50"
        )
        == expected
    )


def test_shared_memory_udf_pipeline_pool_short_result_does_not_hang(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pipeline_pool_short_python(number) "
            "FROM numbers(3) SETTINGS max_block_size=3"
        )

    assert "wrong result, expected 3 row(s), actual 1" in str(exc.value)


def test_shared_memory_udf_does_not_fit(started_cluster):
    skip_test_msan(node)

    # The serialized input is larger than the whole region.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_small_python(number) FROM numbers(1000) FORMAT Null"
        )

    assert "does not fit into the shared-memory region" in str(exc.value)


def test_shared_memory_udf_result_does_not_fit(started_cluster):
    skip_test_msan(node)

    # The input fits, but the result does not fit after it, so the command asks for a larger
    # region. The region may not grow (`shared_memory_max_size` defaults to `shared_memory_size`),
    # so the server fails the query and names the setting to raise.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_tiny_python(1) FORMAT Null")

    assert "The region size requested by the command" in str(exc.value)
    assert "does not fit into the shared-memory region" in str(exc.value)
    assert "increase shared_memory_max_size" in str(exc.value)


def test_shared_memory_udf_command_reports_an_error(started_cluster):
    skip_test_msan(node)

    # The command answers through the protocol's error channel; the server surfaces the message.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_error_python(1) FORMAT Null")

    assert "reported an error" in str(exc.value)
    assert "the command cannot process this request" in str(exc.value)


def test_shared_memory_udf_pool_survives_an_error_response(started_cluster):
    skip_test_msan(node)

    # A command that answers with the protocol's error status has told the server it cannot process
    # this input and is back to waiting for the next request - a report, not a protocol violation.
    # The pooled worker must survive it, which shows as the same region file (a discarded worker
    # takes its region with it and the next borrow creates a new one under a new random name).
    assert shm_file_names("/shm_udf_error") == []

    regions = []
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_pool_error_python(1) FORMAT Null")
        assert "reported an error" in str(exc.value)
        regions.append(shm_file_names("/shm_udf_error"))

    assert len(regions[0]) == 1
    assert regions[0] == regions[1] == regions[2]


def test_shared_memory_udf_pool_command_died(started_cluster):
    skip_test_msan(node)

    # The pooled command exits without answering. Every such borrow has to fail quickly, drop the
    # dead worker together with its region, and give the pool slot back - so more failures than
    # `pool_size` (2 here) must not start timing out, and the pool must still be usable afterwards.
    assert shm_file_names("/shm_udf_die") == []

    for _ in range(5):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_pool_die_python(1) FORMAT Null")
        message = str(exc.value)
        assert "test_function_shm_pool_die_python" in message
        assert "Could not get process from pool" not in message
        assert shm_file_names("/shm_udf_die") == []

    assert node.query("SELECT test_function_shm_python(1)") == "Key 1\n"


def test_shared_memory_udf_invalid_offset(started_cluster):
    skip_test_msan(node)

    # The command reports success but points at an out-of-bounds region.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_bad_offset_python(1) FORMAT Null")

    assert "out-of-bounds region" in str(exc.value)


def test_shared_memory_udf_size_too_large(started_cluster):
    skip_test_msan(node)

    # shared_memory_size larger than the signed range (Int64 / off_t) is rejected at config load,
    # so the function is never created and the huge size never reaches the memory tracker or
    # ftruncate. The query therefore fails instead of charging a negative allocation.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_huge_python(1) FORMAT Null")

    # The function must not exist at all — otherwise any unrelated runtime error mentioning the same
    # name would keep this test green after the signed-range guard is gone.
    assert "test_function_shm_huge_python" in str(exc.value)
    assert "does not exist" in str(exc.value)
    assert node.contains_in_log(
        "Could not load external user defined function 'test_function_shm_huge_python'"
    )
    assert node.contains_in_log("`shared_memory_size` (18446744073709551615) must not exceed")


def test_shared_memory_udf_invalid_config_is_rejected(started_cluster):
    skip_test_msan(node)

    # Each of these functions has an invalid combination of shared-memory options and must be
    # rejected at config load, so the function is never created and using it fails. The rejection
    # is isolated (the other functions in the same config still work).
    for name, diagnostic in [
        ("test_function_shm_bad_chunk_header", "`use_shared_memory` is incompatible with `send_chunk_header`"),
        ("test_function_shm_bad_pipeline_no_shm", "`shared_memory_pipeline` requires `use_shared_memory`"),
        # The rejection is on the key being present, not on its value: writing a shared-memory knob
        # out at its own default says just as clearly that its author believed this function used
        # shared memory, and the function would have run over the pipes all the same.
        ("test_function_shm_bad_pipeline_default_no_shm", "`shared_memory_pipeline` requires `use_shared_memory`"),
        # Every shared-memory-only key has to be rejected the same way. Without `use_shared_memory`
        # they mean nothing, and accepting them would let a function that was explicitly configured
        # for shared memory run over the pipes instead, with nothing said about it at load time.
        ("test_function_shm_bad_size_no_shm", "`shared_memory_size` requires `use_shared_memory`"),
        ("test_function_shm_bad_path_no_shm", "`shared_memory_path` requires `use_shared_memory`"),
        ("test_function_shm_bad_max_lt_size", "`shared_memory_max_size` (524288) must not be smaller"),
        ("test_function_shm_bad_empty_path", "`shared_memory_path` must not be empty"),
        ("test_function_shm_bad_relative_path", "must be an absolute path"),
        ("test_function_shm_unsupported_path", "SharedMemoryRegion"),
    ]:
        # The function must not exist at all: a config the loader rejected leaves no function
        # behind, so this is UNKNOWN_FUNCTION rather than some runtime failure that happens to
        # mention the same name.
        with pytest.raises(Exception) as exc:
            node.query(f"SELECT {name}(1) FORMAT Null")
        assert name in str(exc.value)
        assert "does not exist" in str(exc.value)
        # ... and the loader said why, naming this function.
        assert node.contains_in_log(f"Could not load external user defined function '{name}'")
        assert node.contains_in_log(diagnostic)

    # A valid shared-memory UDF from the same config still works.
    assert node.query("SELECT test_function_shm_python(1)") == "Key 1\n"


def test_shared_memory_udf_pipeline_size_too_large(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_pipeline_huge_python(1) FORMAT Null")

    # As above: the function must be missing because the loader refused the config, not because
    # something else failed at run time. Here the guard being checked is the one on the *sum* of the
    # two regions the pipelined mode maps.
    assert "test_function_shm_pipeline_huge_python" in str(exc.value)
    assert "does not exist" in str(exc.value)
    assert node.contains_in_log(
        "Could not load external user defined function 'test_function_shm_pipeline_huge_python'"
    )
    assert node.contains_in_log("total shared-memory charge (2 regions of up to")


def test_shared_memory_udf_initial_region_reserves_backing_storage(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_initial_enospc_python(1) FORMAT Null")

    assert "Cannot reserve backing storage" in str(exc.value)
    assert "No space left on device" in str(exc.value)


def test_shared_memory_udf_failed_constructor_does_not_wait_for_the_command(started_cluster):
    skip_test_msan(node)

    # The same failure, timed: the region cannot be created, so the source fails before its stdin
    # write buffer exists. The command is already running and blocked reading its stdin, so unless
    # that descriptor is closed anyway, the query only ends once command_termination_timeout
    # (10 seconds by default) expires and the command is signalled.
    started_at = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_initial_enospc_python(1) FORMAT Null")
    elapsed = time.monotonic() - started_at

    assert "Cannot reserve backing storage" in str(exc.value)
    # Well below the 10 seconds the bug cost, and well above what a failing query needs on a loaded
    # CI machine, so the assertion catches the regression without being timing-sensitive.
    assert elapsed < 7


def test_shared_memory_udf_grow_reserves_backing_storage(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_grow_enospc_python(number) FROM numbers(200000) FORMAT Null "
            "SETTINGS max_block_size=200000"
        )

    assert "Cannot reserve backing storage" in str(exc.value)


def test_shared_memory_udf_pipeline_pool_failed_constructor_drops_partial_regions(started_cluster):
    skip_test_msan(node)

    assert tiny_shm_file_count() == 0

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pipeline_pool_partial_region_enospc_python(1) FORMAT Null"
        )

    assert "Cannot reserve backing storage" in str(exc.value)
    assert tiny_shm_file_count() == 0


def test_shared_memory_udf_command_shrinks_the_region(started_cluster):
    skip_test_msan(node)

    # The command truncates the region file and then answers with an offset the server still
    # believes to be inside it. Those pages are no longer backed by the file, so reading them would
    # raise SIGBUS and take the whole server down; the server must notice the resize instead.
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_shrink_python(1) FORMAT Null")
        assert "resized its shared-memory region" in str(exc.value)

    # The point of the test: the server is still there.
    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_command_shrinks_the_region_after_the_check(started_cluster):
    skip_test_msan(node)

    # The gap the region checks cannot close: the command truncates after the server has verified
    # the file is whole, and then answers with an offset and size the bounds check has no reason to
    # reject. The server therefore goes to read output the file no longer holds. Reading it through
    # the mapping is a `SIGBUS` - not an error, a dead server, taking every unrelated query with it -
    # which is why the response is copied out with `pread` instead. Then it is a short read, the one
    # query that caused it fails, and the assertion below that the server is still answering is the
    # whole point of this test.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_shrink_after_check_python(1) FORMAT Null")

    assert "ends after" in str(exc.value)

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_command_shrinks_the_region(started_cluster):
    skip_test_msan(node)

    # Same, through the pool: such a worker is discarded rather than handed to the next query, so it
    # takes its region with it and nothing is left behind in the shared-memory directory.
    assert shm_file_count("/shm_udf_shrink") == 0

    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_shrink_pool_python(1) FORMAT Null")
        assert "resized its shared-memory region" in str(exc.value)
        assert shm_file_count("/shm_udf_shrink") == 0

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_command_enlarges_the_region(started_cluster):
    skip_test_msan(node)

    # The command enlarges the region file and touches the new pages. Nothing faults and the answer
    # itself is valid, so this is only caught by comparing the file against the mapping — and it has
    # to be caught: those pages are committed in the tmpfs and charged to nobody, and trimming the
    # region on the way back to the pool would leave them behind.
    assert shm_file_names("/shm_udf_tamper") == []

    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_enlarge_pool_python(1) FORMAT Null")
        assert "resized its shared-memory region" in str(exc.value)
        # The worker is discarded, so it takes the enlarged file with it.
        assert shm_file_names("/shm_udf_tamper") == []

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_command_unlinks_the_region(started_cluster):
    skip_test_msan(node)

    # The command deletes the region file after mapping it. The server's descriptor keeps the file
    # alive, so the request itself could still succeed — but the path is gone, and reusing such a
    # worker would hand the next borrow a name that leads nowhere and poison the pool slot.
    assert shm_file_names("/shm_udf_tamper") == []

    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_unlink_pool_python(1) FORMAT Null")
        assert "removed or replaced its shared-memory file" in str(exc.value)
        assert shm_file_names("/shm_udf_tamper") == []

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_rechecks_region_during_cleanup(started_cluster):
    skip_test_msan(node)

    # The first response sends its status, waits for the normal response-side integrity check to
    # finish, then unlinks the region before completing the otherwise valid response. Cleanup must
    # re-check the region and discard this worker. The replacement sees the marker and behaves.
    node.exec_in_container(["rm", "-f", LATE_UNLINK_MARKER])
    assert node.query("SELECT test_function_shm_late_unlink_pool_python(1)") == "1\n"
    assert node.query("SELECT test_function_shm_late_unlink_pool_python(2)") == "2\n"

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pipeline_pool_trims_both_grown_regions(started_cluster):
    skip_test_msan(node)

    # Pooled and pipelined at once, with growth on top - the combination the other tests only cover
    # a piece of each. `max_block_size` is small enough that the query passes several blocks, so the
    # producer uses both regions and both have to grow past the configured 4096 bytes; both then
    # have to be trimmed back before the worker goes idle, because nothing would ever shrink them
    # again and their bytes are charged server-wide, where no query is blamed for them. A trim that
    # reached only the region the last chunk happened to use would leave the other one pinned here.
    assert shm_file_sizes("/shm_udf_pipeline_trim") == []

    expected = "".join(f"{i}\n" for i in range(8000))
    for _ in range(3):
        assert (
            node.query(
                "SELECT test_function_shm_pipeline_grow_trim_pool_python(number) "
                "FROM numbers(8000) SETTINGS max_block_size = 2000"
            )
            == expected
        )
        # Two regions, both back at the configured size: the pool holds exactly what it started with.
        assert shm_file_sizes("/shm_udf_pipeline_trim") == [4096, 4096]


def test_shared_memory_udf_pipeline_pool_idle_worker_is_charged_for_both_regions(started_cluster):
    skip_test_msan(node)

    # A pipelined pooled worker sits in the pool holding two regions, and the docs promise that both
    # of them count against `max_server_memory_usage` for as long as it does. Only the exact figure
    # separates two regions from one: a hand-over that gave back a single region's charge and
    # dropped the other leaves the files on disk untouched, so no size assertion can see it.
    total = 2 * PIPELINE_IDLE_REGION_SIZE

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pipeline_idle_charge_python")
    before = pooled_shared_memory_baseline("/shm_udf_pipeline_idle")

    assert node.query("SELECT test_function_shm_pipeline_idle_charge_python(1)") == "Key 1\n"
    assert shm_file_sizes("/shm_udf_pipeline_idle") == [PIPELINE_IDLE_REGION_SIZE] * 2

    wait_for_pooled_shared_memory_bytes(
        before + total,
        "the two regions of an idle pipelined pooled worker are not both charged to the server",
    )

    # Dropping the pool has to release both of them again.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pipeline_idle_charge_python")
    wait_for_pooled_shared_memory_bytes(
        before, "the charge for the two pipelined regions was not released with the pool"
    )
    assert shm_file_count("/shm_udf_pipeline_idle") == 0


def test_shared_memory_udf_pool_idle_worker_is_charged_server_wide(started_cluster):
    skip_test_msan(node)

    # A pooled region stays mapped between invocations, and in between there is no query to charge
    # for it: the borrow hands the charge over to the server, which is what makes those bytes count
    # against `max_server_memory_usage` while the worker just sits in the pool. The region-file
    # checks elsewhere in this suite say nothing about that hand-over - they would stay green if an
    # idle region were accounted to nobody at all.

    # Start from a pool that holds nothing: reloading the function drops any worker, and the region
    # that goes with it, that an earlier run left behind.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_idle_charge_python")
    before = pooled_shared_memory_baseline("/shm_udf_idle_charge")

    assert node.query("SELECT test_function_shm_idle_charge_python(1)") == "Key 1\n"
    assert shm_file_count("/shm_udf_idle_charge") == 1

    # The query is over and its own charge is gone with it, but its worker went back to the pool
    # with the region still mapped, so the region is charged to the server now - all of it, and
    # nothing besides it.
    wait_for_pooled_shared_memory_bytes(
        before + IDLE_CHARGE_REGION_SIZE,
        "the region of an idle pooled worker is not charged to the server",
    )

    # A second invocation borrows the same worker and the same region: the charge moves to that
    # query and back again, and has to come back as exactly what it was, not as twice it.
    assert node.query("SELECT test_function_shm_idle_charge_python(1)") == "Key 1\n"
    wait_for_pooled_shared_memory_bytes(
        before + IDLE_CHARGE_REGION_SIZE,
        "re-borrowing a pooled worker did not leave its region charged exactly once",
    )

    # Dropping the pool releases the worker, its region and the charge that came with it.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_idle_charge_python")
    wait_for_pooled_shared_memory_bytes(
        before, "the charge for the pooled region was not released with the pool"
    )
    assert shm_file_count("/shm_udf_idle_charge") == 0


def test_shared_memory_udf_pool_command_leaves_stdout_dirty(started_cluster):
    skip_test_msan(node)

    # The command answers every request correctly and then writes one byte more to its stdout. The
    # invocation that does it notices nothing - its answer was read in full, and the result comes
    # out of the shared-memory region - so the worker would go back to the pool with that byte
    # sitting in the pipe, and the *next* query to borrow it would read the byte as the status of a
    # response to its own request. The server must check that stdout is empty before it returns a
    # worker, so every one of these queries answers correctly on a worker of its own.
    discards_before = profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards")

    for _ in range(3):
        assert node.query("SELECT test_function_shm_chatty_pool_python(1)") == "Key 1\n"

    # Discarding a worker is otherwise invisible - the query succeeds and the next one just gets a
    # new process - so a command that quietly turns its pool into a process per call has to be
    # visible somewhere. It is counted, and said out loud.
    assert (
        profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards") == discards_before + 3
    )
    assert node.contains_in_log("left unread output on its stdout after answering")

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_stderr_written_on_the_way_out_still_throws(started_cluster):
    skip_test_msan(node)

    # `stderr_reaction` `throw` says anything the command writes to stderr fails the query. This one
    # answers correctly, closes its stdout, waits out the drain that follows - which stops as soon
    # as stderr goes quiet for a moment - and only then writes its line before exiting.
    #
    # Those bytes are found by the bounded wait that reaps the command, the last stretch in which a
    # command can write at all. Read and dropped there, the query would succeed and the setting
    # would quietly mean nothing on the way out.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_stderr_on_the_way_out_python(1) FORMAT Null")

    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "complaining on the way out" in str(exc.value), str(exc.value)

    # And with `check_exit_code` switched off. The two settings are independent: the reaction is
    # about what the command writes, the exit check is about how it ends, and only the wait that
    # reaps the command can observe output produced this late. Reaching that output only when the
    # exit status is also being checked would make `stderr_reaction` quietly conditional on an
    # unrelated setting.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_stderr_on_the_way_out_no_exit_check_python(1) FORMAT Null")

    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "complaining on the way out" in str(exc.value), str(exc.value)

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_unreadable_exit_code_fails_the_query(started_cluster):
    skip_test_msan(node)

    # The command answers correctly and then refuses to leave: it sleeps far past its
    # `command_termination_timeout` instead of exiting when its stdin is closed, and only much later
    # exits non-zero.
    #
    # `check_exit_code` is at its default, so the query has to fail. A status that could not be read
    # is not a passing status, and letting the query succeed on a log line would make the setting
    # mean "checked, unless the command avoids being checked" - which is exactly the command it is
    # there for. The query must also come back at the timeout rather than waiting for the command.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_lingers_python(1) FORMAT Null")
    elapsed = time.monotonic() - started

    assert "did not exit within command_termination_timeout" in str(exc.value), str(exc.value)
    assert elapsed < 60, f"the query took {elapsed:.1f}s to give up on the command"

    # And `check_exit_code = 0` is how such a command is configured: nothing is checked, so the same
    # command answers normally. This is the setting the message above points at, so it has to work.
    assert node.query("SELECT test_function_shm_lingers_no_exit_check_python(1)") == "Key 1\n"

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_command_closes_stderr_and_stalls(started_cluster):
    skip_test_msan(node)

    # A command that closes its own stderr and then stops answering. Closing stderr is legal and
    # ordinary, but from then on `poll` reports a hangup on that descriptor immediately and forever,
    # and reading it yields nothing. A server that leaves it in the set it waits on stops waiting
    # altogether: it spins, and `command_read_timeout` - which is measured by the poll it is no
    # longer doing - never fires, so a command that has merely gone quiet hangs the query for good.
    #
    # The function is configured with a two-second `command_read_timeout`, so the query has to come
    # back with that timeout rather than not at all.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_quiet_stderr_stalls_python(1) FORMAT Null")
    elapsed = time.monotonic() - started

    assert "Pipe read timeout exceeded" in str(exc.value), str(exc.value)
    # Generous, because the point is the difference between "times out" and "never returns", not the
    # precision of the timeout.
    assert elapsed < 60, f"the read timeout took {elapsed:.1f}s to fire"

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_command_writes_stderr_after_closing_stdout(started_cluster):
    skip_test_msan(node)

    # The command answers, closes its stdout, and only then writes megabytes to stderr - a summary
    # dumped on the way out. It is configured with `stderr_reaction` `none`.
    #
    # "None" says what to do with those bytes, not that the pipe may be left unread. Nothing reads
    # it once the answer has been taken, so the command blocks in `write` and never reaches its own
    # exit - and the wait that reaps it, which `check_exit_code` requires, reaps before it closes
    # anything. A blocking `waitpid` there waits for a process that is waiting for the server, and
    # the query hangs with its result already computed. What this pins down is that the wait keeps
    # draining while it waits and is bounded either way. (The other half of the same problem - the
    # stderr left on the pipe when the command's stdout reaches EOF while the server is still
    # reading it - is handled where that EOF is seen, and is not what this query exercises.)
    assert node.query("SELECT test_function_shm_stderr_after_stdout_python(1)") == "Key 1\n"

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_late_stderr_still_throws(started_cluster):
    skip_test_msan(node)

    # `stderr_reaction` `throw` says that anything the command writes to stderr fails the query.
    # This command writes its line after the response has been read, which is the one moment nothing
    # is reading that pipe - the bytes are found only when the worker is handed back and the server
    # notices it is not at a clean boundary. Reporting them as a log line and letting the query
    # succeed would leave the setting saying one thing and the server doing another, so they go
    # through the reaction like any other stderr output.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT DISTINCT test_function_shm_chatty_stderr_throw_pool_python(number) "
            "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000 FORMAT Null"
        )

    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "done" in str(exc.value), str(exc.value)

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_configuration_is_visible_in_system_table(started_cluster):
    skip_test_msan(node)

    # Whatever the transport is configured with has to be answerable from SQL: an operator looking
    # at `system.user_defined_functions` should be able to tell a shared-memory function from a pipe
    # one, and see how large its region may get, without reading the XML off the server.
    row = node.query(
        "SELECT use_shared_memory, shared_memory_size, shared_memory_max_size, shared_memory_pipeline, shared_memory_path "
        "FROM system.user_defined_functions WHERE name = 'test_function_shm_pipeline_pool_python'"
    ).strip()
    assert row == "1\t1048576\t1048576\t1\t/dev/shm", row

    # A region that is allowed to grow reports the bound it may grow to, not the raw `0` the
    # configuration uses to mean "it may not".
    row = node.query(
        "SELECT use_shared_memory, shared_memory_size, shared_memory_max_size, shared_memory_pipeline, shared_memory_path "
        "FROM system.user_defined_functions WHERE name = 'test_function_shm_grow_python'"
    ).strip()
    assert row == "1\t16\t1048576\t0\t/dev/shm", row

    # A function the loader refused has no configuration at all, so the columns are at their
    # defaults rather than showing something half-read out of a config that was never accepted.
    row = node.query(
        "SELECT load_status, use_shared_memory, shared_memory_size, shared_memory_max_size, "
        "shared_memory_pipeline, shared_memory_path "
        "FROM system.user_defined_functions WHERE name = 'test_function_shm_bad_size_no_shm'"
    ).strip()
    # The trailing empty `shared_memory_path` is what `strip` takes off the end.
    assert row == "Failed\t0\t0\t0\t0", row


def test_shared_memory_udf_command_talks_on_stderr_without_answering(started_cluster):
    skip_test_msan(node)

    # The command never writes to stdout and writes to stderr every 50 ms. Both descriptors are
    # polled - `stderr_reaction` `none` still has to take those bytes off the pipe - so each of
    # those writes wakes the read up. A read that restarts its `command_read_timeout` at every
    # wake-up is no longer bounded by anything the command does not control: this one produces
    # nothing the query can use and would hold it open for as long as it kept talking. The timeout
    # is one budget for the whole read, so the query has to come back with it.
    #
    # `nextImpl` also checks that budget itself once it is spent, rather than leaving it to the
    # poll: a zero-millisecond poll is a readiness probe, not a wait, and a probe is satisfied by
    # anything pending. That guard has no test of its own - reaching it needs a command that keeps
    # the pipe non-empty at every probe, and nothing written in a script can outrun a drain loop
    # that reads 4 KiB per iteration with nothing in between.
    started = time.monotonic()
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_chatty_stderr_stalls_python(1) FORMAT Null")
    elapsed = time.monotonic() - started

    assert "Pipe read timeout exceeded" in str(exc.value), str(exc.value)
    # Generous: the point is the difference between "times out" and "never returns", not precision.
    assert elapsed < 60, f"the read timeout took {elapsed:.1f}s to fire"

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_discarded_worker_still_reports_its_cpu(started_cluster):
    skip_test_msan(node)

    # The command burns CPU answering and then leaves a byte past its response frame, so the worker
    # is discarded rather than returned to the pool. That discard is the whole point: the borrow's
    # CPU and peak resident set are read out of `/proc/<pid>`, and everything the teardown does
    # takes that away - closing the child's stdin makes it exit, and a zombie has no `VmHWM`, while
    # the wait that follows reaps the pid outright. Sampling afterwards reports zeros for exactly
    # the borrows whose accounting matters most, and reports them as if the command had done no
    # work at all.
    discards_before = profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards")

    query_id = "shm-busy-chatty-1"
    node.query("SELECT test_function_shm_busy_chatty_pool_python(1) FORMAT Null", query_id=query_id)

    # The worker really was discarded - otherwise this measures the ordinary path instead.
    assert (
        profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards") == discards_before + 1
    )

    cpu = query_profile_event(query_id, "ExecutableUserDefinedFunctionUserTimeMicroseconds")
    # The command burns far more than this; the bound only has to be clear of the 10 ms tick that
    # `/proc/<pid>/stat` counts in, and clear of zero, which is what a reaped worker reports.
    assert cpu >= 20000, f"the discarded worker reported {cpu} us of user CPU"

    peak = query_profile_event(query_id, "ExecutableUserDefinedFunctionPeakMemoryByteSeconds")
    assert peak > 0, "the discarded worker reported no peak memory"


def test_shared_memory_udf_pool_command_floods_stdout(started_cluster):
    skip_test_msan(node)

    # The same accident as the test above, only past the point where it traps the server. This
    # command answers correctly and then writes megabytes past its response frame, filling the pipe
    # and blocking in `write`. Nothing reads that pipe any more, and closing the child's stdin - all
    # that discarding a worker does - does not release a process that is blocked writing rather than
    # reading. Reaping it with a plain blocking `waitpid` therefore never returns: the query hangs
    # with its result already computed, and `command_termination_timeout` does not apply to a wait
    # that has already begun.
    #
    # `check_exit_code` is left at its default `true`, which is what takes that path.
    discards_before = profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards")

    # No timeout is set on these deliberately: the regression is a hang, and the test harness
    # failing the whole run on its own timeout is exactly the signal.
    pids = [
        node.query("SELECT test_function_shm_flooding_stdout_pool_python(1)").strip()
        for _ in range(3)
    ]

    assert all(pid.isdigit() for pid in pids), pids
    assert len(set(pids)) == len(pids), f"a worker with unread stdout was reused: {pids}"
    assert (
        profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards") == discards_before + 3
    )
    assert node.contains_in_log("left unread output on its stdout after answering")

    # And the server is still able to run anything at all afterwards - a worker left blocked in
    # `write` would hold its pool slot and its region for as long as the server lives.
    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_pool_command_floods_stderr_under_reaction_none(started_cluster):
    skip_test_msan(node)

    # `stderr_reaction` `none` says what to do with the command's diagnostics - nothing - not that
    # the pipe may be left unread. This command writes megabytes to stderr *before* it answers, so a
    # server that stops polling stderr because it has no use for those bytes leaves the command
    # blocked in `write` with its answer unwritten, and the query dies of `command_read_timeout`
    # instead. The bytes have to be read off the pipe and dropped.
    #
    # A pooled worker makes the same point twice over: the leftovers would otherwise carry across
    # borrows until some later, entirely innocent query is the one that stalls. So the same worker
    # has to answer all of these, which its pid shows.
    pids = [
        node.query("SELECT test_function_shm_flooding_stderr_pool_python(1)").strip()
        for _ in range(3)
    ]

    assert all(pid.isdigit() for pid in pids), pids
    assert len(set(pids)) == 1, f"the worker was not reused: {pids}"


def test_shared_memory_udf_pool_discard_survives_being_decided_twice(started_cluster):
    skip_test_msan(node)

    # Same misbehaving command as above, but with `check_exit_code` off - which is what makes the
    # decision to discard get made twice. With it on, the discarded child is reaped, and a reaped
    # child is recognised on sight; with it off, nothing about the worker says what was decided
    # about it. And by then the evidence is gone: reporting the discard drained the leftover stderr,
    # and closing the child's stdin is what discarding means. A second look would find two clean
    # pipes and return a worker whose stdin is already closed, and the query after that would fail
    # writing its first request - so all three of these have to succeed, on a worker each.
    pids = [
        node.query(
            "SELECT DISTINCT test_function_shm_chatty_stderr_no_exit_check_pool_python(number) "
            "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000"
        ).strip()
        for _ in range(3)
    ]

    assert all(pid.isdigit() for pid in pids), pids
    assert len(set(pids)) == len(pids), f"a worker with unread stderr was reused: {pids}"


def test_shared_memory_udf_pool_command_may_close_its_stderr(started_cluster):
    skip_test_msan(node)

    # The other side of the check above. A command is entitled to close its own stderr, and once the
    # only writer is gone that pipe polls as a hangup for the rest of the process's life - forever
    # ready, with nothing to read. Read as "something is pending", that would discard this worker on
    # every borrow and turn the pool into a process per call, and every query would still succeed,
    # so nothing would say so. The same pid throughout is what says it did not happen.
    discards_before = profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards")

    pids = [
        node.query("SELECT test_function_shm_quiet_stderr_pool_python(1)").strip()
        for _ in range(3)
    ]

    assert all(pid.isdigit() for pid in pids), pids
    assert len(set(pids)) == 1, f"a worker was discarded for closing its own stderr: {pids}"
    assert (
        profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards") == discards_before
    )


def test_shared_memory_udf_pool_command_leaves_stderr_dirty(started_cluster):
    skip_test_msan(node)

    # The command answers correctly and then writes to stderr, after the server has already read the
    # response. Stderr is drained together with the response, so this line reaches nobody - until the
    # next borrow of the same worker reads it and reports it as that query's output (and fails that
    # query outright under `stderr_reaction` `throw`). So the worker must not go back to the pool
    # with it: each of these queries has to run on a worker of its own, which the answers - the
    # command's pid - make visible.
    #
    # The row count is what holds the two ends of this apart. The command waits 2ms after answering
    # before writing its line - long past the wake-up of the `poll` that was waiting for the
    # response, so the line cannot be drained into the query that earned it (see the script) - and
    # the single 500000-row block makes the server spend far longer than that parsing the answer out
    # of the region, so the line is there by the time the worker is offered back to the pool. One
    # block means one borrow, hence one pid per query.
    pids = [
        node.query(
            "SELECT DISTINCT test_function_shm_chatty_stderr_pool_python(number) "
            "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000"
        ).strip()
        for _ in range(3)
    ]

    assert all(pid.isdigit() for pid in pids), pids
    assert len(set(pids)) == len(pids), f"a worker with unread stderr was reused: {pids}"

    # And the discard is reported, with the output the command left behind: nothing else ever reads
    # a discarded worker's pipes, so this log line is the only place that line is going to surface.
    assert node.contains_in_log("left unread output on its stderr after answering")
    assert node.contains_in_log("Stderr: done")

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_command_died(started_cluster):
    skip_test_msan(node)

    # The command exits without answering; the server must fail the query rather than hang.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_die_python(1) FORMAT Null")

    assert "test_function_shm_die_python" in str(exc.value)

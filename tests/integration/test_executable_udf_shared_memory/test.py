import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[],
    tmpfs=[
        "/shm_udf_tiny:size=1M",
        "/shm_udf_accounting:size=1M",
        "/shm_udf_discard:size=1M",
        "/shm_udf_trim:size=4M",
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


def profile_event_value(event):
    return int(
        node.query(
            f"SELECT ifNull(sum(value), 0) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def shm_file_count(path):
    return int(
        node.exec_in_container(
            ["bash", "-c", f"find {path} -maxdepth 1 -name 'clickhouse_udf_shm_*' | wc -l"]
        ).strip()
    )


def tiny_shm_file_count():
    return shm_file_count("/shm_udf_tiny")


def discard_shm_file_count():
    return shm_file_count("/shm_udf_discard")


def shm_file_sizes(path):
    find = f"find {path} -maxdepth 1 -name 'clickhouse_udf_shm_*' -printf '%s\\n'"
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

    assert "Memory limit" in str(exc.value)


def test_shared_memory_udf_pool_failed_first_borrow_drops_created_region(started_cluster):
    skip_test_msan(node)

    assert shm_file_count("/shm_udf_accounting") == 0

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pool_accounting_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=524288, max_untracked_memory=0"
        )

    assert "Memory limit" in str(exc.value)
    assert shm_file_count("/shm_udf_accounting") == 0

    successful_query = (
        "SELECT test_function_shm_pool_accounting_python(1) "
        "SETTINGS max_memory_usage=10485760, max_untracked_memory=0"
    )
    worker_pid = node.query(successful_query).strip()
    assert worker_pid.isdigit()

    # The region now exists, so this borrow fails while charging it in the source constructor.
    # No request has reached the worker and the same process must remain reusable.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pool_accounting_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=524288, max_untracked_memory=0"
        )

    assert "Memory limit" in str(exc.value)
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

    assert "test_function_shm_huge_python" in str(exc.value)


def test_shared_memory_udf_invalid_config_is_rejected(started_cluster):
    skip_test_msan(node)

    # Each of these functions has an invalid combination of shared-memory options and must be
    # rejected at config load, so the function is never created and using it fails. The rejection
    # is isolated (the other functions in the same config still work).
    for name in [
        "test_function_shm_bad_chunk_header",       # use_shared_memory + send_chunk_header
        "test_function_shm_bad_pipeline_no_shm",     # shared_memory_pipeline without use_shared_memory
        "test_function_shm_bad_max_lt_size",         # shared_memory_max_size < shared_memory_size
        "test_function_shm_bad_empty_path",          # empty shared_memory_path
        "test_function_shm_bad_relative_path",       # relative shared_memory_path
        "test_function_shm_unsupported_path",        # filesystem without O_TMPFILE support
    ]:
        with pytest.raises(Exception) as exc:
            node.query(f"SELECT {name}(1) FORMAT Null")
        assert name in str(exc.value)

    # A valid shared-memory UDF from the same config still works.
    assert node.query("SELECT test_function_shm_python(1)") == "Key 1\n"


def test_shared_memory_udf_pipeline_size_too_large(started_cluster):
    skip_test_msan(node)

    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_pipeline_huge_python(1) FORMAT Null")

    assert "test_function_shm_pipeline_huge_python" in str(exc.value)


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
            "SELECT test_function_shm_grow_enospc_python(number) FROM numbers(200000) FORMAT Null"
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


def test_shared_memory_udf_command_died(started_cluster):
    skip_test_msan(node)

    # The command exits without answering; the server must fail the query rather than hang.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_die_python(1) FORMAT Null")

    assert "test_function_shm_die_python" in str(exc.value)

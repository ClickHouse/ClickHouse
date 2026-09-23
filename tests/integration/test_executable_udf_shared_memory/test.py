import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


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


def shm_region_committed_bytes():
    # `(size, committed)` per region: the file's length and how much of it is backed by pages
    # (`st_blocks`, in 512-byte units - for a `memfd`, exactly its resident pages). A region the
    # server reserved in full has the two equal; a hole punched into it shows as the difference.
    pid = node.get_process_pid("clickhouse server")
    assert pid is not None
    listing = node.exec_in_container(
        [
            "bash",
            "-c",
            f"for fd in /proc/{pid}/fd/*; do "
            f'if [ "$(readlink "$fd")" = "/memfd:clickhouse_udf_shm (deleted)" ]; '
            f'then stat -L -c "%s %b" "$fd"; fi; done',
        ]
    ).splitlines()
    return sorted((int(size), int(blocks) * 512) for size, blocks in (line.split() for line in listing if line))


def shm_region_count():
    return len(shm_regions())


def page_size():
    # The unit the server compares footprints and caps in, read where the server runs: the page
    # size of whatever kernel this is (a test that spelled 4096 would fail on 64 KiB pages) - or
    # the transparent huge page, where the kernel backs `shmem` with those regardless of file size
    # (`shmem_enabled` `always`/`force`) and `st_blocks` of any region reports at least one.
    base = int(node.exec_in_container(["getconf", "PAGESIZE"]).strip())
    mode = node.exec_in_container(
        ["bash", "-c", "cat /sys/kernel/mm/transparent_hugepage/shmem_enabled 2>/dev/null || true"]
    )
    if "[always]" in mode or "[force]" in mode:
        huge = node.exec_in_container(["bash", "-c", "cat /sys/kernel/mm/transparent_hugepage/hpage_pmd_size"])
        return max(base, int(huge.strip()))
    return base


def round_up_to_pages(size):
    page = page_size()
    return (size + page - 1) // page * page


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


# Where `shm_udf_stray_byte_after_probe.py` reports that it has written its stray byte.
STRAY_BYTE_MARKER = "/tmp/shm_udf_stray_byte_written"


def wait_for_container_file(path, timeout=30):
    # Waits for a marker a command drops to say it has reached a state nothing else can observe -
    # bytes written into a pipe the server holds, for instance. The command creates it after the
    # write it announces, so the file existing means the bytes are already there.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.exec_in_container(["bash", "-c", f"test -e {path} && echo yes || true"]).strip() == "yes":
            return
        time.sleep(0.05)
    raise AssertionError(f"the command did not create {path} within {timeout}s")


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


def test_shared_memory_udf_without_arguments_is_still_called(started_cluster):
    skip_test_msan(node)

    # A function without arguments has no input to serialize: its input block has no columns, so it
    # has no rows either and the input pipeline carries nothing at all. It is still a call - the
    # command is asked to produce a row - and the request that asks it carries an empty payload,
    # which is what the pipe transport does for the same function. A transport that took "no rows
    # to serialize" for "nothing to ask" would never call the command and fail the query for the
    # row it never produced.
    assert node.query("SELECT test_function_shm_zero_arg_python()") == "42\n"

    # The same pooled, where each request is a frame of its own - which is what makes a pooled
    # function without arguments work at all here: one worker answers all three calls, and the one
    # region it holds is the only thing this test leaves behind.
    regions_before = shm_region_count()
    for _ in range(3):
        assert node.query("SELECT test_function_shm_zero_arg_pool_python()") == "42\n"

    assert shm_region_count() == regions_before + 1


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
    # memory charges, so pooled reuse must not count the same region on every borrow. Counted in
    # whole pages, like the memory charge - and a page is the transparent huge page where the
    # kernel backs `shmem` with those, so the figure is not the configured size on every host.
    assert after - before == round_up_to_pages(1048576)


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

    regions_before = shm_region_count()

    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pool_accounting_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=524288, max_untracked_memory=0"
        )

    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value)
    assert shm_region_count() == regions_before

    successful_query = (
        "SELECT test_function_shm_pool_accounting_python(1) "
        "SETTINGS max_memory_usage=10485760, max_untracked_memory=0"
    )
    worker_pid = node.query(successful_query).strip()
    assert worker_pid.isdigit()
    regions_with_worker = set(shm_regions())
    assert len(regions_with_worker) == regions_before + 1

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
    # never touched it - together with the very same region: the worker inherited that region's
    # descriptor when it was started and answers into it, so a worker kept with its region dropped
    # would have the next borrow read its answer out of a fresh region the worker never writes to.
    assert set(shm_regions()) == regions_with_worker
    assert node.query(successful_query).strip() == worker_pid
    assert set(shm_regions()) == regions_with_worker


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

    # A discarded worker takes its region with it: nothing stays pinned on the pool slot for the
    # replacement, which inherits a region of its own when it is started.
    regions_before = shm_region_count()

    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query(
                "SELECT test_function_shm_pool_discard_over_python(number) "
                "FROM numbers(3) FORMAT Null"
            )
        assert "wrong result, expected 3 row(s)" in str(exc.value)
        assert shm_region_count() == regions_before


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

    # Same, but through the pool: the first borrow grows the region and the later ones reuse it at
    # its grown size (see test_shared_memory_udf_pool_keeps_grown_region).
    expected = "".join(f"{i}\n" for i in range(200))
    for _ in range(3):
        assert (
            node.query("SELECT test_function_shm_grow_pool_python(number) FROM numbers(200)")
            == expected
        )


def test_shared_memory_udf_pool_keeps_grown_region(started_cluster):
    skip_test_msan(node)

    # A pooled region that one chunk had to grow stays that large for as long as its worker lives:
    # the region is sealed against shrinking, so there is nothing to trim it back with, and the
    # next borrow finds it already big enough. `shared_memory_max_size` is therefore what a pooled
    # worker may hold, and the idle charge (see the idle-worker tests) is the grown size.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_grow_keep_pool_python")
    sizes_before = shm_region_sizes()
    growths_before = profile_event_value("ExecutableUDFSharedMemoryRegionGrowths")

    expected = "".join(f"{i}\n" for i in range(2000))
    assert (
        node.query("SELECT test_function_shm_grow_keep_pool_python(number) FROM numbers(2000)")
        == expected
    )
    growths_after_first = profile_event_value("ExecutableUDFSharedMemoryRegionGrowths")
    assert growths_after_first > growths_before

    sizes_after = shm_region_sizes()
    # Exactly one region appeared, and it is larger than the configured 4096 bytes.
    assert len(sizes_after) == len(sizes_before) + 1
    grown = sorted(set(sizes_after) - set(sizes_before)) or [
        size for size in sizes_after if sizes_after.count(size) > sizes_before.count(size)
    ]
    assert grown and grown[0] > 4096, (sizes_before, sizes_after)

    # The later borrows find the region at its grown size and never grow it again.
    for _ in range(2):
        assert (
            node.query("SELECT test_function_shm_grow_keep_pool_python(number) FROM numbers(2000)")
            == expected
        )
        assert shm_region_sizes() == sizes_after
    assert profile_event_value("ExecutableUDFSharedMemoryRegionGrowths") == growths_after_first


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
    # The pooled worker must survive it, which shows as the same region (a discarded worker takes
    # its region with it and the next borrow creates a new one, with a new inode).
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pool_error_python")
    regions_before = set(shm_regions())

    regions = []
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_pool_error_python(1) FORMAT Null")
        assert "reported an error" in str(exc.value)
        regions.append(set(shm_regions()) - regions_before)

    assert len(regions[0]) == 1
    assert regions[0] == regions[1] == regions[2]


def test_shared_memory_udf_pool_command_died(started_cluster):
    skip_test_msan(node)

    # The pooled command exits without answering. Every such borrow has to fail quickly, drop the
    # dead worker together with its region, and give the pool slot back - so more failures than
    # `pool_size` (2 here) must not start timing out, and the pool must still be usable afterwards.
    regions_before = shm_region_count()

    for _ in range(5):
        with pytest.raises(Exception) as exc:
            node.query("SELECT test_function_shm_pool_die_python(1) FORMAT Null")
        message = str(exc.value)
        assert "test_function_shm_pool_die_python" in message
        assert "Could not get process from pool" not in message
        assert shm_region_count() == regions_before

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
        ("test_function_shm_bad_max_size_no_shm", "`shared_memory_max_size` requires `use_shared_memory`"),
        ("test_function_shm_bad_max_lt_size", "`shared_memory_max_size` (524288) must not be smaller"),
        # The size is the one thing the transport cannot default: a missing one and an explicit
        # zero are both a region of nothing, and both are refused the same way.
        ("test_function_shm_bad_no_size", "`shared_memory_size` must be greater than zero"),
        ("test_function_shm_bad_zero_size", "`shared_memory_size` must be greater than zero"),
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


def test_shared_memory_udf_size_of_int64_max_is_too_large_in_pages(started_cluster):
    skip_test_msan(node)

    # The largest size the signed range admits - and a file holds whole pages, so what gets charged
    # for it is the next page boundary, which is one past the signed range: the tracker would be
    # handed a negative allocation. The loader measures the charge as it will be made, in pages.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_int64_max_python(1) FORMAT Null")

    assert "test_function_shm_int64_max_python" in str(exc.value)
    assert "does not exist" in str(exc.value)
    assert node.contains_in_log(
        "Could not load external user defined function 'test_function_shm_int64_max_python'"
    )
    assert node.contains_in_log(
        "total shared-memory charge (1 regions of up to 9223372036854775808 bytes, rounded up to whole pages) "
        "must not exceed 9223372036854775807"
    )


def test_shared_memory_udf_pipeline_pool_failed_constructor_drops_partial_regions(started_cluster):
    skip_test_msan(node)

    # Two regions of 768 KiB each, and a limit of 1 MiB: the first region is charged and created,
    # the second one is refused by the memory limit. The region that was created must not survive
    # the failed borrow on the pool slot - the next borrow starts a process of its own and would
    # not be able to use it anyway.
    regions_before = shm_region_count()

    query_id = "shm_pipeline_pool_partial_region"
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_pipeline_pool_partial_region_python(1) FORMAT Null "
            "SETTINGS max_memory_usage=1048576, max_untracked_memory=0",
            query_id=query_id,
        )

    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value)
    # The first region was indeed created before the second one was refused, so this is the
    # partial case and not a borrow that failed before it did anything.
    assert query_profile_event(
        query_id, "ExecutableUDFSharedMemoryAllocatedBytes"
    ) == round_up_to_pages(786432)
    assert shm_region_count() == regions_before


def test_shared_memory_udf_pipeline_pool_keeps_both_grown_regions(started_cluster):
    skip_test_msan(node)

    # Pooled and pipelined at once, with growth on top - the combination the other tests only cover
    # a piece of each. An executable function passes one block per call, so only the region the
    # first block lands in ever grows; the second one stays at the configured 4096 bytes. What has
    # to hold is that a grown region stays grown with its worker, that the second region is neither
    # lost nor grown for nothing, and that the later borrows use both as they are.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pipeline_grow_keep_pool_python")
    sizes_before = shm_region_sizes()
    growths_before = profile_event_value("ExecutableUDFSharedMemoryRegionGrowths")
    # The reload above dropped this function's pool; whatever is charged now belongs to others.
    pooled_before = pooled_shared_memory_bytes()

    expected = "".join(f"{i}\n" for i in range(8000))
    assert (
        node.query(
            "SELECT test_function_shm_pipeline_grow_keep_pool_python(number) "
            "FROM numbers(8000) SETTINGS max_block_size = 2000"
        )
        == expected
    )
    growths_after_first = profile_event_value("ExecutableUDFSharedMemoryRegionGrowths")
    assert growths_after_first > growths_before

    sizes_after = shm_region_sizes()
    assert len(sizes_after) == len(sizes_before) + 2
    added = list(sizes_after)
    for size in sizes_before:
        added.remove(size)
    assert sorted(added)[0] == 4096 and sorted(added)[1] > 4096, (sizes_before, sizes_after)

    # The worker is idle now, and the server is charged for what it actually holds: the grown
    # region at its grown size plus the other one at the configured size - not two base regions
    # (the charge would have to be the size the borrow asked for) and not two grown ones (a growth
    # of one region must not be counted against both).
    # In whole pages, which is what the charge is made of: the regions here are counted by the
    # length of their files, and the base one is smaller than a page on any kernel with pages
    # larger than it - or where `shmem` is backed with huge pages.
    idle_charge = sum(round_up_to_pages(size) for size in added)
    wait_for_pooled_shared_memory_bytes(
        pooled_before + idle_charge,
        "an idle pipelined worker is not charged for one grown and one base-sized region",
    )

    for _ in range(2):
        assert (
            node.query(
                "SELECT test_function_shm_pipeline_grow_keep_pool_python(number) "
                "FROM numbers(8000) SETTINGS max_block_size = 2000"
            )
            == expected
        )
        assert shm_region_sizes() == sizes_after
        # Borrowed and handed back again: the charge comes back as exactly what it was.
        wait_for_pooled_shared_memory_bytes(
            pooled_before + idle_charge, "re-borrowing the pipelined worker changed its idle charge"
        )
    assert profile_event_value("ExecutableUDFSharedMemoryRegionGrowths") == growths_after_first

    # Dropping the pool releases both regions and the whole charge.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pipeline_grow_keep_pool_python")
    wait_for_pooled_shared_memory_bytes(
        pooled_before, "the charge for the grown pipelined regions was not released with the pool"
    )


def test_shared_memory_udf_pipeline_pool_idle_worker_is_charged_for_both_regions(started_cluster):
    skip_test_msan(node)

    # A pipelined pooled worker sits in the pool holding two regions, and the docs promise that both
    # of them count against `max_server_memory_usage` for as long as it does. Only the exact figure
    # separates two regions from one: a hand-over that gave back a single region's charge and
    # dropped the other leaves the regions themselves untouched, so no size assertion can see it.
    total = 2 * PIPELINE_IDLE_REGION_SIZE

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pipeline_idle_charge_python")
    before = pooled_shared_memory_baseline(PIPELINE_IDLE_REGION_SIZE)

    assert node.query("SELECT test_function_shm_pipeline_idle_charge_python(1)") == "Key 1\n"
    assert shm_region_sizes().count(PIPELINE_IDLE_REGION_SIZE) == 2

    wait_for_pooled_shared_memory_bytes(
        before + total,
        "the two regions of an idle pipelined pooled worker are not both charged to the server",
    )

    # Dropping the pool has to release both of them again.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_pipeline_idle_charge_python")
    wait_for_pooled_shared_memory_bytes(
        before, "the charge for the two pipelined regions was not released with the pool"
    )
    assert PIPELINE_IDLE_REGION_SIZE not in shm_region_sizes()


def test_shared_memory_udf_pool_idle_worker_is_charged_server_wide(started_cluster):
    skip_test_msan(node)

    # A pooled region stays mapped between invocations, and in between there is no query to charge
    # for it: the borrow hands the charge over to the server, which is what makes those bytes count
    # against `max_server_memory_usage` while the worker just sits in the pool. The region checks
    # elsewhere in this suite say nothing about that hand-over - they would stay green if an idle
    # region were accounted to nobody at all.

    # Start from a pool that holds nothing: reloading the function drops any worker, and the region
    # that goes with it, that an earlier run left behind.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_idle_charge_python")
    before = pooled_shared_memory_baseline(IDLE_CHARGE_REGION_SIZE)

    assert node.query("SELECT test_function_shm_idle_charge_python(1)") == "Key 1\n"
    assert shm_region_sizes().count(IDLE_CHARGE_REGION_SIZE) == 1

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
    assert IDLE_CHARGE_REGION_SIZE not in shm_region_sizes()


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
    skip_test_msan(node)

    # The pooled-bytes metric elsewhere in this suite is the idle charge told apart from everything
    # else; this is the charge doing what a charge is for. An idle pooled worker holds a region
    # nobody is querying through, and the documentation promises it counts against
    # `max_server_memory_usage`. Two things have to be true for that, and both are checked here:
    # the server-wide tracker's amount goes up by the region while the worker is idle, and an
    # allocation that would fit into the server's headroom is refused while it does.
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
        # region, once, give or take what the server allocates and frees on its own meanwhile.
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


def test_shared_memory_udf_command_extending_the_region_is_charged_at_the_next_hand_over(started_cluster):
    skip_test_msan(node)

    # The seals stop a command from shrinking its region, not from extending it, and a command that
    # does holds pages the server did not ask for. They are still the server's cost, so the region's
    # file is measured again wherever its charge changes hands: when the worker goes back to the
    # pool - the idle charge is then the extended size - and when it is borrowed again - the query
    # is then charged the extended size, and a query that cannot afford it is refused before it
    # touches the worker. The command doubles the file on every request, so each borrow sees a
    # larger file than the one before.
    region_size = 4 * 1048576

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_extend_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_extend_pool_python(1)") == "Key 1\n"
    wait_for_pooled_shared_memory_bytes(
        pooled_before + 2 * region_size,
        "the idle charge does not cover the pages the command added to its region",
    )
    assert 2 * region_size in shm_region_sizes()

    # The next borrow is charged the extended size - so under a limit that would have fitted the
    # configured region it is refused, before anything reaches the worker.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT test_function_shm_extend_pool_python(2) FORMAT Null "
            f"SETTINGS max_memory_usage = {region_size + region_size // 2}, max_untracked_memory = 0"
        )
    assert "MEMORY_LIMIT_EXCEEDED" in str(exc.value), str(exc.value)
    wait_for_pooled_shared_memory_bytes(
        pooled_before + 2 * region_size, "a refused borrow changed the idle charge"
    )

    # A borrow that can afford it goes through, the command doubles the file again, and the idle
    # charge follows.
    assert node.query("SELECT test_function_shm_extend_pool_python(3)") == "Key 3\n"
    wait_for_pooled_shared_memory_bytes(
        pooled_before + 4 * region_size, "the idle charge did not follow the second extension"
    )

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_extend_pool_python")
    wait_for_pooled_shared_memory_bytes(pooled_before, "the extended region's charge was not released with the pool")


def test_shared_memory_udf_command_extending_the_region_past_the_cap_costs_it_the_worker(started_cluster):
    skip_test_msan(node)

    # `shared_memory_max_size` is what a pooled worker may hold, and what an administrator sizes a
    # pool by. The server's own growth stops there; a command's does not - the seals forbid
    # shrinking the file, not extending it - so a worker whose file has passed the cap is not
    # handed back to the pool: it is discarded with its regions, the next query starts a fresh
    # one, and the server is never charged for, never maps and never zeroes a file the command
    # stretched past what was configured. Here the cap is the region size (the default), and the
    # command doubles the file on every request.
    region_size = 4 * 1048576

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_extend_past_cap_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_extend_past_cap_pool_python(1)") == "Key 1\n"

    # The extended region is gone with its worker: nothing of it is charged to the server while
    # the slot sits in the pool, and the server holds no file of the extended size.
    wait_for_pooled_shared_memory_bytes(pooled_before, "the region extended past the cap stayed with the pool")
    assert 2 * region_size not in shm_region_sizes()
    assert node.contains_in_log("past shared_memory_max_size")

    # The next query is served by a fresh worker with a fresh region - which the command extends
    # again, with the same outcome.
    assert node.query("SELECT test_function_shm_extend_past_cap_pool_python(2)") == "Key 2\n"
    wait_for_pooled_shared_memory_bytes(pooled_before, "the region extended past the cap stayed with the pool")


def test_shared_memory_udf_pages_committed_past_the_end_of_the_file_count_against_the_cap(started_cluster):
    skip_test_msan(node)

    # The cap is on what a region holds, not on how long its file is. `fallocate` with
    # `FALLOC_FL_KEEP_SIZE` past the end of the file commits pages without moving the end - the seals
    # allow it - so a server that judged its regions by their length alone would let a command park
    # any amount of memory in a pooled region, unseen by every check and every charge. The region's
    # footprint is the larger of its length and its committed pages, and it is that which is
    # measured against `shared_memory_max_size` where the worker is handed back: the command here
    # commits twice the file's length past its end, so the worker is discarded with the region
    # and nothing of it stays charged - or held - in the pool. The region size is one no other
    # function in this file uses.
    region_size = 1572864

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_alloc_beyond_eof_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_alloc_beyond_eof_pool_python(1)") == "Key 1\n"

    wait_for_pooled_shared_memory_bytes(pooled_before, "a region with pages committed past its end stayed with the pool")
    assert region_size not in shm_region_sizes()
    assert node.contains_in_log("(its length, the pages it committed, or what it would hold once mapped whole), past shared_memory_max_size")

    # A fresh worker serves the next query, with the same outcome.
    assert node.query("SELECT test_function_shm_alloc_beyond_eof_pool_python(2)") == "Key 2\n"
    wait_for_pooled_shared_memory_bytes(pooled_before, "a region with pages committed past its end stayed with the pool")


def test_shared_memory_udf_region_smaller_than_a_page_is_not_over_its_own_cap(started_cluster):
    skip_test_msan(node)

    # A region of 24 bytes holds a page, because a file holds whole pages, and its cap defaults to
    # its size - 24 bytes. Measured in bytes, the region would be over its cap from the moment it
    # is created, and the pooled worker would be thrown away after every call for a configuration
    # that is perfectly valid. Footprints and caps are compared in whole pages, so the worker
    # stays: the same region (same inode) serves every call. The size is one no other function in
    # this file uses, so the region can be told apart by it.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_tiny_region_pool_python")

    assert node.query("SELECT test_function_shm_tiny_region_pool_python(1)") == "Key 1\n"
    regions_after_first = [inode for inode, size in shm_regions() if size == 24]
    assert len(regions_after_first) == 1, shm_regions()

    assert node.query("SELECT test_function_shm_tiny_region_pool_python(2)") == "Key 2\n"
    assert node.query("SELECT test_function_shm_tiny_region_pool_python(3)") == "Key 3\n"
    regions_after_third = [inode for inode, size in shm_regions() if size == 24]
    assert regions_after_third == regions_after_first, (regions_after_first, regions_after_third)


def test_shared_memory_udf_pages_committed_within_the_cap_are_charged_once(started_cluster):
    skip_test_msan(node)

    # The command commits three pages past the end of its 40-byte file - well within a cap of
    # 1.75 MiB - and the next borrow charges the query for them, as it should. A growth that then
    # reaches into those pages commits nothing new and must not charge them a second time. The
    # pool keeps the same worker and region throughout (same inode), the region grows into the
    # pages the command committed (200 rows of input into a 40-byte region), and the idle charge
    # afterwards is the region's footprint, counted once. The size is one no other function in
    # this file uses.
    region_size = 40
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_alloc_beyond_eof_within_cap_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_alloc_beyond_eof_within_cap_pool_python(1)") == "Key 1\n"
    regions_after_first = [inode for inode, size in shm_regions() if size == region_size]
    assert len(regions_after_first) == 1, shm_regions()
    committed = dict(shm_region_committed_bytes())
    assert committed.get(region_size, 0) >= 3 * page_size(), committed
    wait_for_pooled_shared_memory_bytes(pooled_before + committed[region_size], "the idle charge is not the region's footprint")

    # `sum(length(...))` rather than `count()`: a call whose result nothing needs is optimized out.
    assert node.query(
        "SELECT sum(length(test_function_shm_alloc_beyond_eof_within_cap_pool_python(number))) FROM numbers(200)"
    ) == "1290\n"
    # Same worker: it was within the cap, and the same region grew.
    grown = [(inode, size) for inode, size in shm_regions() if inode in regions_after_first]
    assert len(grown) == 1 and grown[0][1] > region_size, shm_regions()
    # The idle charge is the footprint once: the grown length rounded to pages, or the pages the
    # command committed on top of it - never both added together.
    committed = dict(shm_region_committed_bytes())
    footprint = max(round_up_to_pages(grown[0][1]), committed[grown[0][1]])
    wait_for_pooled_shared_memory_bytes(pooled_before + footprint, "pages the command committed were charged twice")


def test_shared_memory_udf_growth_on_top_of_pages_committed_far_past_the_end_is_charged(started_cluster):
    skip_test_msan(node)

    # The command commits three pages a megabyte past the end of its 56-byte file - within the cap
    # of 1.75 MiB, so the worker is kept and the next borrow charges the query for a footprint of
    # four pages. A growth to a few dozen KiB stops well short of those three pages: it commits
    # its own pages on top of them, and the footprint says how many pages the file holds, not
    # where. A charge that took the three pages for pages of the growth would leave the query
    # charged three pages less than the region grew by; the footprint is re-read after the growth
    # and the query is charged for exactly what it committed - `ExecutableUDFSharedMemoryAllocatedBytes`
    # counts the charge, so it must equal the growth of the pages the file holds. The size is one
    # no other function in this file uses.
    region_size = 56
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_alloc_far_beyond_eof_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_alloc_far_beyond_eof_pool_python(1)") == "Key 1\n"
    regions_after_first = [inode for inode, size in shm_regions() if size == region_size]
    assert len(regions_after_first) == 1, shm_regions()
    committed_before = dict(shm_region_committed_bytes())[region_size]
    assert committed_before == round_up_to_pages(region_size) + 3 * page_size(), committed_before
    wait_for_pooled_shared_memory_bytes(pooled_before + committed_before, "the idle charge is not the region's footprint")

    # `sum(length(...))` rather than `count()`: a call whose result nothing needs is optimized out.
    query_id = "shm_growth_on_top_of_far_pages"
    assert node.query(
        "SELECT sum(length(test_function_shm_alloc_far_beyond_eof_pool_python(number))) FROM numbers(5000)",
        query_id=query_id,
    ) == "38890\n"
    # Same worker, same region, grown - but not as far as the three pages.
    grown = [(inode, size) for inode, size in shm_regions() if inode in regions_after_first]
    assert len(grown) == 1 and region_size < grown[0][1] < 1048576, shm_regions()
    committed_after = dict(shm_region_committed_bytes())[grown[0][1]]
    assert committed_after == round_up_to_pages(grown[0][1]) + 3 * page_size(), (grown, committed_after)
    assert query_profile_event(query_id, "ExecutableUDFSharedMemoryAllocatedBytes") == committed_after - committed_before
    wait_for_pooled_shared_memory_bytes(pooled_before + committed_after, "the idle charge is not the region's footprint")


def test_shared_memory_udf_growth_that_would_take_the_footprint_past_the_cap_is_refused_before_it_commits(started_cluster):
    skip_test_msan(node)

    # The command commits the last 256 KiB below a cap of 1.75 MiB (whole pages of it), far past
    # the end of its 72-byte file. A growth that reaches into them would commit everything between the end of
    # the file and them on top of them and take the footprint past the cap - and the server's own
    # growth is what the cap is a promise about. So the growth is refused before it commits
    # anything, by the bound on what it could commit at most, and the worker that filled the
    # region with pages of its own is discarded: the next chunk would fail the same way. The error
    # is this one and not "does not fit" from the command asking for room for its result, which is
    # where a server that grew first and measured later would have failed. The size is one no
    # other function in this file uses.
    region_size = 72
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_alloc_far_beyond_eof_filling_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_alloc_far_beyond_eof_filling_pool_python('1')") == "Key 1\n"
    regions_after_first = [inode for inode, size in shm_regions() if size == region_size]
    assert len(regions_after_first) == 1, shm_regions()
    committed_before = dict(shm_region_committed_bytes())[region_size]
    assert committed_before == round_up_to_pages(region_size) + 262144 // page_size() * page_size(), committed_before

    # One block of 65536 rows of about 11 bytes (one thread, or `numbers` splits it): some 700 KB
    # of input, which grows the region to 1.125 MiB, and then a result that needs about 1.6 MB of
    # region - the growth that reaches into the command's pages.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT sum(length(test_function_shm_alloc_far_beyond_eof_filling_pool_python(concat(toString(number), 'xxxxx')))) "
            "FROM numbers(65536) SETTINGS max_threads = 1, max_block_size = 65536"
        )
    assert "would take its footprint from" in str(exc.value), str(exc.value)
    assert "past shared_memory_max_size" in str(exc.value), str(exc.value)

    # The worker and its region are gone, and nothing of them stays charged to the server.
    wait_for_pooled_shared_memory_bytes(pooled_before, "the discarded worker's region stayed charged")
    assert not [inode for inode, _ in shm_regions() if inode in regions_after_first], shm_regions()
    # A fresh worker serves the next query.
    assert node.query("SELECT test_function_shm_alloc_far_beyond_eof_filling_pool_python('2')") == "Key 2\n"


def test_shared_memory_udf_pages_committed_during_the_request_are_seen_by_the_growth_it_asks_for(started_cluster):
    skip_test_msan(node)

    # The command has the region to itself while it serves a request, and it can commit pages
    # past the end of the file then and there - and then ask for a larger region. The growth the
    # server makes for it is bounded and checked against the footprint as it is at that moment,
    # not as it was when the worker was borrowed: here 500 KB of pages a megabyte past the end,
    # committed during the request, and a request for a region the doubling takes to the cap of
    # 1 MiB. A server that grew by the footprint it knew from the borrow would find the region
    # at 1.5 MiB afterwards; this one refuses the growth before it commits a page, and the worker
    # goes.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_alloc_far_during_request_pool_python")
    regions_before = shm_regions()

    # 300 KB of input grow the region to 512 KiB before the request; the result needs 600 KB more
    # than that, and the doubling asks for the cap.
    with pytest.raises(Exception) as exc:
        node.query("SELECT length(test_function_shm_alloc_far_during_request_pool_python(repeat('x', 300000)))")
    assert "The region size requested by the command" in str(exc.value), str(exc.value)
    assert "would take its footprint from" in str(exc.value), str(exc.value)
    assert "past shared_memory_max_size (1048576 bytes)" in str(exc.value), str(exc.value)

    # Nothing of the worker stays: no region of the doubled size, none over the cap.
    assert not set(shm_regions()) - set(regions_before), (regions_before, shm_regions())


def test_shared_memory_udf_sparse_length_and_pages_past_the_end_are_not_filled_in_by_the_server(started_cluster):
    skip_test_msan(node)

    # The footprint says how many pages a file holds, not where. The command stretches its 88-byte
    # file to the cap of 2 MiB without committing a page, and commits just under 2 MiB of pages
    # past the end (whole pages of 2 MiB - 4 KiB, so under it on every page size): by its length
    # the file is at the cap, by its pages it is under it, and a
    # server that judged by the larger of the two would find it within the cap, keep the worker,
    # map the file whole at the next borrow - committing the 2 MiB under its length on top of the
    # 2 MiB past it - and hold twice the cap while charging the query for half of that. What
    # mapping the file whole would cost is counted against the cap where the worker changes
    # hands, so the worker is discarded at the hand-back, before the server commits anything:
    # nothing of the stretched region stays in the pool, and the next query is served by a fresh
    # worker. The size is one no other function in this file uses.
    region_size = 88
    cap = 2097152
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_sparse_to_the_cap_and_pages_past_it_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    assert node.query("SELECT test_function_shm_sparse_to_the_cap_and_pages_past_it_pool_python(1)") == "Key 1\n"
    wait_for_pooled_shared_memory_bytes(pooled_before, "the stretched region stayed with the pool")
    assert cap not in shm_region_sizes(), shm_regions()
    # The figure reported is what the file would hold once mapped whole - the cap's worth of
    # length on top of the pages past the end - not its length or its pages, both of which are
    # within the cap.
    far_pages_bytes = 2093056 // page_size() * page_size()
    assert node.contains_in_log(
        f"grown its shared-memory region to {cap + far_pages_bytes} bytes (its length, the pages it committed, "
        "or what it would hold once mapped whole), past shared_memory_max_size (2097152 bytes); "
        "the process will not be reused"
    )

    # A fresh worker serves the next query, with the same outcome.
    assert node.query("SELECT test_function_shm_sparse_to_the_cap_and_pages_past_it_pool_python(2)") == "Key 2\n"
    wait_for_pooled_shared_memory_bytes(pooled_before, "the stretched region stayed with the pool")
    assert cap not in shm_region_sizes(), shm_regions()


def test_shared_memory_udf_tiny_cap_is_in_bytes_for_the_length_of_the_file(started_cluster):
    skip_test_msan(node)

    # A region of 28 bytes with the cap defaulting to its size. Footprints are compared with the
    # cap in whole pages - the file holds a page either way - but the length of the file is exact
    # and the command's to change, and it is held to the exact cap: a command that stretches the
    # 28-byte file to a page has stretched it past 28 bytes, and the worker goes, as it would for
    # a file stretched to a terabyte. The size is one no other function in this file uses.
    region_size = 28
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_extend_tiny_pool_python")
    pooled_before = pooled_shared_memory_baseline(region_size)

    regions_before = shm_regions()
    assert node.query("SELECT test_function_shm_extend_tiny_pool_python(1)") == "Key 1\n"
    wait_for_pooled_shared_memory_bytes(pooled_before, "the region stretched past its cap stayed with the pool")
    # Nothing new is held: the region (a page long by now) went with its worker.
    assert not set(shm_regions()) - set(regions_before), (regions_before, shm_regions())
    assert node.contains_in_log("past shared_memory_max_size (28 bytes)")


def test_shared_memory_udf_hole_punched_by_the_command_is_not_fatal(started_cluster):
    skip_test_msan(node)

    # The seals stop a command from shrinking its region's file; they do not stop it from freeing
    # pages inside it (`fallocate(FALLOC_FL_PUNCH_HOLE)` - only `F_SEAL_WRITE` would, and the
    # command has to write). That is not a `SIGBUS` for the server: the file is as long as it was,
    # a punched page reads as zeros and takes a write like any other. What is lost is the
    # reservation - the server's next write into the hole allocates the page on its hot path -
    # and that is the command's own slowness, not a failure. The command here answers and then
    # frees everything past its answer; the next borrow, same worker, same region, must work.
    # The region size is one no other function in this file uses, so the region can be told
    # apart by it.
    region_size = 1310720

    node.query("SYSTEM RELOAD FUNCTION test_function_shm_punch_hole_pool_python")

    assert node.query("SELECT test_function_shm_punch_hole_pool_python(1)") == "Key 1\n"

    # The hole is there: the file is as long as it was, most of it no longer backed by pages.
    committed = dict(shm_region_committed_bytes())
    assert region_size in committed, committed
    assert committed[region_size] < region_size // 2, committed

    # The same region serves the next query - the server writes the input into the hole and
    # reads the answer out of it - and the pool still holds one worker.
    assert node.query("SELECT test_function_shm_punch_hole_pool_python(2)") == "Key 2\n"
    assert shm_region_sizes().count(region_size) == 1


def test_shared_memory_udf_file_extended_by_the_command_is_mapped_whole_at_the_next_borrow(started_cluster):
    skip_test_msan(node)

    # A file that is longer than the server's mapping - a command extended it, or a growth committed
    # its pages and could not map them - is brought into the mapping when the region is handed to
    # its next borrow: the command maps the whole file on every request and may put its answer
    # anywhere in it, so the region the server validates that answer against has to be the file.
    # The command here writes its answer - the file's size - at the end of the file, and extends a
    # 64 KiB file to 128 KiB (within the cap) after its first answer; the second answer therefore
    # lies past everything the server had mapped when it created the region.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_report_size_pool_python")

    assert node.query("SELECT test_function_shm_report_size_pool_python(1)") == "65536\n"
    assert node.query("SELECT test_function_shm_report_size_pool_python(1)") == "131072\n"


def test_shared_memory_udf_pooled_region_is_scrubbed_between_users(started_cluster):
    skip_test_msan(node)

    # A pooled region keeps what the last request left in it, and the pool serves everybody: over
    # the pipes a command only ever saw what it was sent, here it could read the tail of another
    # user's query for free. The server therefore zeroes the region - but only when the user
    # changes: the same user borrowing again sees its own leftovers, which keeps the cost off the
    # common path.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_peek_pool_python")
    node.query("CREATE USER IF NOT EXISTS shm_peek_other IDENTIFIED WITH no_password")
    node.query("GRANT SELECT ON *.* TO shm_peek_other")
    scrubbed_before = profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes")

    # The first request finds a fresh region and dirties 4 KiB past its input.
    assert node.query("SELECT test_function_shm_peek_pool_python(1)") == "clean\n"
    # The same user again: the leftovers are still there, nothing was scrubbed.
    assert node.query("SELECT test_function_shm_peek_pool_python(1)") == "dirty\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") == scrubbed_before

    # Another user: the region is clean again. The whole region was scrubbed, not just what the
    # server knew it had used: the command wrote its 4 KiB where the server never looked.
    assert node.query("SELECT test_function_shm_peek_pool_python(1)", user="shm_peek_other") == "clean\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") - scrubbed_before == 65536

    # And back: the first user does not get the other user's leftovers either.
    assert node.query("SELECT test_function_shm_peek_pool_python(1)") == "clean\n"

    # Same worker throughout - the pool holds one - so this is the scrub, not a fresh process.
    node.query("DROP USER shm_peek_other")


def test_shared_memory_udf_pooled_region_is_scrubbed_between_roles_of_one_user(started_cluster):
    skip_test_msan(node)

    # The boundary is the borrower's identity, not the login: the roles a query runs with can be
    # tied to different row policies, so what the same user's query saw under one set of roles
    # must not be readable by the command while it serves that user under another - the same
    # line the query result cache draws. Roles are switched through the user's default roles,
    # which every fresh session picks up.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_peek_pool_python")
    node.query("CREATE ROLE IF NOT EXISTS shm_peek_role_a")
    node.query("CREATE ROLE IF NOT EXISTS shm_peek_role_b")
    node.query("CREATE USER IF NOT EXISTS shm_peek_roles IDENTIFIED WITH no_password")
    node.query("GRANT SELECT ON *.* TO shm_peek_role_a, shm_peek_role_b")
    node.query("GRANT shm_peek_role_a, shm_peek_role_b TO shm_peek_roles")
    node.query("SET DEFAULT ROLE shm_peek_role_a TO shm_peek_roles")
    scrubbed_before = profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes")

    assert node.query("SELECT test_function_shm_peek_pool_python(1)", user="shm_peek_roles") == "clean\n"
    # The same user under the same role: leftovers, no scrub.
    assert node.query("SELECT test_function_shm_peek_pool_python(1)", user="shm_peek_roles") == "dirty\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") == scrubbed_before

    # The same user under another role: the region is clean again, and the scrub is counted.
    node.query("SET DEFAULT ROLE shm_peek_role_b TO shm_peek_roles")
    assert node.query("SELECT test_function_shm_peek_pool_python(1)", user="shm_peek_roles") == "clean\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") - scrubbed_before == 65536
    # And stays that user's own under that role.
    assert node.query("SELECT test_function_shm_peek_pool_python(1)", user="shm_peek_roles") == "dirty\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") - scrubbed_before == 65536

    node.query("DROP USER shm_peek_roles")
    node.query("DROP ROLE shm_peek_role_a, shm_peek_role_b")


def test_shared_memory_udf_scrub_between_users_covers_a_tail_the_server_never_mapped(started_cluster):
    skip_test_msan(node)

    # The file behind a pooled region can be longer than what the server has mapped - a command
    # extended it (only shrinking is sealed), or a growth committed its pages and could not map
    # them. The command maps the whole file, so a stale tail beyond the server's mapping is as
    # readable as the rest, and a scrub that only covered the mapping would leave exactly the
    # bytes another user's command could still read. Here the command extends a 64 KiB region to
    # 128 KiB and probes the tail at 64 KiB - past everything the server mapped when it created
    # the region. The cap is 256 KiB: the page the command dirties in the tail is a page the
    # server did not commit, and the hand-back takes such pages for pages past the end of the
    # file until it maps the file and sees; at a cap of 128 KiB that would cost the worker its
    # place in the pool, and the tail with it.
    node.query("SYSTEM RELOAD FUNCTION test_function_shm_peek_extended_pool_python")
    node.query("CREATE USER IF NOT EXISTS shm_peek_other IDENTIFIED WITH no_password")
    node.query("GRANT SELECT ON *.* TO shm_peek_other")
    scrubbed_before = profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes")

    assert node.query("SELECT test_function_shm_peek_extended_pool_python(1)") == "clean\n"
    assert node.query("SELECT test_function_shm_peek_extended_pool_python(1)") == "dirty\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") == scrubbed_before

    # Another user: the tail is clean, and the scrub covered the whole 128 KiB file - the server
    # grew its mapping to the file before zeroing it.
    assert node.query("SELECT test_function_shm_peek_extended_pool_python(1)", user="shm_peek_other") == "clean\n"
    assert profile_event_value("ExecutableUDFSharedMemoryScrubbedBytes") - scrubbed_before == 131072
    assert node.query("SELECT test_function_shm_peek_extended_pool_python(1)") == "clean\n"

    node.query("DROP USER shm_peek_other")


def test_shared_memory_udf_idle_dead_worker_late_stderr_is_reported(started_cluster):
    skip_test_msan(node)

    # The worker answers, waits out the hand-back probe, writes a diagnostic and exits in the pool.
    # Nobody is reading its pipes at that point. The next borrow finds it dead and starts a
    # replacement; the diagnostic has to be reported against the process before its pipes are
    # closed with it, or the one line that explains why the worker died is lost.
    first = node.query("SELECT test_function_shm_stderr_then_late_exit_pool_python(0)").strip()
    assert first.isdigit()
    wait_until_exited(first)
    second = node.query("SELECT test_function_shm_stderr_then_late_exit_pool_python(1)").strip()

    assert first != second, f"the dead worker was reused: {first}"
    assert node.contains_in_log("exited while it was idle in the pool, after writing to its stderr")
    assert node.contains_in_log("last words of the worker")


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


def test_shared_memory_udf_previous_borrow_stderr_is_not_thrown_at_the_next_query(started_cluster):
    skip_test_msan(node)

    # The quiet-gap command under `throw`: it answers, waits out the hand-back probe, then writes two
    # pipefuls to stderr and sits blocked in `write`. The next borrow finds those bytes on the pipe.
    # They are the earlier query's, and that query has already succeeded - the probe finished before
    # they arrived, which is the documented boundary - so the borrow has to take them off the pipe
    # without putting them through its own reaction: under `throw`, feeding them through would fail
    # this query for a diagnostic it did not cause. Two pipefuls, because the borrow-start report is
    # capped at a few KiB and the rest goes through a separate drain: only a flood proves that drain
    # is reaction-free as well.
    first = node.query("SELECT test_function_shm_stderr_flood_after_gap_throw_pool_python(0)").strip()

    pids = {first}
    for i in range(1, 3):
        # The command's gap is long (2 s) so that the query it answered is over - probe and all -
        # before the flood starts, on any machine; and the next borrow waits for the flood to be
        # under way, rather than for a fixed time.
        wait_until_blocked_writing(first)
        pids.add(node.query(f"SELECT test_function_shm_stderr_flood_after_gap_throw_pool_python({i})").strip())

    assert len(pids) == 1, f"the worker was not reused: {pids}"
    assert node.contains_in_log(
        "The process of an executable UDF had unread output on its stderr when it was borrowed"
    )


def test_shared_memory_udf_none_reaction_worker_flooding_after_a_quiet_gap(started_cluster):
    skip_test_msan(node)

    # The harder shape of the test below: the command answers, stays quiet for longer than the
    # server spends draining its stderr when it takes the worker back, and only then writes two
    # pipefuls. Any check that only looks at the pipe at hand-back time sees nothing and cannot say
    # anything about what the command does next, so if that check were what kept `stderr_reaction`
    # `none` from blocking a chatty command, this shape would defeat it.
    #
    # What actually keeps the promise is that the read loop polls stderr alongside stdout: the query
    # waiting for a response is the one that drains the command writing it. The next borrow starts
    # only once the worker is provably blocked in `write` on its full stderr pipe - the state this
    # test is about - rather than after a sleep long enough to hope for it.
    first = node.query("SELECT test_function_shm_stderr_flood_after_gap_pool_python(0)").strip()

    pids = {first}
    for i in range(1, 3):
        wait_until_blocked_writing(first)
        started = time.monotonic()
        pids.add(node.query(f"SELECT test_function_shm_stderr_flood_after_gap_pool_python({i})").strip())
        elapsed = time.monotonic() - started
        assert elapsed < 5, f"query {i} took {elapsed:.1f}s - the worker was left blocked on stderr"

    assert len(pids) == 1, f"the worker was not reused: {pids}"


def test_shared_memory_udf_startup_stderr_of_a_fresh_pooled_worker_fails_the_query(started_cluster):
    skip_test_msan(node)

    # The command logs a line to stderr as it starts, before it reads its first request. The
    # process is new for this borrow, so that line is this query's and nobody else's: under
    # `throw` it fails the query, exactly as the pipe transport does. Taking it for a previous
    # invocation's leftovers - the borrow-start cleanup does that for a worker that served an
    # earlier borrow - would log it against nobody and let the query succeed, and `throw` would
    # then mean something different on the two transports.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_stderr_at_startup_throw_pool_python(1)")
    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "starting up" in str(exc.value), str(exc.value)


def test_shared_memory_udf_none_reaction_worker_is_not_left_blocked_on_stderr(started_cluster):
    skip_test_msan(node)

    # The command answers and then writes twice a pipeful to stderr before going back to read the
    # next request. `stderr_reaction` `none` documents that a chatty command never blocks on a full
    # stderr pipe - easy to honour while a query is running, and easy to lose at the moment the
    # worker goes back into the pool, where nothing is reading that pipe any more and `none` says
    # the bytes are nobody's. A worker handed back with a full pipe is blocked in `write` and will
    # never read the next request; the borrow after it waits out `command_read_timeout` instead.
    #
    # A pool of one, so every query gets that same worker back.
    pids = set()
    for i in range(3):
        started = time.monotonic()
        pids.add(node.query(f"SELECT test_function_shm_stderr_flood_none_pool_python({i})").strip())
        elapsed = time.monotonic() - started
        assert elapsed < 5, f"query {i} took {elapsed:.1f}s - the worker was left blocked on stderr"

    # And it really is one worker being reused: `none` costs the command nothing here.
    assert len(pids) == 1, f"the worker was not reused: {pids}"


def test_shared_memory_udf_round_trips_a_binary_format(started_cluster):
    skip_test_msan(node)

    # Every other function in this suite speaks a line-oriented text format, which is exactly the
    # kind that would absorb a framing bug: a byte lost or gained lands on a newline and nobody
    # notices. `RowBinary` has no such give. This one carries two columns, a `Nullable` with its own
    # null map, and strings with embedded NUL bytes and newlines - so a mistake anywhere in the
    # exchange shows up as a parse failure or a wrong value rather than as nothing.
    result = node.query(
        """
        SELECT test_function_shm_binary_pool_python(id, label)
        FROM values(
            'id UInt64, label Nullable(String)',
            (1, 'plain'),
            (2, NULL),
            (3, 'with\\0embedded\\0nuls'),
            (4, 'with\\nnewline'),
            (5, ''))
        ORDER BY id
        FORMAT TSVRaw
        """
    )

    assert result == (
        "#1=plain\n"
        "#2=<null>\n"
        "#3=with\0embedded\0nuls\n"
        "#4=with\nnewline\n"
        "#5=\n"
    ), repr(result)

    # And it survives the pool: the same worker answers again, with the region reused.
    assert (
        node.query(
            "SELECT test_function_shm_binary_pool_python(7, 'again') FORMAT TSVRaw"
        )
        == "#7=again\n"
    )


def test_shared_memory_udf_stderr_written_just_before_exit_still_throws(started_cluster):
    skip_test_msan(node)

    # The command answers, complains on stderr and exits in the same breath - no pause anywhere,
    # which is the timing the other late-stderr test deliberately avoids. Reaping a child closes its
    # pipes, so a server that reaps before it reads throws away everything the child had written and
    # nobody had read: under `stderr_reaction` `throw` the query would succeed while the command was
    # shouting.
    #
    # The row count is what makes it deterministic rather than a race. The server parses half a
    # million rows out of the region before it gets anywhere near reaping, so the command is
    # provably gone by then - with a single row the two finish at the same moment and the server
    # usually happens to read the pipe first, which proves nothing.
    with pytest.raises(Exception) as exc:
        node.query(
            "SELECT DISTINCT test_function_shm_stderr_then_exit_python(number) "
            "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000 FORMAT Null"
        )

    assert "Executable generates stderr" in str(exc.value), str(exc.value)
    assert "eeee" in str(exc.value), str(exc.value)

    assert node.query("SELECT 1") == "1\n"


def test_shared_memory_udf_stray_byte_after_the_probe_cannot_poison_the_next_borrow(started_cluster):
    skip_test_msan(node)

    # The worker answers, is taken back into the pool while its pipes are provably empty, and only
    # then writes one stray byte. The probe cannot catch that - it is one instant - so the byte is
    # sitting on the worker's stdout when the next query borrows it.
    #
    # What matters is what the next query does with it. Read as a bare status varint it would be a
    # perfectly plausible frame - `0` is success, and the rest of the real frame shifts into the
    # offset and size fields - so the query could come back with the right number of rows and the
    # wrong values in them. But before the first request is sent the byte is provably not this
    # query's: nothing has been asked yet. So the worker is discarded at the borrow, a replacement
    # is started, and the query is answered correctly by it - not failed for what the previous
    # query's command did. The request id stays as the last line, for a byte that lands between
    # the borrow-time probe and the request.
    node.exec_in_container(["bash", "-c", f"rm -f {STRAY_BYTE_MARKER}"])
    assert node.query("SELECT test_function_shm_stray_byte_after_probe_pool_python(1)") == "Key 1\n"
    # This function's region size is its own, so its regions can be told from every other pooled
    # worker's in the server.
    region_size = 12288
    regions_before = [region for region in shm_regions() if region[1] == region_size]
    assert len(regions_before) == 1, shm_regions()

    # The byte has to be on the pipe before the next borrow, and when it lands is the command's
    # business, not this test's: the command reports it by creating a marker file right after the
    # write, and the borrow starts once that file exists.
    wait_for_container_file(STRAY_BYTE_MARKER)

    discards_before = profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards")
    assert node.query("SELECT test_function_shm_stray_byte_after_probe_pool_python(2)") == "Key 2\n"
    assert profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards") == discards_before + 1
    assert node.contains_in_log("had unread output on its stdout when it was borrowed")

    # The discarded worker is alive - it wrote that byte and went back to reading - and it holds a
    # writable descriptor to the region it was started with. So the region goes with it: the
    # replacement is started on a fresh one, which the inode shows, and the old one is released
    # rather than leaked, which the count shows. Handing the old region to the replacement would
    # leave this query reading a mapping a discarded process can still write into.
    regions_after = [region for region in shm_regions() if region[1] == region_size]
    assert len(regions_after) == 1, shm_regions()
    assert regions_after[0][0] != regions_before[0][0], (regions_before, regions_after)


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
        "SELECT use_shared_memory, shared_memory_size, shared_memory_max_size, shared_memory_pipeline "
        "FROM system.user_defined_functions WHERE name = 'test_function_shm_pipeline_pool_python'"
    ).strip()
    assert row == "1\t1048576\t1048576\t1", row

    # A region that is allowed to grow reports the bound it may grow to, not the raw `0` the
    # configuration uses to mean "it may not".
    row = node.query(
        "SELECT use_shared_memory, shared_memory_size, shared_memory_max_size, shared_memory_pipeline "
        "FROM system.user_defined_functions WHERE name = 'test_function_shm_grow_python'"
    ).strip()
    assert row == "1\t16\t1048576\t0", row

    # A function the loader refused has no configuration at all, so the columns are at their
    # defaults rather than showing something half-read out of a config that was never accepted.
    row = node.query(
        "SELECT load_status, use_shared_memory, shared_memory_size, shared_memory_max_size, "
        "shared_memory_pipeline "
        "FROM system.user_defined_functions WHERE name = 'test_function_shm_bad_size_no_shm'"
    ).strip()
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


def test_shared_memory_udf_pool_command_late_stderr_under_throw_fails_the_query_that_caused_it(started_cluster):
    skip_test_msan(node)

    # The command answers correctly and then, 2 ms later, writes a line to stderr - after the server
    # has read the response, but while it is still parsing the 500000-row answer out of the area.
    # Under `stderr_reaction` `throw` that line is a verdict, and it has to land on the query whose
    # arguments caused it: the drain that follows the last read finds it, and this query fails.
    # The worker is then discarded (a command that failed a query is not handed on), so every one
    # of these queries fails for its own line and none for a previous query's. One block means one
    # borrow.
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query(
                "SELECT DISTINCT test_function_shm_chatty_stderr_pool_python(number) "
                "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000"
            )
        assert "Executable generates stderr: done" in str(exc.value), str(exc.value)


def test_shared_memory_udf_pool_command_logging_after_its_answer_keeps_its_worker(started_cluster):
    skip_test_msan(node)

    # The same command, under `stderr_reaction` `log_last`: the line it writes after answering is
    # a log line, not a verdict. It is taken off the pipe where the worker is handed back and
    # logged against the query that caused it, and the worker goes back to the pool - one process
    # serves every call. Discarding it, as under `throw`, would turn `executable_pool` into a
    # process per call for every command that logs after its rows.
    query_ids = [f"shm-chatty-stderr-log-{i}" for i in range(3)]
    pids = [
        node.query(
            "SELECT DISTINCT test_function_shm_chatty_stderr_log_pool_python(number) "
            "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000",
            query_id=query_id,
        ).strip()
        for query_id in query_ids
    ]
    assert all(pid.isdigit() for pid in pids), pids
    assert len(set(pids)) == 1, f"a worker that only logged after answering was not reused: {pids}"

    # Keeping the worker is half of it: the line must also have been logged, under the query that
    # caused it. A server that kept the worker by not looking at its stderr would pass the check
    # above and drop the one diagnostic the command wrote.
    for query_id in query_ids:
        logged = node.exec_in_container(
            [
                "bash",
                "-c",
                f"grep -a '{query_id}' /var/log/clickhouse-server/clickhouse-server.log "
                "| grep -c 'Executable generates stderr at the end: done' || true",
            ]
        ).strip()
        assert logged == "1", f"query {query_id} logged the command's line {logged} times"


def test_shared_memory_udf_pool_discard_survives_being_decided_twice(started_cluster):
    skip_test_msan(node)

    # Same misbehaving command under `throw`, but with `check_exit_code` off - which is what makes
    # the decision to discard get made twice. With it on, the discarded child is reaped, and a
    # reaped child is recognised on sight; with it off, nothing about the worker says what was
    # decided about it. And by then the evidence is gone: reporting the discard drained the
    # leftover stderr, and closing the child's stdin is what discarding means. A second look would
    # find two clean pipes and return a worker whose stdin is already closed, and the query after
    # that would fail writing its first request instead of for its own stderr line. So all three
    # queries have to fail the same way - for the line their own command wrote.
    for _ in range(3):
        with pytest.raises(Exception) as exc:
            node.query(
                "SELECT DISTINCT test_function_shm_chatty_stderr_no_exit_check_pool_python(number) "
                "FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000"
            )
        assert "Executable generates stderr: done" in str(exc.value), str(exc.value)


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


def test_shared_memory_udf_command_died(started_cluster):
    skip_test_msan(node)

    # The command exits without answering; the server must fail the query rather than hang.
    with pytest.raises(Exception) as exc:
        node.query("SELECT test_function_shm_die_python(1) FORMAT Null")

    assert "test_function_shm_die_python" in str(exc.value)

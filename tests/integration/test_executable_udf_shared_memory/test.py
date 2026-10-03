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


def shm_region_count():
    return len(shm_regions())


config = """<clickhouse>
    <user_defined_executable_functions_config>/etc/clickhouse-server/functions/test_function_config.xml</user_defined_executable_functions_config>
</clickhouse>"""

# The idle charge is a charge to the server-wide memory tracker, and the background memory worker
# replaces that tracker's value with a measurement on every tick by default - one that does not see
# the pages of a region where it is jemalloc's resident size or a sanitizer's allocator statistic.
# What `test_shared_memory_udf_idle_pooled_region_counts_against_the_server_limit` checks is the
# charge itself, so the tracker is kept as a plain counter here. Read once at startup.
memory_worker_config = """<clickhouse>
    <memory_worker_correct_memory_tracker>0</memory_worker_correct_memory_tracker>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.replace_config(
            "/etc/clickhouse-server/config.d/executable_user_defined_functions_config.xml",
            config,
        )
        node.replace_config(
            "/etc/clickhouse-server/config.d/memory_worker_does_not_correct_the_tracker.xml",
            memory_worker_config,
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

    # The exact idle charge is checked by the stateless test
    # `05321_executable_udf_shared_memory_accounting` through the pooled-bytes metric; this is the
    # charge doing what a charge is for. An idle pooled worker holds a region nobody is querying
    # through, and with the server-wide tracker kept as a counter (`memory_worker_config` above) the
    # charge counts against `max_server_memory_usage`. Two things have to be true for that, and both
    # are checked here:
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


def test_shared_memory_udf_slow_cleanup_is_waited_for(started_cluster):
    skip_test_msan(node)

    # The command answers, and on `stdin` EOF exits successfully only after its
    # `command_termination_timeout`. Under the default `check_exit_code` a non-pooled command is
    # waited for until it exits, as on the pipe transport, so the query succeeds.
    assert node.query("SELECT test_function_shm_lingers_python(1)") == "Key 1\n"

    # With `check_exit_code = 0` the status is not wanted, and the same command answers normally.
    assert node.query("SELECT test_function_shm_lingers_no_exit_check_python(1)") == "Key 1\n"


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


def test_shared_memory_udf_pool_command_floods_stdout(started_cluster):
    skip_test_msan(node)

    # A worker that writes past its response frame is discarded, and this one writes past the point
    # where that traps the server: it answers correctly and then writes megabytes, filling the pipe
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


def test_shared_memory_udf_worker_discarded_for_its_stdout_does_not_fail_a_query_under_throw(started_cluster):
    skip_test_msan(node)

    # The command answers correctly and leaves one byte past its response frame, so the worker is
    # discarded - and that discard looks at its stderr, to report whatever the command left there
    # along with the reason. Here it left nothing: the byte was on its *stdout*.
    #
    # Under `stderr_reaction` `throw` an empty read must stay empty. "The command wrote to stderr"
    # is a verdict on the query, and a query whose rows are already correct must not be failed by
    # a probe that read nothing - least of all with an empty message where the diagnostic would be.
    discards_before = profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards")

    # The command answers with its own pid (see the script), so the answer is just "a row that is
    # this borrow's": what matters here is that the query is answered at all.
    answer = node.query("SELECT test_function_shm_busy_chatty_throw_pool_python(1)").strip()
    assert answer.isdigit(), answer

    # The discard really happened, so this is the path under test and not the ordinary one.
    assert (
        profile_event_value("ExecutableUDFSharedMemoryDirtyChannelDiscards") == discards_before + 1
    )


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

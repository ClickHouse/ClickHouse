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
    skip_test_msan(node)

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

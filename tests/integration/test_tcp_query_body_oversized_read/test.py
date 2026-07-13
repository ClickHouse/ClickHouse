"""
Regression test for the native TCP handler: a client that sends a query body
larger than the memory limit must be rejected cleanly, without driving the
server's RSS past `max_server_memory_usage` and tripping the cgroup OOM
killer.

The previous implementation in `src/Server/TCPHandler.cpp` read the query
string via `readStringBinary(state->query, *in)`, where `state->query` was
`std::string`. The resize on receive went through `operator new` ->
`Memory::trackMemoryFromC` -> `allocNoThrow`, which never throws on per-
server / per-query memory limits. A single client could push the server's
RSS past `max_server_memory_usage` and get cgroup-OOM-killed.

The fix changes `state->query` to `StringWithMemoryTracking`, whose
allocator goes through `CurrentMemoryTracker::alloc` (the throwing path).
The oversized resize now throws `MEMORY_LIMIT_EXCEEDED` and the server
stays alive.

This test runs ClickHouse in a 1 GiB Docker memory cgroup, sends a single
~950 MiB query body, and asserts:
  - the server is still running afterwards,
  - no cgroup OOM kill was recorded,
  - the server log records the `MEMORY_LIMIT_EXCEEDED` exception.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# 1 GiB container memory limit. ClickHouse will detect this via cgroup v2 and
# derive `max_server_memory_usage = 0.9 * 1 GiB = 921.6 MiB`.
node = cluster.add_instance(
    "node",
    main_configs=["configs/no_log_noise.xml"],
    mem_limit="1g",
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _cgroup_oom_kill_count(n):
    """Read kernel-recorded OOM kills from the container's cgroup v2 stats."""
    out = n.exec_in_container(
        ["bash", "-c", "cat /sys/fs/cgroup/memory.events 2>/dev/null || true"],
        user="root",
        nothrow=True,
    )
    for line in (out or "").splitlines():
        key, _, value = line.strip().partition(" ")
        if key == "oom_kill":
            return int(value)
    return 0


def test_tcp_query_body_oversized_read():
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers — they perturb the memory cap.")

    # 1. Confirm the server saw and applied the 1 GiB cgroup limit. Without
    # this, the assertions below would not be exercising the per-server
    # memory limit at all.
    assert node.contains_in_log("Available RAM: 1.00 GiB"), (
        "server did not detect the 1 GiB cgroup limit at startup"
    )
    assert node.contains_in_log("Changed setting 'max_server_memory_usage'"), (
        "server did not derive max_server_memory_usage from available RAM"
    )

    pid_before = node.get_process_pid("clickhouse")
    oom_kill_before = _cgroup_oom_kill_count(node)

    # 2. Send a ~950 MiB query body via the native TCP protocol from the
    # host-side client. The client process is on the host (outside the
    # container's cgroup) so it can buffer the query body without being
    # OOM-killed; only the server is on the diet.
    # `ignore_error=True` because the server rejects mid-send and the
    # client typically observes the disconnection.
    big_query = "SELECT length('" + "x" * (950 * 1024 * 1024) + "')"
    node.query(
        big_query,
        settings={"max_memory_usage": 1000000, "max_query_size": 1000},
        ignore_error=True,
        timeout=60,
    )

    # 3. Give the server a moment to settle.
    time.sleep(1)

    # 4. Server must still be alive — no OOM kill, no crash.
    pid_after = node.get_process_pid("clickhouse")
    assert pid_after is not None, (
        "expected ClickHouse to stay alive after rejecting the oversized "
        "query, but the process is gone"
    )
    assert pid_after == pid_before, (
        f"expected same ClickHouse process to keep running, "
        f"got PID {pid_before} -> {pid_after}"
    )
    assert node.query("SELECT 1").strip() == "1", (
        "server is unresponsive after rejecting the oversized query"
    )

    # 5. The kernel cgroup OOM killer must not have fired.
    oom_kill_after = _cgroup_oom_kill_count(node)
    assert oom_kill_after == oom_kill_before, (
        f"unexpected cgroup OOM kill: counter {oom_kill_before} -> {oom_kill_after}"
    )

    # 6. Confirm the rejection went through `MEMORY_LIMIT_EXCEEDED`, not via
    # some other code path (which would not protect against memory growth
    # from the unbounded read).
    assert node.contains_in_log("MEMORY_LIMIT_EXCEEDED"), (
        "expected MEMORY_LIMIT_EXCEEDED in server log after oversized query"
    )

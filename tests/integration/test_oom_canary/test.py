import time
from dataclasses import dataclass

import pytest

from helpers.cluster import ClickHouseCluster


@dataclass(frozen=True)
class CanaryProcess:
    pid: int
    start: str

cluster = ClickHouseCluster(__file__)

# Canary enabled
node_enabled = cluster.add_instance(
    "enabled",
    main_configs=["configs/enabled.xml"],
    stay_alive=True,
)

# Canary enabled but relaunch disabled
node_no_relaunch = cluster.add_instance(
    "no_relaunch",
    main_configs=["configs/no_relaunch.xml"],
    stay_alive=True,
)

# Three "disabled" variants
node_explicit_off = cluster.add_instance(
    "explicit_off",
    main_configs=["configs/enable_off.xml"],
)
node_default = cluster.add_instance("default")
node_gate_off = cluster.add_instance(
    "gate_off",
    main_configs=["configs/gate_off.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def find_canary(node):
    output = node.exec_in_container(
        ["pgrep", "-f", "oom-canary"],
        nothrow=True,
    ).split()
    if not output:
        return None
    pid = int(output[0])
    start = node.exec_in_container(
        ["ps", "-o", "lstart=", "-p", str(pid)], nothrow=True
    ).strip()
    if not start:
        return None
    return CanaryProcess(pid=pid, start=start)


def wait_for_relaunch(node, before, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        c = find_canary(node)
        if c is not None and c != before:
            return c
        time.sleep(0.05)
    raise AssertionError(f"no relaunch within {timeout}s")


def crash_log_oom_count(node):
    node.query("SYSTEM FLUSH LOGS crash_log")
    return int(
        node.query(
            "SELECT count() FROM system.crash_log "
            "WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'"
        ).strip()
    )


def test_failpoint_confirmed_oom():
    node = node_enabled
    crash_log_oom_count_before = crash_log_oom_count(node)
    canary = find_canary(node)
    assert canary is not None

    node.query("SYSTEM ENABLE FAILPOINT oom_canary_force_oom_evidence")
    node.exec_in_container(["kill", "-9", str(canary.pid)])
    wait_for_relaunch(node, canary)
    node.wait_for_log_line(
        "OOM canary killed by SIGKILL with cgroup OOM evidence"
    )
    assert crash_log_oom_count(node) > crash_log_oom_count_before


def test_manual_sigkill_no_evidence():
    node = node_enabled
    node.query("SYSTEM DISABLE FAILPOINT oom_canary_force_oom_evidence")
    crash_log_oom_count_before = crash_log_oom_count(node)
    canary = find_canary(node)
    assert canary is not None

    node.exec_in_container(["kill", "-9", str(canary.pid)])
    wait_for_relaunch(node, canary)

    assert crash_log_oom_count(node) == crash_log_oom_count_before


def test_relaunch_false_stops_after_one_kill():
    node = node_no_relaunch
    canary = find_canary(node)
    assert canary is not None

    node.query("SYSTEM ENABLE FAILPOINT oom_canary_force_oom_evidence")
    node.exec_in_container(["kill", "-9", str(canary.pid)])
    node.wait_for_log_line("OOM canary monitor thread exiting")
    assert find_canary(node) is None


@pytest.mark.parametrize(
    "node",
    [node_explicit_off, node_default, node_gate_off],
    ids=["explicit_off", "default", "gate_off"],
)
def test_canary_disabled_variants(node):
    assert node.contains_in_log("OOM canary is disabled")
    assert find_canary(node) is None

"""
Test that CGroup CPU metrics (CGroupUserTime, CGroupSystemTime) work correctly
when ClickHouse runs in a container with a nested cgroup path.

This test reproduces the path concatenation bug from PR 62003 where
getCgroupsV2PathContainingFile returns a path without trailing separator,
causing openCgroupv2MetricFile to build invalid file paths.

The bug: src/Common/AsynchronousMetrics.cpp:119 does
    openFileIfExists((path.value() + filename).c_str(), out);
which concatenates strings without a path separator. If path is
"/sys/fs/cgroup/docker/abc" and filename is "cpu.stat", the result is
"/sys/fs/cgroup/docker/abccpu.stat" instead of the correct
"/sys/fs/cgroup/docker/abc/cpu.stat".

This causes CGroup metrics to silently fail for containers in nested cgroups.

Environment requirement: This test requires a cgroup v2 environment.
The test will be skipped if cgroup v2 is not available.
"""

import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# Use stay_alive=True to keep the container running for multiple queries
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["configs/config.xml"],
)

# Retry settings for polling metrics
MAX_RETRIES = 30
RETRY_DELAY = 1  # seconds


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_cgroup_info(instance):
    """Read the cgroup path from /proc/self/cgroup inside the container.

    Raises on infrastructure failures (container errors, permission issues).
    """
    # Let exceptions propagate - infrastructure failures should fail the test
    result = instance.exec_in_container(["cat", "/proc/self/cgroup"])
    return result.strip()


def is_cgroupv2_enabled(instance):
    """Check if cgroup v2 is enabled by looking for cgroup.controllers file.

    Returns False if the file doesn't exist (expected for cgroup v1).
    Raises on infrastructure failures (container errors, permission issues).
    """
    try:
        instance.exec_in_container(["test", "-f", "/sys/fs/cgroup/cgroup.controllers"])
        return True
    except Exception as e:
        # "test -f" exits non-zero if file doesn't exist - this is expected for cgroup v1
        # But distinguish from real infrastructure errors by checking the error message
        error_str = str(e).lower()
        # exec_in_container raises with message "return non-zero code {code}:"
        if "non-zero code 1" in error_str:
            # Command executed but file doesn't exist - cgroup v1 environment
            return False
        # Real infrastructure error (Docker failure, permission denied, etc.)
        raise


def get_cgroup_path(instance):
    """Extract the cgroup v2 path from /proc/self/cgroup."""
    cgroup_info = get_cgroup_info(instance)
    # cgroup v2 lines start with "0::"
    for line in cgroup_info.split("\n"):
        if line.startswith("0::"):
            # Format: 0::/path  where /path is the cgroup path
            return line[3:]  # Remove "0::" prefix
    return None


def get_async_metric_value(instance, metric_name):
    """Get the current value of an asynchronous metric."""
    result = instance.query(
        f"""
        SELECT value
        FROM system.asynchronous_metrics
        WHERE metric = '{metric_name}'
        """
    ).strip()
    return result


def wait_for_metric(instance, metric_name, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    """
    Poll system.asynchronous_metrics until the metric appears or timeout.
    Returns the metric value if found, empty string if not found after retries.
    """
    for attempt in range(max_retries):
        value = get_async_metric_value(instance, metric_name)
        if value != "":
            return value
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    return ""


def test_cgroup_cpu_metrics_exist(started_cluster):
    """
    Test that CGroupUserTime and CGroupSystemTime metrics are reported
    when running in a cgroup v2 environment.

    If the path concatenation bug exists, these metrics will not be present
    because the invalid file path prevents reading the cgroup cpu.stat file.

    This is the core test for the bug: if metrics are missing, the bug is present.
    """
    # Check cgroup v2 availability first
    cgroupv2_enabled = is_cgroupv2_enabled(node)
    if not cgroupv2_enabled:
        pytest.skip("Cgroup v2 is not enabled in this environment")

    # Log cgroup environment for debugging
    cgroup_path = get_cgroup_path(node)
    print(f"Cgroup v2 enabled: {cgroupv2_enabled}")
    print(f"Cgroup path: {cgroup_path}")

    # Check if this is a nested cgroup (important for reproducing the bug)
    is_nested = cgroup_path and cgroup_path != "/"
    print(f"Nested cgroup: {is_nested}")

    # Skip if not in a nested cgroup - the path concatenation bug only manifests
    # when the cgroup path is not "/" (e.g., "/docker/abc123...")
    if not is_nested:
        pytest.skip(
            f"Test requires nested cgroup to exercise path concatenation bug. "
            f"Current cgroup path: '{cgroup_path}'"
        )

    # Wait for metrics to be collected using retry/poll loop
    user_time = wait_for_metric(node, "CGroupUserTime")
    system_time = wait_for_metric(node, "CGroupSystemTime")

    print(f"CGroupUserTime: '{user_time}'")
    print(f"CGroupSystemTime: '{system_time}'")

    # These metrics must exist in a cgroup v2 environment
    # If they don't exist, it indicates the path concatenation bug
    assert user_time != "", (
        f"CGroupUserTime metric not found in cgroup v2 environment. "
        f"This indicates the path concatenation bug in openCgroupv2MetricFile. "
        f"Cgroup path: {cgroup_path}, nested: {is_nested}"
    )
    assert system_time != "", (
        f"CGroupSystemTime metric not found in cgroup v2 environment. "
        f"This indicates the path concatenation bug in openCgroupv2MetricFile. "
        f"Cgroup path: {cgroup_path}, nested: {is_nested}"
    )

    # Values should be non-negative numbers
    user_time_val = float(user_time)
    system_time_val = float(system_time)

    assert user_time_val >= 0, f"CGroupUserTime should be non-negative, got {user_time_val}"
    assert system_time_val >= 0, f"CGroupSystemTime should be non-negative, got {system_time_val}"


def test_cgroup_metrics_update_after_cpu_work(started_cluster):
    """
    Verify that cgroup metrics report CPU time that increases after work.

    This test runs a CPU-intensive query and verifies that the CGroupUserTime
    metric increases, confirming the metrics are actually tracking CPU usage.
    """
    cgroupv2_enabled = is_cgroupv2_enabled(node)
    if not cgroupv2_enabled:
        pytest.skip("Cgroup v2 is not enabled in this environment")

    # Require nested cgroup to exercise path concatenation bug
    cgroup_path = get_cgroup_path(node)
    if not cgroup_path or cgroup_path == "/":
        pytest.skip(
            f"Test requires nested cgroup to exercise path concatenation bug. "
            f"Current cgroup path: '{cgroup_path}'"
        )

    # Get initial values using retry loop
    initial_user = wait_for_metric(node, "CGroupUserTime")
    initial_system = wait_for_metric(node, "CGroupSystemTime")

    # If metrics aren't available, this is a test failure (bug present)
    assert initial_user != "", "CGroupUserTime metric should exist"
    assert initial_system != "", "CGroupSystemTime metric should exist"

    initial_user_val = float(initial_user)
    initial_system_val = float(initial_system)
    initial_total = initial_user_val + initial_system_val

    print(f"Initial values - User: {initial_user_val}, System: {initial_system_val}, Total: {initial_total}")

    # Run a CPU-intensive query
    node.query(
        "SELECT sum(number * number) FROM numbers(10000000) FORMAT Null",
        settings={"max_threads": 1},
    )

    # Poll for updated metrics - wait until we see an increase
    # The metrics should update after async metrics collection cycle
    final_user_val = initial_user_val
    final_system_val = initial_system_val
    final_total = initial_total

    for attempt in range(MAX_RETRIES):
        final_user = get_async_metric_value(node, "CGroupUserTime")
        final_system = get_async_metric_value(node, "CGroupSystemTime")

        if final_user and final_system:
            final_user_val = float(final_user)
            final_system_val = float(final_system)
            final_total = final_user_val + final_system_val

            # Check if we've seen a meaningful increase (at least 0.001 seconds)
            if final_total > initial_total + 0.001:
                break

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)

    print(f"Final values - User: {final_user_val}, System: {final_system_val}, Total: {final_total}")
    print(f"Change - User: +{final_user_val - initial_user_val}, System: +{final_system_val - initial_system_val}")
    print(f"Total change: +{final_total - initial_total}")

    # After CPU work, the combined user+system time should have increased
    # by at least some meaningful amount (0.001 seconds = 1ms)
    min_expected_increase = 0.001
    actual_increase = final_total - initial_total

    assert actual_increase >= min_expected_increase, (
        f"CGroup CPU time (user+system) should increase by at least {min_expected_increase}s after CPU work, "
        f"but only increased by {actual_increase}s. "
        f"Initial: {initial_total}s, Final: {final_total}s. "
        f"This may indicate the metrics are not being updated correctly."
    )

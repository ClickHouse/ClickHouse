import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

# The default of the `seccomp` server setting is `trap`, so this node gets a filter without being
# configured for one.
default_node = cluster.add_instance("default_node")
disabled_node = cluster.add_instance(
    "disabled_node", main_configs=["configs/disabled.xml"]
)
errno_node = cluster.add_instance("errno_node", main_configs=["configs/errno.xml"])

# `/proc/<pid>/status` reports the seccomp mode of a process: 0 is no filter, 2 is a BPF filter.
SECCOMP_MODE_DISABLED = "0"
SECCOMP_MODE_FILTER = "2"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [default_node, disabled_node, errno_node]:
            os.system(
                f"docker cp {os.path.join(SCRIPT_DIR, 'user_scripts/.')} "
                f"{node.docker_id}:/var/lib/clickhouse/user_scripts"
            )
            node.exec_in_container(
                ["bash", "-c", "chmod 0755 /var/lib/clickhouse/user_scripts/*"],
                user="root",
            )

        yield cluster
    finally:
        cluster.shutdown()


def get_status_field(node, pid, field):
    output = node.exec_in_container(
        ["bash", "-c", f"grep -E '^{field}:' /proc/{pid}/status"], user="root"
    )
    return output.split(":", 1)[1].strip()


def get_server_pid(node):
    # Not `get_process_pid`, which matches the watchdog parent as well - and the watchdog forks
    # before the filter is installed, so it is not filtered. The status file holds the pid of the
    # process that actually serves queries.
    output = node.exec_in_container(
        ["bash", "-c", "grep -E '^PID:' /var/lib/clickhouse/status"], user="root"
    )
    return int(output.split(":", 1)[1].strip())


def run_probe(node):
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")

    # A seccomp filter survives both `fork` and `execve`, so an executable user defined function or
    # dictionary, a bridge, or the OOM canary runs under the same policy as the server.
    return node.query(
        "SELECT * FROM executable('seccomp_status.py', 'TabSeparated', "
        "'mode String, getxattr_result String')"
    )


def test_setting_is_reported(started_cluster):
    for node, expected in [
        (default_node, "trap"),
        (disabled_node, "disabled"),
        (errno_node, "errno"),
    ]:
        assert (
            node.query(
                "SELECT value FROM system.server_settings WHERE name = 'seccomp'"
            )
            == expected + "\n"
        )


def test_filter_is_installed_by_default(started_cluster):
    pid = get_server_pid(default_node)
    assert get_status_field(default_node, pid, "Seccomp") == SECCOMP_MODE_FILTER
    # Installing a filter requires it, and it also stops anything the server starts from gaining
    # privileges through a setuid program.
    assert get_status_field(default_node, pid, "NoNewPrivs") == "1"
    assert default_node.contains_in_log("Applied a seccomp policy to this process")


def test_filter_covers_every_thread(started_cluster):
    # The filter is installed with `SECCOMP_FILTER_FLAG_TSYNC`, so the threads that already existed
    # when it was installed must be filtered too, not just the one that installed it.
    pid = get_server_pid(default_node)
    modes = default_node.exec_in_container(
        [
            "bash",
            "-c",
            f"for task in /proc/{pid}/task/*; do grep -hE '^Seccomp:' $task/status 2>/dev/null; "
            f"done | sort -u",
        ],
        user="root",
    )
    assert modes.split() == ["Seccomp:", SECCOMP_MODE_FILTER]


def test_no_filter_when_disabled(started_cluster):
    pid = get_server_pid(disabled_node)
    assert get_status_field(disabled_node, pid, "Seccomp") == SECCOMP_MODE_DISABLED
    assert not disabled_node.contains_in_log("Applied a seccomp policy to this process")

    # Without a filter the call goes through to the kernel, which answers about the attribute
    # itself - which of the two answers it gives depends on the filesystem, and neither is `EPERM`.
    mode, getxattr_result = run_probe(disabled_node).split()
    assert mode == SECCOMP_MODE_DISABLED
    assert getxattr_result in ("ENODATA", "ENOTSUP", "EOPNOTSUPP")


def test_system_call_outside_the_policy_is_refused(started_cluster):
    # `getxattr` is not in the policy, so the `errno` mode turns it into `EPERM` - which is what
    # makes the filter more than a formality.
    assert run_probe(errno_node) == f"{SECCOMP_MODE_FILTER}\tEPERM\n"


def test_server_works_under_the_filter(started_cluster):
    # A smoke test over the system calls a query needs: reading and writing files, spawning
    # threads, networking, timers. `errno_node` is the interesting one: if the policy were missing
    # something, the call would fail with `EPERM` instead of taking the server down, so the query
    # would report a strange error rather than losing the connection.
    for node in [default_node, errno_node]:
        node.query("CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k")
        node.query("INSERT INTO t SELECT number, toString(number) FROM numbers(100000)")
        node.query("OPTIMIZE TABLE t FINAL")
        assert node.query("SELECT count(), uniqExact(s) FROM t") == "100000\t100000\n"
        assert node.query("SELECT count() > 0 FROM system.stack_trace") == "1\n"
        node.query("SYSTEM FLUSH LOGS")
        node.query("DROP TABLE t SYNC")

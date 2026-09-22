import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

# The default of the `seccomp` server setting is `log`, so this node gets a filter - one that
# enforces nothing - without being configured for one.
default_node = cluster.add_instance(
    "default_node", main_configs=["configs/binary_checksum.xml"]
)
disabled_node = cluster.add_instance(
    "disabled_node", main_configs=["configs/disabled.xml"]
)
errno_node = cluster.add_instance(
    "errno_node", main_configs=["configs/errno.xml", "configs/binary_checksum.xml"]
)
log_node = cluster.add_instance("log_node", main_configs=["configs/log.xml"])
trap_node = cluster.add_instance(
    "trap_node", main_configs=["configs/trap.xml", "configs/binary_checksum.xml"]
)

# `/proc/<pid>/status` reports the seccomp mode of a process: 0 is no filter, 2 is a BPF filter.
SECCOMP_MODE_DISABLED = "0"
SECCOMP_MODE_FILTER = "2"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [default_node, disabled_node, errno_node, log_node, trap_node]:
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
        "'mode String, getxattr_result String, clone_result String, clone3_result String, "
        "thread_result String')"
    ).split()


def test_setting_is_reported(started_cluster):
    for node, expected in [
        (default_node, "log"),
        (disabled_node, "disabled"),
        (errno_node, "errno"),
        (log_node, "log"),
        (trap_node, "trap"),
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
            f"for task in /proc/{pid}/task/*; do grep -hE '^(Seccomp|NoNewPrivs):' $task/status 2>/dev/null; "
            f"done | sort -u",
        ],
        user="root",
    )
    # `PR_SET_NO_NEW_PRIVS` is set on the installing thread only, but `TSYNC` carries it over to
    # every thread it synchronizes the filter to - so no thread may be left with `NoNewPrivs: 0`.
    assert modes.split() == ["NoNewPrivs:", "1", "Seccomp:", SECCOMP_MODE_FILTER]


def test_no_filter_when_disabled(started_cluster):
    pid = get_server_pid(disabled_node)
    assert get_status_field(disabled_node, pid, "Seccomp") == SECCOMP_MODE_DISABLED
    assert not disabled_node.contains_in_log("Applied a seccomp policy to this process")

    # Without a filter every call goes through to the kernel, which answers about the call itself:
    # about the attribute, which is missing (and which of the two answers it gives for that depends
    # on the filesystem), about the flags of the `clone`, which name a combination it rejects on its
    # own, and about the null `struct clone_args` of the `clone3`. None of those answers is `EPERM`
    # or `ENOSYS`.
    mode, getxattr_result, clone_result, clone3_result, thread_result = run_probe(
        disabled_node
    )
    assert mode == SECCOMP_MODE_DISABLED
    assert getxattr_result in ("ENODATA", "ENOTSUP", "EOPNOTSUPP")
    assert clone_result == "EINVAL"
    # A kernel too old for `clone3` answers `ENOSYS` by itself, and then the policy has nothing to
    # refuse either.
    assert clone3_result in ("EFAULT", "ENOSYS")
    assert thread_result == "OK"


def test_system_call_outside_the_policy_is_refused(started_cluster):
    # `getxattr` is not in the policy, so the `errno` mode turns it into `EPERM` - which is what
    # makes the filter more than a formality. A `clone` that asks for a user namespace is refused
    # by its flags, and `clone3`, whose arguments a filter cannot read, is refused as a whole with
    # `ENOSYS` - and making a thread keeps working, because `ENOSYS` is what sends the libc back to
    # `clone`.
    mode, getxattr_result, clone_result, clone3_result, thread_result = run_probe(
        errno_node
    )
    assert mode == SECCOMP_MODE_FILTER
    assert getxattr_result == "EPERM"
    assert clone_result == "EPERM"
    assert clone3_result == "ENOSYS"
    assert thread_result == "OK"


def test_log_mode_refuses_nothing(started_cluster):
    # The `log` mode installs a filter and has the kernel record what the policy does not allow,
    # but refuses nothing at all - so the probe sees exactly what it sees without a filter. What it
    # does share with the enforcing modes is `PR_SET_NO_NEW_PRIVS`, which the kernel asks for
    # before it accepts a filter.
    pid = get_server_pid(log_node)
    assert get_status_field(log_node, pid, "Seccomp") == SECCOMP_MODE_FILTER
    assert get_status_field(log_node, pid, "NoNewPrivs") == "1"

    mode, getxattr_result, clone_result, clone3_result, thread_result = run_probe(
        log_node
    )
    assert mode == SECCOMP_MODE_FILTER
    assert getxattr_result in ("ENODATA", "ENOTSUP", "EOPNOTSUPP")
    assert clone_result == "EINVAL"
    assert clone3_result in ("EFAULT", "ENOSYS")
    assert thread_result == "OK"


def test_server_works_under_the_filter(started_cluster):
    # A smoke test over the system calls a query needs: reading and writing files, spawning
    # threads, networking, timers. `errno_node` is the interesting one: if the policy were missing
    # something, the call would fail with `EPERM` instead of taking the server down, so the query
    # would report a strange error rather than losing the connection.
    for node in [trap_node, errno_node]:
        node.query("CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k")
        node.query("INSERT INTO t SELECT number, toString(number) FROM numbers(100000)")
        node.query("OPTIMIZE TABLE t FINAL")
        assert node.query("SELECT count(), uniqExact(s) FROM t") == "100000\t100000\n"
        assert node.query("SELECT count() > 0 FROM system.stack_trace") == "1\n"
        node.query("SYSTEM FLUSH LOGS")
        node.query("DROP TABLE t SYNC")


def test_binary_integrity_check_survives_the_filter(started_cluster):
    # The filter is installed before the integrity check of the executable, and `ptrace` is outside
    # the policy, so the check must not depend on it: the `trap` mode would take the server down on
    # the call, and the `errno` mode would turn a real checksum mismatch into the "run under
    # debugger" warning. The check now reads `TracerPid` instead, so it runs to the end in every
    # mode - as the log line it writes shows, on a server that is up and answering.
    # These nodes turn off `skip_binary_checksum_checks`, which the common configuration of the
    # integration tests turns on - otherwise the check would not run at all.
    for node in [trap_node, errno_node, default_node]:
        assert not node.contains_in_log("Binary checksum checks disabled")
        assert node.contains_in_log(
            "Integrity check of the executable successfully passed"
        ) or node.contains_in_log("Integrity check of the executable skipped")
        assert node.query("SELECT 1") == "1\n"
        assert not node.contains_in_log("is modified (most likely with breakpoints)")


def test_log_mode_starts_where_seccomp_is_unavailable(started_cluster):
    # The default `log` mode refuses nothing, so a kernel or a container runtime that cannot
    # install a filter must not stop the server from starting: it runs without one and says why.
    # This starts a second server in the container of `disabled_node` under an outer filter that
    # refuses the `seccomp` system call, which is what a restrictive container runtime does.
    node = disabled_node
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "scripts/refuse_seccomp.py"), "/refuse_seccomp.py"
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm -rf /tmp/seccomp_unavailable && mkdir -p /tmp/seccomp_unavailable && "
            "cat > /tmp/seccomp_unavailable/config.xml <<'EOF'\n"
            "<clickhouse>\n"
            "    <path>/tmp/seccomp_unavailable/</path>\n"
            "    <listen_host>127.0.0.1</listen_host>\n"
            "    <tcp_port>19123</tcp_port>\n"
            "    <users_config>/etc/clickhouse-server/users.xml</users_config>\n"
            "    <logger><log>/tmp/seccomp_unavailable/server.log</log><level>information</level></logger>\n"
            "    <skip_binary_checksum_checks>true</skip_binary_checksum_checks>\n"
            "    <seccomp>log</seccomp>\n"
            "</clickhouse>\n"
            "EOF",
        ],
        user="root",
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "CLICKHOUSE_WATCHDOG_ENABLE=0 python3 /refuse_seccomp.py /usr/bin/clickhouse server "
            "--config-file=/tmp/seccomp_unavailable/config.xml "
            "> /tmp/seccomp_unavailable/stdout.log 2>&1 & echo $! > /tmp/seccomp_unavailable/pid",
        ],
        user="root",
    )
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "for _ in $(seq 1 300); do "
                "grep -q 'Ready for connections' /tmp/seccomp_unavailable/server.log 2>/dev/null && exit 0; "
                "kill -0 $(cat /tmp/seccomp_unavailable/pid) || exit 1; sleep 0.5; done; exit 1",
            ],
            user="root",
        )
        log = node.exec_in_container(
            ["cat", "/tmp/seccomp_unavailable/server.log"], user="root"
        )
        assert "the `seccomp` system call cannot install a filter" in log
        assert "so the server is running without a seccomp policy" in log
        assert "Applied a seccomp policy to this process" not in log

        pid = node.exec_in_container(
            ["cat", "/tmp/seccomp_unavailable/pid"], user="root"
        ).strip()
        # The outer filter, not one of the server's own, and `PR_SET_NO_NEW_PRIVS` all the same.
        assert get_status_field(node, pid, "Seccomp") == SECCOMP_MODE_FILTER
        assert get_status_field(node, pid, "NoNewPrivs") == "1"
        assert (
            node.exec_in_container(
                ["clickhouse", "client", "--port", "19123", "--query", "SELECT 1"]
            )
            == "1\n"
        )
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "kill -9 $(cat /tmp/seccomp_unavailable/pid) 2>/dev/null; rm -rf /tmp/seccomp_unavailable",
            ],
            user="root",
        )

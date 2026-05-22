import os
import re
import socket
import subprocess
import time

import paramiko
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/server.xml", "keys/ssh_host_rsa_key"],
    user_configs=["configs/users.xml"],
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_simple_query_with_openssh_client(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "SELECT 1;"'

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    expected = instance.query("SELECT 1;")
    output = completed_process.stdout
    assert completed_process.returncode == 0
    assert output.replace("\n\x00", "\n") == expected


def test_no_queries_from_file(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -o SetEnv="max_threads=9999 format=JSONEachRow" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "\\i /etc/passwd"'

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # SUPPORT_IS_DISABLED error code is 344, exit code = 344 % 256 = 88
    assert completed_process.returncode == 88
    assert "SUPPORT_IS_DISABLED" in completed_process.stderr


def test_no_selects_into_outfile(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -o SetEnv="max_threads=9999 format=JSONEachRow" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "SELECT 1 INTO OUTFILE \'/tmp/result.tsv\';"'

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert "SUPPORT_IS_DISABLED" in completed_process.stderr


def test_no_inserts_from_infile(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f"ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -o SetEnv=\"max_threads=9999 format=JSONEachRow\" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 \"INSERT INTO function null('x UInt64') FROM INFILE '/etc/passwd';\""

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert "SUPPORT_IS_DISABLED" in completed_process.stderr


def test_create_table(started_cluster):
    def execute_command_and_get_output(command, input=""):
        # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
        ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "{command}"'

        completed_process = subprocess.run(
            ssh_command,
            shell=True,
            text=True,
            input = input,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output = completed_process.stdout
        return output

    execute_command_and_get_output("DROP TABLE IF EXISTS test;")
    execute_command_and_get_output(
        "CREATE TABLE test (a UInt64) ENGINE=MergeTree() ORDER BY a;"
    )
    execute_command_and_get_output("INSERT INTO test VALUES (1), (2), (3);")
    execute_command_and_get_output("INSERT INTO test FORMAT TSV", input="4\n5\n6")
    result = execute_command_and_get_output("SELECT * FROM test ORDER BY a;")
    assert result.replace("\n\x00", "\n") == "1\n2\n3\n4\n5\n6\n"
    execute_command_and_get_output("TRUNCATE test;")
    result = execute_command_and_get_output("SELECT * FROM test;")
    # Output should be empty
    assert result.replace("\x00", "") == ""
    execute_command_and_get_output("DROP TABLE IF EXISTS test;")


def test_simple_query_with_paramiko(started_cluster):
    pkey = paramiko.Ed25519Key.from_private_key_file(f"{SCRIPT_DIR}/keys/lucy_ed25519")
    client = paramiko.SSHClient()
    policy = paramiko.AutoAddPolicy()
    client.set_missing_host_key_policy(policy)
    client.connect(hostname=instance.ip_address, port=9022, username="lucy", pkey=pkey)

    stdin, stdout, stderr = client.exec_command("SELECT 1;")
    stdin.close()
    result = stdout.read().decode()
    expected = instance.query("SELECT 1;")
    assert result.replace("\n\x00", "\n") == expected

    # FIXME: If I'm trying to execute more queries with the same client I get the error:
    # Secsh channel 1 open FAILED: : Administratively prohibited

    client.close()

def test_no_password_user_with_openssh_client(started_cluster):
    # `no_password` users must be able to log in via SSH "none" authentication
    # without OpenSSH prompting for an empty password interactively.
    # `BatchMode=yes` disables any interactive prompt, so if the server does not
    # accept the "none" method, ssh will fail instead of hanging.
    ssh_command = (
        f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes "
        f"-o PreferredAuthentications=none "
        f"nobody@{instance.ip_address} -p 9022 \"SELECT 1;\""
    )

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    expected = instance.query("SELECT 1;")
    assert completed_process.returncode == 0, completed_process.stderr
    assert completed_process.stdout.replace("\n\x00", "\n") == expected


def test_empty_password_user_with_openssh_client(started_cluster):
    # A user with an explicit empty password (`IDENTIFIED WITH plaintext_password BY ''`)
    # is functionally equivalent to `no_password` and must also be accepted via the
    # SSH "none" method without an interactive prompt.
    instance.query(
        "CREATE USER OR REPLACE empty_pass IDENTIFIED WITH plaintext_password BY '';"
    )

    ssh_command = (
        f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes "
        f"-o PreferredAuthentications=none "
        f"empty_pass@{instance.ip_address} -p 9022 \"SELECT 1;\""
    )

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    expected = instance.query("SELECT 1;")
    assert completed_process.returncode == 0, completed_process.stderr
    assert completed_process.stdout.replace("\n\x00", "\n") == expected


def test_password_user_rejected_with_none_auth(started_cluster):
    # A user that has a password must NOT be authenticated via the SSH "none"
    # method. `BatchMode=yes` disables interactive prompts, and
    # `PreferredAuthentications=none` forces ssh to only try "none" — so the
    # connection must fail rather than fall through to a password prompt.
    instance.query("CREATE USER OR REPLACE mister IDENTIFIED BY 'P@$$WORD';")

    ssh_command = (
        f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes "
        f"-o PreferredAuthentications=none "
        f"mister@{instance.ip_address} -p 9022 \"SELECT 1;\""
    )

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    assert completed_process.returncode != 0
    assert "Permission denied" in completed_process.stderr


def test_paramiko_password(started_cluster):
    instance.query("CREATE USER OR REPLACE mister IDENTIFIED BY 'P@$$WORD';")

    pkey = paramiko.Ed25519Key.from_private_key_file(f"{SCRIPT_DIR}/keys/lucy_ed25519")
    client = paramiko.SSHClient()
    policy = paramiko.AutoAddPolicy()
    client.set_missing_host_key_policy(policy)
    client.connect(hostname=instance.ip_address, port=9022, username="mister", password='P@$$WORD')

    stdin, stdout, stderr = client.exec_command("SELECT 1;")
    stdin.close()
    result = stdout.read().decode()
    expected = instance.query("SELECT 1;")
    assert result.replace("\n\x00", "\n") == expected

    # FIXME: If I'm trying to execute more queries with the same client I get the error:
    # Secsh channel 1 open FAILED: : Administratively prohibited

    client.close()


def _server_open_files():
    pid = instance.get_process_pid("clickhouse")
    assert pid is not None, "could not locate clickhouse PID"
    out = instance.exec_in_container(
        ["bash", "-c", f"ls -1 /proc/{pid}/fd | wc -l"]
    )
    return int(out.strip())


def _hold_server_fds(port, count):
    sockets = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((instance.ip_address, port))
        sockets.append(s)
    return sockets


def test_ssh_interactive_pty_with_high_fds(started_cluster):
    """
    Regression test for the `select(2)` / `fd_set` overflow inside
    `replxx::Terminal::wait_for_input` when the embedded clickhouse-client is
    reached over SSH PTY in a process whose fd table extends past
    `FD_SETSIZE` (1024 on glibc).

    With the bug present, the PTY-slave fd handed to replxx ends up >= 1024;
    `select(nfds > FD_SETSIZE, ...)` returns -1/EINVAL on every iteration,
    `wait_for_input` busy-loops, and the interactive shell never observes
    user input — so the query result never comes back to the SSH peer. Under
    ASan the same code path trips a stack-buffer-overflow on the on-stack
    `fd_set`.

    The test asserts the positive behavior: an interactive PTY session with
    > 1024 fds open on the server returns the expected query result within a
    bounded time.
    """
    # Inflate the server-side fd count to well above FD_SETSIZE (1024) so the
    # fds opened by the new SSH session (TCP socket, PTY master/slave, libssh
    # internal pipes) are guaranteed to land at >= 1024. We pick a margin large
    # enough that the test is deterministic regardless of how many fds the
    # server has already allocated for its own internals, but small enough to
    # stay well under `max_connections` (bumped to 8192 in the test config).
    DUMMY_CONNECTIONS = 1500

    baseline_files = _server_open_files()
    keepalive = _hold_server_fds(9000, DUMMY_CONNECTIONS)
    try:
        # Sanity check: the server-process fd count rose by at least most of
        # our connections (allow some slack: a few connects may have been
        # rejected, ephemeral handler threads may have closed listener-side
        # fds, etc.).
        after_files = _server_open_files()
        delta = after_files - baseline_files
        assert delta >= DUMMY_CONNECTIONS * 9 // 10, (
            f"expected clickhouse-server fd count to grow by ~{DUMMY_CONNECTIONS}, "
            f"got delta={delta} (baseline={baseline_files}, after={after_files}); "
            "the fd-inflation step did not take effect"
        )

        # Interactive PTY session via paramiko `invoke_shell`. We cannot use
        # `exec_command` here: it is non-interactive and never reaches
        # `replxx::Terminal::wait_for_input`, which is the function the bug
        # lives in.
        pkey = paramiko.Ed25519Key.from_private_key_file(
            f"{SCRIPT_DIR}/keys/lucy_ed25519"
        )
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=instance.ip_address,
            port=9022,
            username="lucy",
            pkey=pkey,
            timeout=30,
        )
        try:
            channel = client.invoke_shell(term="xterm", width=80, height=24)
            channel.settimeout(20)
            try:
                # Drain the initial banner / prompt so the post-query output
                # is easy to identify. We don't assert on the banner content
                # (it changes across versions); break once the output has
                # been quiet for a short idle window.
                banner_deadline = time.time() + 10
                last_data = time.time()
                while time.time() < banner_deadline:
                    if channel.recv_ready():
                        channel.recv(65536)
                        last_data = time.time()
                    elif time.time() - last_data > 0.5:
                        break
                    else:
                        time.sleep(0.05)

                channel.sendall("SELECT 1 FORMAT TSV\n")

                # Wait for the query result. With the bug, `wait_for_input`
                # is spinning on EINVAL and no input is ever delivered to
                # the client, so the result never appears and we time out.
                #
                # `SELECT 1 FORMAT TSV` returns the bare line "1", followed
                # by the client footer `"<N> row in set. Elapsed: ..."`.
                # The query itself is echoed back by replxx (with ANSI
                # colors), so a naive `b"1" in buf` would match the echoed
                # query and produce a false positive even on a hung session.
                # The footer string is printed only after the result has
                # been emitted and never appears in echoed input, so use it
                # as the unambiguous completion marker. Strip ANSI escapes
                # before searching, since replxx interleaves SGR / cursor
                # codes throughout the output stream.
                ansi_re = re.compile(rb"\x1b\[[0-9;?]*[A-Za-z]")
                buf = b""
                got_result = False
                deadline = time.time() + 15
                while time.time() < deadline:
                    if channel.recv_ready():
                        buf += channel.recv(65536)
                        clean = ansi_re.sub(b"", buf)
                        if b"1 row in set" in clean:
                            got_result = True
                            break
                    else:
                        time.sleep(0.05)

                assert got_result, (
                    "interactive SSH PTY session did not return query result "
                    "within timeout — likely select(2)/fd_set overflow hang "
                    f"in replxx::Terminal::wait_for_input. raw output: {buf!r}"
                )
            finally:
                channel.close()
        finally:
            client.close()
    finally:
        for s in keepalive:
            try:
                s.close()
            except OSError:
                pass

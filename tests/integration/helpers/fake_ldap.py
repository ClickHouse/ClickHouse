import os
import time

HELPERS_DIR = os.path.dirname(os.path.realpath(__file__))
FAKE_LDAP_SERVER_SCRIPT = os.path.join(HELPERS_DIR, "fake_ldap_server.py")


def start_fake_ldap_server(node, port, mode, timeout=60):
    """Runs `fake_ldap_server.py` inside `node` on 127.0.0.1:`port` in `mode` (`bind` or `search`,
    see the script) and waits until it listens. Idempotent per port: a second start finds the port
    taken and exits, the first instance keeps serving, and the wait looks for the first
    `listening` line of the appended log."""
    log = f"/var/log/clickhouse-server/fake_ldap_server_{port}.log"
    node.copy_file_to_container(FAKE_LDAP_SERVER_SCRIPT, "/fake_ldap_server.py")
    node.exec_in_container(
        ["bash", "-c", f"python3 /fake_ldap_server.py {port} {mode} >> {log} 2>&1"],
        detach=True,
        user="root",
    )
    deadline = time.time() + timeout
    while True:
        listening = node.exec_in_container(
            ["bash", "-c", f"grep -c 'listening on {port} ' {log}"], nothrow=True
        )
        if listening.strip() not in ("", "0"):
            return
        assert (
            time.time() < deadline
        ), f"the fake LDAP server on port {port} did not start"
        time.sleep(0.5)

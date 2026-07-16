import os
import subprocess

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/server.xml", "keys/ssh_host_rsa_key", "configs/options_propagation.xml"],
    user_configs=["configs/users.xml"],
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def server_in_fips_mode():
    # MD5 is rejected with SUPPORT_IS_DISABLED under OpenSSL FIPS mode, using the
    # same isFIPSEnabled() signal that filters out Ed25519 SSH keys. If the query
    # succeeds the server is not in FIPS mode.
    try:
        instance.query("SELECT MD5('')")
        return False
    except Exception:
        return True


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        if server_in_fips_mode():
            # Both tests authenticate "lucy" with an Ed25519 key, which is not FIPS-approved:
            # UsersConfigParser drops that key on FIPS builds, so the user cannot log in.
            pytest.skip("Ed25519 SSH keys are not usable in FIPS mode")
        yield cluster

    finally:
        cluster.shutdown()

def test_options_and_settings_propagation(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null lucy@{instance.ip_address} -o SetEnv="max_threads=42 format=JSONEachRow" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "SELECT getSetting(\'max_threads\');"'

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output = completed_process.stdout
    assert completed_process.returncode == 0
    assert output.replace("\n\x00", "\n") == "{\"getSetting('max_threads')\":42}\n"

def test_no_server_logs_file(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null lucy@{instance.ip_address} -o SetEnv=\"server_logs_file='logs.log'\" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 \"SELECT * FROM system.numbers SETTINGS send_logs_level='trace' FORMAT Null;\""

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert "SUPPORT_IS_DISABLED" in completed_process.stderr

import os
import subprocess

import paramiko
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/server.xml", "keys/ssh_host_rsa_key", "configs/options_propagation.xml"],
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

def test_options_and_settings_propagation(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -o SetEnv="max_threads=42 format=JSONEachRow" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "SELECT getSetting(\'max_threads\');"'

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
    ssh_command = f"ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -o SetEnv=\"server_logs_file='logs.log'\" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 \"SELECT * FROM system.numbers SETTINGS send_logs_level='trace' FORMAT Null;\""

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert "SUPPORT_IS_DISABLED" in completed_process.stderr

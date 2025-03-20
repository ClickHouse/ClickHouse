import os
import subprocess

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
    assert output.replace("\n\x00", "\n") == expected


def test_options_and_settings_propagation(started_cluster):
    # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
    ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -o SetEnv="max_threads=9999 format=JSONEachRow" -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "SELECT getSetting(\'max_threads\');"'

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output = completed_process.stdout
    assert output.replace("\n\x00", "\n") == "{\"getSetting('max_threads')\":9999}\n"


def test_create_table(started_cluster):
    def execute_command_and_get_output(command):
        # StrictHostKeyChecking=no means we will not warn and ask to add a public key of a server to .known_hosts
        ssh_command = f'ssh -o StrictHostKeyChecking=no lucy@{instance.ip_address} -p 9022 -i {SCRIPT_DIR}/keys/lucy_ed25519 "{command}"'

        completed_process = subprocess.run(
            ssh_command,
            shell=True,
            text=True,
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
    result = execute_command_and_get_output("SELECT * FROM test;")
    assert result.replace("\n\x00", "\n") == "1\n2\n3\n"
    execute_command_and_get_output("TRUNCATE test;")
    result = execute_command_and_get_output("SELECT * FROM test;")
    # Output should be empty
    assert result.replace("\x00", "\n") == "\n"
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

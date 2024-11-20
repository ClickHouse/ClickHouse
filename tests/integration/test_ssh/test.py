import os
import subprocess

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


def test_simple_query_with_openssh_client():
    ssh_command = (
        "ssh  -o StrictHostKeyChecking"
        + f"=no lucy@{instance.ip_address} -p 9022"
        + f' -i {SCRIPT_DIR}/keys/lucy_ed25519 "select 1"'
    )

    completed_process = subprocess.run(
        ssh_command,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    expected = instance.query("select 1")

    output = completed_process.stdout

    assert output.replace("\n\x00", "\n") == expected

import pytest
import socket
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
import os

DOCKER_COMPOSE_PATH = get_docker_compose_path()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

ch_server = cluster.add_instance(
    "clickhouse-server",
    main_configs=["configs/config.xml", "configs/listen_host.xml"],
)

client = cluster.add_instance(
    "clickhouse-client",
)


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def stress_test(started_cluster):
    server_ip = cluster.get_instance_ip("clickhouse-server")
    client_ip = cluster.get_instance_ip("clickhouse-client")

    current_dir = os.path.dirname(__file__)
    client.copy_file_to_container(
        os.path.join(current_dir, "scripts", "stress_test.py"), "stress_test.py"
    )

    client.exec_in_container(["python3", f"stress_test.py", client_ip, server_ip])

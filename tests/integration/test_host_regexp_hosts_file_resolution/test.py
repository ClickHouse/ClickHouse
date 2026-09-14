import os

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

ch_server = cluster.add_instance(
    "clickhouse-server",
    main_configs=["configs/listen_host.xml"],
    user_configs=["configs/host_regexp.xml"],
)

client = cluster.add_instance(
    "clickhouse-client",
)


def build_endpoint_v4(ip):
    return f"'http://{ip}:8123/?query=SELECT+1&user=test_dns'"


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_host_regexp_multiple_ptr_hosts_file_v4(started_cluster):
    server_ip = cluster.get_instance_ip("clickhouse-server")
    client_ip = cluster.get_instance_ip("clickhouse-client")

    ch_server.exec_in_container(
        (["bash", "-c", f"echo '{client_ip} test1.example.com' > /etc/hosts"])
    )

    endpoint = build_endpoint_v4(server_ip)

    assert "1\n" == client.exec_in_container(["bash", "-c", f"curl {endpoint}"])

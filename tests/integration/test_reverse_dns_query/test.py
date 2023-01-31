import pytest
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
from time import sleep
import os

DOCKER_COMPOSE_PATH = get_docker_compose_path()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

ch_server = cluster.add_instance(
    "clickhouse-server",
    with_coredns=True,
    main_configs=["configs/listen_host.xml"],
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


def setup_ch_server(dns_server_ip):
    ch_server.exec_in_container(
        (["bash", "-c", f"echo 'nameserver {dns_server_ip}' > /etc/resolv.conf"])
    )
    ch_server.exec_in_container(
        (["bash", "-c", "echo 'options ndots:0' >> /etc/resolv.conf"])
    )
    ch_server.query("SYSTEM DROP DNS CACHE")


def test_host_regexp_multiple_ptr_v4(started_cluster):
    dns_server_ip = cluster.get_instance_ip(cluster.coredns_host)

    setup_ch_server(dns_server_ip)

    for _ in range(0, 300):
        response = client.query("select reverseDNSQuery('2001:4860:4860::8888')")
        assert response == "['dns.google']\n"

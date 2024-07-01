import pytest
import socket
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
from time import sleep
import os

DOCKER_COMPOSE_PATH = get_docker_compose_path()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

ch_server = cluster.add_instance(
    "clickhouse-server",
    with_coredns=True,
    main_configs=[
        "configs/config.xml",
        "configs/reverse_dns_function.xml",
        "configs/listen_host.xml",
    ],
)


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def check_ptr_record(ip, hostname):
    try:
        host, aliaslist, ipaddrlist = socket.gethostbyaddr(ip)
        if hostname.lower() == host.lower():
            return True
    except socket.herror:
        pass
    return False


def setup_dns_server(ip):
    domains_string = "test.example.com"
    example_file_path = f'{ch_server.env_variables["COREDNS_CONFIG_DIR"]}/example.com'
    run_and_check(f"echo '{ip} {domains_string}' > {example_file_path}", shell=True)

    # DNS server takes time to reload the configuration.
    for try_num in range(10):
        if all(check_ptr_record(ip, host) for host in domains_string.split()):
            break
        sleep(1)


def setup_ch_server(dns_server_ip):
    ch_server.exec_in_container(
        (["bash", "-c", f"echo 'nameserver {dns_server_ip}' > /etc/resolv.conf"])
    )
    ch_server.exec_in_container(
        (["bash", "-c", "echo 'options ndots:0' >> /etc/resolv.conf"])
    )
    ch_server.query("SYSTEM DROP DNS CACHE")


def test_reverse_dns_query(started_cluster):
    dns_server_ip = cluster.get_instance_ip(cluster.coredns_host)
    random_ipv6 = "4ae8:fa0f:ee1d:68c5:0b76:1b79:7ae6:1549"  # https://commentpicker.com/ip-address-generator.php
    setup_dns_server(random_ipv6)
    setup_ch_server(dns_server_ip)

    for _ in range(0, 200):
        response = ch_server.query(f"select reverseDNSQuery('{random_ipv6}')")
        assert response == "['test.example.com']\n"

import os
import socket
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

ch_server = cluster.add_instance(
    "clickhouse-server",
    with_coredns=True,
    main_configs=["configs/listen_host.xml"],
    user_configs=["configs/host_regexp.xml"],
    ipv6_address="2001:3984:3989::1:1111",
)

client = cluster.add_instance(
    "clickhouse-client",
    ipv6_address="2001:3984:3989::1:1112",
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
    domains_string = "test3.example.com test2.example.com test1.example.com"
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


def build_endpoint_v4(ip):
    return f"'http://{ip}:8123/?query=SELECT+1&user=test_dns'"


def build_endpoint_v6(ip):
    return build_endpoint_v4(f"[{ip}]")


def test_host_regexp_multiple_ptr_v4_fails_with_wrong_resolution(started_cluster):
    server_ip = cluster.get_instance_ip("clickhouse-server")
    random_ip = "9.9.9.9"
    dns_server_ip = cluster.get_instance_ip(cluster.coredns_host)

    setup_dns_server(random_ip)
    setup_ch_server(dns_server_ip)

    endpoint = build_endpoint_v4(server_ip)

    assert "1\n" != client.exec_in_container(["bash", "-c", f"curl {endpoint}"])


def test_host_regexp_multiple_ptr_v4(started_cluster):
    server_ip = cluster.get_instance_ip("clickhouse-server")
    client_ip = cluster.get_instance_ip("clickhouse-client")
    dns_server_ip = cluster.get_instance_ip(cluster.coredns_host)

    setup_dns_server(client_ip)
    setup_ch_server(dns_server_ip)

    endpoint = build_endpoint_v4(server_ip)

    assert "1\n" == client.exec_in_container(["bash", "-c", f"curl {endpoint}"])


def test_host_regexp_multiple_ptr_v6(started_cluster):
    setup_dns_server(client.ipv6_address)
    setup_ch_server(cluster.get_instance_global_ipv6(cluster.coredns_host))

    endpoint = build_endpoint_v6(ch_server.ipv6_address)

    assert "1\n" == client.exec_in_container(["bash", "-c", f"curl -6 {endpoint}"])

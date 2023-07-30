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
    main_configs=["configs/config.xml", "configs/listen_host.xml"],
    user_configs=["configs/host_regexp.xml"],
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


def test_host_regexp_multiple_ptr_v4(started_cluster):
    server_ip = cluster.get_instance_ip("clickhouse-server")
    client_ip = cluster.get_instance_ip("clickhouse-client")
    dns_server_ip = cluster.get_instance_ip(cluster.coredns_host)

    setup_dns_server(client_ip)
    setup_ch_server(dns_server_ip)

    current_dir = os.path.dirname(__file__)
    client.copy_file_to_container(
        os.path.join(current_dir, "scripts", "stress_test.py"), "stress_test.py"
    )

    client.exec_in_container(["python3", f"stress_test.py", client_ip, server_ip])

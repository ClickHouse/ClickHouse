import socket
import time
from kazoo.client import KazooClient


def get_keeper_socket(cluster, node, port=9181):
    hosts = cluster.get_instance_ip(node.name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, port))
    return client


def send_4lw_cmd(cluster, node, cmd="ruok", port=9181):
    client = None
    try:
        client = get_keeper_socket(cluster, node, port)
        client.send(cmd.encode())
        data = client.recv(100_000)
        data = data.decode()
        return data
    finally:
        if client is not None:
            client.close()


NOT_SERVING_REQUESTS_ERROR_MSG = "This instance is not currently serving requests"


def wait_until_connected(cluster, node, port=9181, timeout=30.0):
    elapsed = 0.0

    while send_4lw_cmd(cluster, node, "mntr", port) == NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.1)
        elapsed += 0.1

        if elapsed >= timeout:
            raise Exception(
                f"{timeout}s timeout while waiting for {node.name} to start serving requests"
            )


def wait_until_quorum_lost(cluster, node, port=9181):
    while send_4lw_cmd(cluster, node, "mntr", port) != NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.1)


def wait_nodes(cluster, nodes):
    for node in nodes:
        wait_until_connected(cluster, node)


def is_leader(cluster, node, port=9181):
    stat = send_4lw_cmd(cluster, node, "stat", port)
    return "Mode: leader" in stat


def get_leader(cluster, nodes):
    for node in nodes:
        if is_leader(cluster, node):
            return node
    raise Exception("No leader in Keeper cluster.")


def get_fake_zk(cluster, node, timeout: float = 30.0) -> KazooClient:
    _fake = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake.start()
    return _fake


def get_config_str(zk: KazooClient) -> str:
    """
    Return decoded contents of /keeper/config node
    """
    return zk.get("/keeper/config")[0].decode("utf-8")


def wait_configs_equal(left_config: str, right_zk: KazooClient, timeout: float = 30.0):
    """
    Check whether get /keeper/config result in left_config is equal
    to get /keeper/config on right_zk ZK connection.
    """
    elapsed: float = 0.0
    while sorted(left_config.split("\n")) != sorted(
        get_config_str(right_zk).split("\n")
    ):
        time.sleep(1)
        elapsed += 1
        if elapsed >= timeout:
            raise Exception(
                f"timeout while checking nodes configs to get equal. "
                f"Left: {left_config}, right: {get_config_str(right_zk)}"
            )

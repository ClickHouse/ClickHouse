import socket
import time


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


def wait_until_connected(cluster, node, port=9181):
    while send_4lw_cmd(cluster, node, "mntr", port) == NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.1)


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

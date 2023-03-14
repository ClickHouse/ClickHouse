import socket
import time
from helper.client import CommandRequest


def send_4lw_cmd(cluster, node, cmd="ruok", port=9181):
    return CommandRequest(
        ["cluster.server_bin_path", "keeper-client", f"{cluster.get_instance_ip(node.name)}:{port}", "-q", cmd]
    ).get_answer()


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

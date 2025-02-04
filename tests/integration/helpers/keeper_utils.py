import contextlib
import io
import logging
import re
import select
import socket
import subprocess
import time
from os import path as p
from typing import Iterable, List, Optional, Sequence, Union

from kazoo.client import KazooClient

from helpers.client import CommandRequest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

ss_established = [
    "ss",
    "--resolve",
    "--tcp",
    "--no-header",
    "state",
    "ESTABLISHED",
    "( dport = 2181 or sport = 2181 )",
]


def get_active_zk_connections(node: ClickHouseInstance) -> List[str]:
    return (
        str(node.exec_in_container(ss_established, privileged=True, user="root"))
        .strip()
        .split("\n")
    )


def get_zookeeper_which_node_connected_to(node: ClickHouseInstance) -> str:
    line = str(
        node.exec_in_container(ss_established, privileged=True, user="root")
    ).strip()

    pattern = re.compile(r"zoo[0-9]+", re.IGNORECASE)
    result = pattern.findall(line)
    assert (
        len(result) == 1
    ), "ClickHouse must be connected only to one Zookeeper at a time"
    assert isinstance(result[0], str)
    return result[0]


def execute_keeper_client_query(
    cluster: ClickHouseCluster, node: ClickHouseInstance, query: str
) -> str:
    request = CommandRequest(
        [
            cluster.server_bin_path,
            "keeper-client",
            "--host",
            str(cluster.get_instance_ip(node.name)),
            "--port",
            str(cluster.zookeeper_port),
            "-q",
            query,
        ],
        stdin="",
    )

    return request.get_answer()


class KeeperException(Exception):
    pass


class KeeperClient(object):
    SEPARATOR = b"\a\a\a\a\n"

    def __init__(self, bin_path: str, host: str, port: int, connection_tries=30):
        self.bin_path = bin_path
        self.host = host
        self.port = port

        retry_count = 0

        while True:
            try:
                self.proc = subprocess.Popen(
                    [
                        bin_path,
                        "keeper-client",
                        "--host",
                        host,
                        "--port",
                        str(port),
                        "--log-level",
                        "error",
                        "--tests-mode",
                        "--no-confirmation",
                    ],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                self.poller = select.epoll()
                self.poller.register(self.proc.stdout)
                self.poller.register(self.proc.stderr)

                self._fd_nums = {
                    self.proc.stdout.fileno(): self.proc.stdout,
                    self.proc.stderr.fileno(): self.proc.stderr,
                }

                self.stopped = False

                self.get("/keeper", 60.0)
                break
            except Exception as e:
                retry_count += 1
                if (
                    "All connection tries failed while connecting to ZooKeeper"
                    in str(e)
                    and retry_count < connection_tries
                ):
                    logging.debug(
                        "Got exception while connecting to Keeper: %s\nWill reconnect, reconnect count = %s",
                        e,
                        retry_count,
                    )
                    time.sleep(1)
                else:
                    raise

    def execute_query(self, query: str, timeout: float = 60.0) -> str:
        output = io.BytesIO()

        self.proc.stdin.write(query.encode() + b"\n")
        self.proc.stdin.flush()

        events = self.poller.poll(timeout)
        if not events:
            raise TimeoutError(f"Keeper client returned no output")

        for fd_num, event in events:
            if event & (select.EPOLLIN | select.EPOLLPRI):
                file = self._fd_nums[fd_num]

                if file == self.proc.stdout:
                    while True:
                        chunk = file.readline()
                        if chunk.endswith(self.SEPARATOR):
                            break

                        output.write(chunk)

                elif file == self.proc.stderr:
                    self.proc.stdout.readline()
                    raise KeeperException(self.proc.stderr.readline().strip().decode())

            else:
                raise ValueError(f"Failed to read from pipe. Flag {event}")

        data = output.getvalue().strip().decode()
        return data

    def cd(self, path: str, timeout: float = 60.0):
        self.execute_query(f"cd '{path}'", timeout)

    def ls(self, path: str, timeout: float = 60.0) -> list[str]:
        return self.execute_query(f"ls '{path}'", timeout).split(" ")

    def create(self, path: str, value: str, timeout: float = 60.0):
        self.execute_query(f"create '{path}' '{value}'", timeout)

    def get(self, path: str, timeout: float = 60.0) -> str:
        return self.execute_query(f"get '{path}'", timeout)

    def set(self, path: str, value: str, version: Optional[int] = None) -> None:
        self.execute_query(
            f"set '{path}' '{value}' {version if version is not None else ''}"
        )

    def rm(self, path: str, version: Optional[int] = None) -> None:
        self.execute_query(f"rm '{path}' {version if version is not None else ''}")

    def exists(self, path: str, timeout: float = 60.0) -> bool:
        return bool(int(self.execute_query(f"exists '{path}'", timeout)))

    def stop(self):
        if not self.stopped:
            self.stopped = True
            self.proc.communicate(b"exit\n", timeout=10.0)

    def sync(self, path: str, timeout: float = 60.0):
        self.execute_query(f"sync '{path}'", timeout)

    def touch(self, path: str, timeout: float = 60.0):
        self.execute_query(f"touch '{path}'", timeout)

    def find_big_family(self, path: str, n: int = 10, timeout: float = 60.0) -> str:
        return self.execute_query(f"find_big_family '{path}' {n}", timeout)

    def find_super_nodes(self, threshold: int, timeout: float = 60.0) -> str:
        return self.execute_query(f"find_super_nodes {threshold}", timeout)

    def get_direct_children_number(self, path: str, timeout: float = 60.0) -> str:
        return self.execute_query(f"get_direct_children_number '{path}'", timeout)

    def get_all_children_number(self, path: str, timeout: float = 60.0) -> str:
        return self.execute_query(f"get_all_children_number '{path}'", timeout)

    def delete_stale_backups(self, timeout: float = 60.0) -> str:
        return self.execute_query("delete_stale_backups", timeout)

    def reconfig(
        self,
        joining: Optional[str],
        leaving: Optional[str],
        new_members: Optional[str],
        timeout: float = 60.0,
    ) -> str:
        if bool(joining) + bool(leaving) + bool(new_members) != 1:
            raise ValueError(
                "Exactly one of joining, leaving or new_members must be specified"
            )

        if joining is not None:
            operation = "add"
        elif leaving is not None:
            operation = "remove"
        elif new_members is not None:
            operation = "set"
        else:
            raise ValueError(
                "At least one of joining, leaving or new_members must be specified"
            )

        return self.execute_query(
            f"reconfig {operation} '{joining or leaving or new_members}'", timeout
        )

    @classmethod
    @contextlib.contextmanager
    def from_cluster(
        cls, cluster: ClickHouseCluster, keeper_node: str, port: Optional[int] = None
    ) -> "KeeperClient":
        client = cls(
            cluster.server_bin_path,
            cluster.get_instance_ip(keeper_node),
            port or cluster.zookeeper_port,
        )

        try:
            yield client
        finally:
            client.stop()


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
    start = time.time()

    while send_4lw_cmd(cluster, node, "mntr", port) == NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.1)

        if time.time() - start > timeout:
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


def is_follower(cluster, node, port=9181):
    stat = send_4lw_cmd(cluster, node, "stat", port)
    return "Mode: follower" in stat


def get_leader(cluster, nodes):
    for node in nodes:
        if is_leader(cluster, node):
            return node
    raise Exception("No leader in Keeper cluster.")


def get_any_follower(cluster, nodes):
    for node in nodes:
        if is_follower(cluster, node):
            return node
    raise Exception("No followers in Keeper cluster.")


def get_fake_zk(cluster, node, timeout: float = 30.0) -> KazooClient:
    _fake = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake.start()
    return _fake


def get_config_str(zk: KeeperClient) -> str:
    """
    Return decoded contents of /keeper/config node
    """
    return zk.get("/keeper/config")


def wait_configs_equal(left_config: str, right_zk: KeeperClient, timeout: float = 30.0):
    """
    Check whether get /keeper/config result in left_config is equal
    to get /keeper/config on right_zk ZK connection.
    """
    start = time.time()
    left_config = sorted(left_config.split("\n"))
    while True:
        right_config = sorted(get_config_str(right_zk).split("\n"))
        if left_config == right_config:
            return

        time.sleep(1)
        if time.time() - start > timeout:
            raise Exception(
                f"timeout while checking nodes configs to get equal. "
                f"Left: {left_config}, right: {right_config}"
            )


def replace_zookeeper_config(
    nodes: Union[Sequence[ClickHouseInstance], ClickHouseInstance], new_config: str
) -> None:
    if not isinstance(nodes, Sequence):
        nodes = (nodes,)
    for node in nodes:
        node.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
        node.query("SYSTEM RELOAD CONFIG")


def reset_zookeeper_config(
    nodes: Union[Sequence[ClickHouseInstance], ClickHouseInstance],
    file_path: str = p.join(p.dirname(p.realpath(__file__)), "zookeeper_config.xml"),
) -> None:
    """Resets the keeper config to default or to a given path on the disk"""
    with open(file_path, "r", encoding="utf-8") as cf:
        replace_zookeeper_config(nodes, cf.read())

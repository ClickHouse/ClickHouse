import contextlib
import io
import logging
import re
import select
import socket
import subprocess
import time
import struct
from os import path as p
from typing import Iterable, List, Optional, Sequence, Union

from helpers.kazoo_client import KazooClientWithImplicitRetries
from kazoo.exceptions import ConnectionLoss, OperationTimeoutError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.client import EventType, KazooClient

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
    # In tests-mode, the keeper-client writes this separator to stdout after
    # each command so we can detect where one command's output ends.
    # Four BEL (0x07) characters were chosen because they never appear in
    # normal command output.
    SEPARATOR = b"\a\a\a\a\n"

    def __init__(
        self, bin_path: str, host: str, port: int, connection_tries=30, identity=None
    ):
        self.bin_path = bin_path
        self.host = host
        self.port = port

        retry_count = 0

        identity_arg = []
        if identity:
            identity_arg = ["--identity", identity]

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
                        *identity_arg,
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
        error = b""

        self.proc.stdin.write(query.encode() + b"\n")
        self.proc.stdin.flush()

        while True:
            events = self.poller.poll(timeout)
            if not events:
                raise TimeoutError(f"Keeper client returned no output")

            # Process stderr events before stdout to ensure errors are
            # collected before checking the stdout separator.
            for fd_num, event in events:
                if event & (select.EPOLLIN | select.EPOLLPRI):
                    file = self._fd_nums[fd_num]
                    if file == self.proc.stderr:
                        error += self.proc.stderr.readline()
                elif not (event & select.EPOLLHUP):
                    raise ValueError(f"Failed to read from pipe. Flag {event}")

            for fd_num, event in events:
                if event & (select.EPOLLIN | select.EPOLLPRI):
                    file = self._fd_nums[fd_num]
                    if file == self.proc.stdout:
                        while True:
                            chunk = file.readline()
                            if chunk.endswith(self.SEPARATOR):
                                if error:
                                    raise KeeperException(
                                        error.strip().decode()
                                    )
                                return output.getvalue().strip().decode()

                            output.write(chunk)

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

    def rmr(self, path: str) -> None:
        self.execute_query(f"rmr '{path}'")

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

    def get_acl(self, path: str, timeout: float = 60.0):
        return self.execute_query(f"get_acl '{path}'", timeout)

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
        cls,
        cluster: ClickHouseCluster,
        keeper_node: Optional[str] = None,
        keeper_ip: Optional[str] = None,
        port: Optional[int] = None,
        identity: Optional[str] = None,
    ) -> "KeeperClient":
        if keeper_node is None and keeper_ip is None:
            raise ValueError("Must specify either keeper_node or keeper_ip")

        instance_ip = keeper_ip
        if instance_ip is None:
            instance_ip = cluster.get_instance_ip(keeper_node)

        client = cls(
            cluster.server_bin_path,
            instance_ip,
            port or cluster.zookeeper_port,
            identity=identity,
        )

        try:
            yield client
        finally:
            client.stop()


def get_keeper_socket(cluster, nodename, port=9181, timeout_sec=60):
    host = cluster.get_instance_ip(nodename)
    client = socket.socket()
    client.settimeout(timeout_sec)
    client.connect((host, port))
    return client


def send_4lw_cmd(cluster, node, cmd="ruok", port=9181, argument=None, timeout_sec=60):
    client = None
    logging.debug("Sending %s to %s:%d", cmd, node, port)
    try:
        client = get_keeper_socket(cluster, node.name, port, timeout_sec)
        if argument is not None:
            client.send(
                cmd.encode() + struct.pack(">L", len(argument)) + argument.encode()
            )
        else:
            client.send(cmd.encode())

        data = client.recv(100_000)
        data = data.decode()
        return data
    finally:
        if client is not None:
            client.close()


NOT_SERVING_REQUESTS_ERROR_MSG = "This instance is not currently serving requests"


def wait_until_connected(
    cluster, node, port=9181, timeout=30.0, wait_complete_readiness=True, password=None
):
    start = time.time()

    logging.debug(
        "Waiting until keeper will be ready on %s:%d (timeout=%f)",
        node.name,
        port,
        timeout,
    )
    while send_4lw_cmd(cluster, node, "mntr", port) == NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.1)

        if time.time() - start > timeout:
            raise Exception(
                f"{timeout}s timeout while waiting for {node.name} to start serving requests"
            )

    if wait_complete_readiness:
        host = cluster.get_instance_ip(node.name)
        logging.debug(
            "Waiting until keeper can create sessions on %s:%d (timeout=%f)",
            host,
            port,
            timeout,
        )
        while True:
            zk_cli = None
            try:
                time_passed = time.time() - start
                if time_passed >= timeout:
                    raise Exception(
                        f"{timeout}s timeout while waiting for {node.name} to start serving requests"
                    )
                client_id = None
                if password is not None:
                    client_id = (0, password)

                zk_cli = KazooClient(
                    hosts=f"{host}:9181",
                    timeout=min(timeout - time_passed, 5.0),
                    client_id=client_id,
                )
                zk_cli.start()
                zk_cli.get("/keeper/api_version")
                break
            except (ConnectionLoss, OperationTimeoutError, KazooTimeoutError):
                pass
            finally:
                if zk_cli:
                    try:
                        # stop() can raise if the connection is already broken
                        zk_cli.stop()
                    except Exception:
                        pass
                    zk_cli.close()


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


def get_fake_zk(
    cluster, nodename, timeout: float = 30.0, password=None, retries=10, start=True
) -> KazooClientWithImplicitRetries:
    kazoo_retry = {
        "max_tries": retries,
    }

    client_id = None
    if password is not None:
        client_id = (0, password)

    _fake = KazooClientWithImplicitRetries(
        hosts=cluster.get_instance_ip(nodename) + ":9181",
        client_id=client_id,
        timeout=timeout,
        connection_retry=kazoo_retry,
        command_retry=kazoo_retry,
    )
    if start:
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


def is_znode_watch_event(event: EventType) -> bool:
    return event.type in [
        EventType.CREATED,
        EventType.DELETED,
        EventType.CHANGED,
        EventType.CHILD,
    ]

from helpers.client import CommandRequest
from helpers.cluster import ClickHouseCluster
from kazoo.exceptions import NodeExistsError
import logging


def quote_string(s):
    if type(s) == bytes:
        s = s.decode("unicode_escape")
    escaped = s.replace("'", "\\'")
    return f"'{escaped}'"


class KeeperClient:
    def __init__(self, cluster: ClickHouseCluster, instance):
        self.cluster = cluster
        self.instance = instance

    def query(self, query: str):
        args = [
            self.cluster.server_bin_path,
            "keeper-client",
            "-q",
            query,
        ]

        if self.cluster.with_foundationdb:
            args += ["--fdb", "--fdb-cluster", self.cluster.foundationdb_cluster]
        elif self.cluster.with_zookeeper:
            args += [
                "--host",
                str(self.cluster.get_instance_ip(self.instance)),
                "--port",
                str(self.cluster.zookeeper_port),
            ]
        elif self.cluster.with_zookeeper_secure:
            raise Exception("Secure ZooKeeper is not support")
        else:
            raise Exception("Cluster has no ZooKeeper")

        command = CommandRequest(args, stdin="")
        ans, err = command.get_answer_and_error()
        if err:
            if err.startswith("Node exists"):
                raise NodeExistsError()
            raise Exception(err)
        return ans

    def command(self, cmd):
        return self.query(cmd)

    def exists(self, path):
        stat_resp = self.query(f"exists {path}")
        return stat_resp.strip() == "1"

    def sync(self, path):
        return self.query(f"sync {path}")

    def create(self, path, value="default", makepath=False):
        opt_parent = "PARENT" if makepath else ""
        return self.query(f"create {path} {quote_string(value)} {opt_parent}")

    def get_children(self, path):
        return self.query(f"ls {path}").strip().split(" ")

    def delete(self, path, recursive=False):
        if recursive:
            self.query(f"rmr {path} force")
        else:
            self.query(f"rm {path}")

    def set(self, path, value):
        return self.query(f"set {path} {quote_string(value)}")

    def get(self, path):
        return self.query(f"get {path}")[:-1]

    def stop(self):
        return

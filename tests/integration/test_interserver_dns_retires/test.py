"""
This test makes sure interserver cluster queries handle invalid DNS
records for replicas.
"""

import multiprocessing.dummy
from contextlib import contextmanager

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance


def bootstrap(cluster: ClickHouseCluster):
    node: ClickHouseInstance
    for node in cluster.instances.values():
        node_number = int(node.name[-1])

        # getaddrinfo(...) may hang for a log time without these options.
        node.exec_in_container(
            [
                "bash",
                "-c",
                'echo -e "options timeout:1\noptions attempts:1" >> /etc/resolv.conf',
            ],
            privileged=True,
            user="root",
        )

        node.query(f"CREATE DATABASE IF NOT EXISTS r0")
        node.query(f"CREATE TABLE r0.test_data(v UInt64) ENGINE = Memory()")
        node.query(
            f"INSERT INTO r0.test_data SELECT number + {node_number} * 10 FROM numbers(10)"
        )
        node.query(
            f"""CREATE TABLE default.test AS r0.test_data ENGINE = Distributed(cluster_missing_replica, 'r0', test_data, rand())"""
        )


@contextmanager
def start_cluster():
    cluster = ClickHouseCluster(__file__)
    # node1 is missing on purpose to test DNS resolution errors.
    # It exists in configs/remote_servers.xml to create the failure condition.
    for node in ["node2", "node3", "node4"]:
        cluster.add_instance(node, main_configs=["configs/remote_servers.xml"])
    try:
        cluster.start()
        bootstrap(cluster)
        yield cluster
    finally:
        cluster.shutdown()


def test_query():
    with start_cluster() as cluster:
        n_queries = 16

        # thread-based pool
        p = multiprocessing.dummy.Pool(n_queries)

        def send_query(x):
            try:
                # queries start at operational shard 2, and will hit either the
                # 'normal' node2 or the missing node1 on shard 1.
                node = (
                    cluster.instances["node3"]
                    if (x % 2 == 0)
                    else cluster.instances["node4"]
                )
                # numbers between 0 and 19 are on the first ("broken") shard.
                # we need to make sure we're querying them successfully
                assert node.query(
                    "SELECT count() FROM default.test where v < (rand64() % 20)"
                )
                return 1
            except QueryRuntimeException as e:
                # DNS_ERROR because node1 doesn't exist.
                assert 198 == e.returncode
                # We shouldn't be getting here due to interserver retries.
                raise

        p.map(send_query, range(n_queries))

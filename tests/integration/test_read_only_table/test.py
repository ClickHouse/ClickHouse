import time
import re

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


def fill_nodes(nodes):
    for node in nodes:
        node.query(
            f"""
                CREATE TABLE test_table(a UInt64)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated', '{node.name}') ORDER BY tuple();
            """
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)
node3 = cluster.add_instance("node3", with_zookeeper=True)
nodes = [node1, node2, node3]


def sync_replicas(table):
    for node in nodes:
        node.query(f"SYSTEM SYNC REPLICA {table}")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node1, node2, node3])

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_restart_zookeeper(start_cluster):

    node1.query("INSERT INTO test_table VALUES (1), (2), (3), (4), (5);")

    def get_zookeeper_which_node_connected_to(node):
        line = str(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "lsof -a -i4 -i6 -itcp -w | grep 2181 | grep ESTABLISHED",
                ],
                privileged=True,
                user="root",
            )
        ).strip()

        pattern = re.compile(r"zoo[0-9]+", re.IGNORECASE)
        result = pattern.findall(line)
        assert (
            len(result) == 1
        ), "ClickHouse must be connected only to one Zookeeper at a time"
        return result[0]

    node1_zk = get_zookeeper_which_node_connected_to(node1)

    # ClickHouse should +- immediately reconnect to another zookeper node
    cluster.stop_zookeeper_nodes([node1_zk])
    time.sleep(10)

    node1.query("INSERT INTO test_table VALUES (6), (7), (8), (9), (10);")

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import get_zookeeper_which_node_connected_to
from helpers.test_tools import assert_eq_with_retry

NUM_TABLES = 10


def fill_nodes(nodes):
    for table_id in range(NUM_TABLES):
        for node in nodes:
            node.query(
                f"""
                    CREATE TABLE test_table_{table_id}(a UInt64)
                    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated/{table_id}', '{node.name}') ORDER BY tuple();
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

        fill_nodes(nodes)

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_restart_zookeeper(start_cluster):
    for table_id in range(NUM_TABLES):
        node1.query(
            f"INSERT INTO test_table_{table_id} VALUES (1), (2), (3), (4), (5);"
        )

    logging.info("Inserted test data and initialized all tables")

    node1_zk = get_zookeeper_which_node_connected_to(node1)

    # ClickHouse should +- immediately reconnect to another zookeeper node
    cluster.stop_zookeeper_nodes([node1_zk])
    time.sleep(5)

    for table_id in range(NUM_TABLES):
        node1.query_with_retry(
            sql=f"INSERT INTO test_table_{table_id} VALUES (6), (7), (8), (9), (10);",
            retry_count=10,
            sleep_time=1,
        )
    # restore the cluster state
    cluster.start_zookeeper_nodes([node1_zk])

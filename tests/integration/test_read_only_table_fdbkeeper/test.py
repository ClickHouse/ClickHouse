import time
import re
import logging

import pytest
from helpers.cluster import ClickHouseCluster
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

    def get_zookeeper_which_node_connected_to(node):
        line = str(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "lsof -a -i4 -i6 -itcp -w | grep 4501 | grep ESTABLISHED",
                ],
                privileged=True,
                user="root",
            )
        ).strip()

        pattern = re.compile(r"foundationdb[0-9]+", re.IGNORECASE)
        result = pattern.findall(line)
        assert (
            len(result) == 1
        ), "ClickHouse must be connected only to one FDBkeeper at a time"
        return result[0]

    get_zookeeper_which_node_connected_to(node1)

    def replace_fdbkeeper_cluster(new_fdb_cluster):
        logging.debug(f"Use new fdb cluster: {new_fdb_cluster}")
        node1.replace_config("/tmp/fdb.cluster", new_fdb_cluster)
        node1.replace_in_config(
            "/etc/clickhouse-server/conf.d/fdb_config.xml",
            "\\/etc\\/foundationdb\\/fdb.cluster",
            "\\/tmp\\/fdb.cluster",
        )
        node1.query("SYSTEM RELOAD CONFIG")

    cluster.stop_fdb()
    cluster.switch_to_fdb2()
    replace_fdbkeeper_cluster(cluster.get_fdb2_cluster())
    time.sleep(5)

    for table_id in range(NUM_TABLES):
        node1.query_with_retry(
            sql=f"INSERT INTO test_table_{table_id} VALUES (6), (7), (8), (9), (10);",
            retry_count=120,
            sleep_time=0.5,
        )

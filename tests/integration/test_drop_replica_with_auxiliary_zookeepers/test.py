import time

import pytest

import helpers.client as client
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))


def test_drop_replica_in_auxiliary_zookeeper(started_cluster):
    drop_table([node1, node2], "test_auxiliary_zookeeper")
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE test_auxiliary_zookeeper(a Int32)
                ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/test/test_auxiliary_zookeeper', '{replica}')
                ORDER BY a;
            """.format(
                replica=node.name
            )
        )

    # stop node2 server
    node2.stop_clickhouse()
    time.sleep(5)

    # check is_active
    retries = 0
    max_retries = 5
    zk = cluster.get_kazoo_client("zoo1")
    while True:
        if (
            zk.exists(
                "/clickhouse/tables/test/test_auxiliary_zookeeper/replicas/node2/is_active"
            )
            is None
        ):
            break
        else:
            retries += 1
            if retries > max_retries:
                raise Exception("Failed to stop server.")
            time.sleep(1)

    # drop replica node2
    node1.query("SYSTEM DROP REPLICA 'node2'")

    assert zk.exists("/clickhouse/tables/test/test_auxiliary_zookeeper")
    assert (
        zk.exists("/clickhouse/tables/test/test_auxiliary_zookeeper/replicas/node2")
        is None
    )

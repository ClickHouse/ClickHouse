import random
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

"""
Both ssl_conf.xml and no_ssl_conf.xml have the same port
"""


def _fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                CREATE DATABASE test;
    
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
            """.format(
                shard=shard, replica=node.name
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def both_https_cluster():
    try:
        cluster.start()

        _fill_nodes([node1, node2], 1)

        yield cluster

    finally:
        cluster.shutdown()


def test_both_https(both_https_cluster):
    node1.query("insert into test_table values ('2017-06-16', 111, 0)")

    assert_eq_with_retry(node1, "SELECT id FROM test_table order by id", "111")
    assert_eq_with_retry(node2, "SELECT id FROM test_table order by id", "111")

    node2.query("insert into test_table values ('2017-06-17', 222, 1)")

    assert_eq_with_retry(node1, "SELECT id FROM test_table order by id", "111\n222")
    assert_eq_with_retry(node2, "SELECT id FROM test_table order by id", "111\n222")


def test_replication_after_partition(both_https_cluster):
    node1.query("truncate table test_table")
    node2.query("truncate table test_table")

    manager = PartitionManager()

    def close(num):
        manager.partition_instances(node1, node2, port=9010)
        time.sleep(1)
        manager.heal_all()

    def insert_data_and_check(num):
        node1.query("insert into test_table values('2019-10-15', {}, 888)".format(num))
        time.sleep(0.5)

    closing_pool = Pool(1)
    inserting_pool = Pool(5)
    cres = closing_pool.map_async(close, [random.randint(1, 3) for _ in range(10)])
    ires = inserting_pool.map_async(insert_data_and_check, list(range(100)))

    cres.wait()
    ires.wait()

    assert_eq_with_retry(node1, "SELECT count() FROM test_table", "100")
    assert_eq_with_retry(node2, "SELECT count() FROM test_table", "100")


node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml", "configs/no_ssl_conf.xml"],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml", "configs/no_ssl_conf.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def both_http_cluster():
    try:
        cluster.start()

        _fill_nodes([node3, node4], 2)

        yield cluster

    finally:
        cluster.shutdown()


def test_both_http(both_http_cluster):
    node3.query("insert into test_table values ('2017-06-16', 111, 0)")

    assert_eq_with_retry(node3, "SELECT id FROM test_table order by id", "111")
    assert_eq_with_retry(node4, "SELECT id FROM test_table order by id", "111")

    node4.query("insert into test_table values ('2017-06-17', 222, 1)")

    assert_eq_with_retry(node3, "SELECT id FROM test_table order by id", "111\n222")
    assert_eq_with_retry(node4, "SELECT id FROM test_table order by id", "111\n222")


node5 = cluster.add_instance(
    "node5",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    with_zookeeper=True,
)
node6 = cluster.add_instance(
    "node6",
    main_configs=["configs/remote_servers.xml", "configs/no_ssl_conf.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def mixed_protocol_cluster():
    try:
        cluster.start()

        _fill_nodes([node5, node6], 3)

        yield cluster

    finally:
        cluster.shutdown()


def test_mixed_protocol(mixed_protocol_cluster):
    node5.query("insert into test_table values ('2017-06-16', 111, 0)")

    assert_eq_with_retry(node5, "SELECT id FROM test_table order by id", "111")
    assert_eq_with_retry(node6, "SELECT id FROM test_table order by id", "")

    node6.query("insert into test_table values ('2017-06-17', 222, 1)")

    assert_eq_with_retry(node5, "SELECT id FROM test_table order by id", "111")
    assert_eq_with_retry(node6, "SELECT id FROM test_table order by id", "222")

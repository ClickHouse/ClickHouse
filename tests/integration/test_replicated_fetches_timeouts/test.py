#!/usr/bin/env python3

import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    'node1', with_zookeeper=True,
    main_configs=['configs/server.xml'])

node2 = cluster.add_instance(
    'node2', with_zookeeper=True,
    main_configs=['configs/server.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def test_no_stall(started_cluster):
    for instance in started_cluster.instances.values():
        instance.query("""
            CREATE TABLE t (key UInt64, data String)
            ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '{instance}')
                ORDER BY tuple()
                PARTITION BY key""")

    # Pause node3 until the test setup is prepared
    node2.query("SYSTEM STOP FETCHES t")

    node1.query("INSERT INTO t SELECT 1, '{}' FROM numbers(500)".format(get_random_string(104857)))
    node1.query("INSERT INTO t SELECT 2, '{}' FROM numbers(500)".format(get_random_string(104857)))

    with PartitionManager() as pm:
        pm.add_network_delay(node1, 2000)
        node2.query("SYSTEM START FETCHES t")

        # Wait for timeout exceptions to confirm that timeout is triggered.
        while True:
            conn_timeout_exceptions = int(node2.query(
                """
                SELECT count()
                FROM system.replication_queue
                WHERE last_exception LIKE '%connect timed out%'
                """))

            if conn_timeout_exceptions >= 2:
                break

            time.sleep(0.1)

        print("Connection timeouts tested!")

        # Increase connection timeout and wait for receive timeouts.
        node2.query("""
            ALTER TABLE t
                MODIFY SETTING replicated_fetches_http_connection_timeout = 30,
                    replicated_fetches_http_receive_timeout = 1""")

        while True:
            timeout_exceptions = int(node2.query(
                """
                SELECT count()
                FROM system.replication_queue
                WHERE last_exception LIKE '%e.displayText() = Timeout%'
                    AND last_exception NOT LIKE '%connect timed out%'
                """).strip())

            if timeout_exceptions >= 2:
                break

            time.sleep(0.1)

    for instance in started_cluster.instances.values():
        # Workaround for DROP TABLE not finishing if it is started while table is readonly.
        instance.query("SYSTEM RESTART REPLICA t")

        # Cleanup data directory from test results archive.
        instance.query("DROP TABLE t SYNC")

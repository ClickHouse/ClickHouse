import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.wait_for_helpers import (
    wait_for_delete_empty_parts,
    wait_for_delete_inactive_parts,
)

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_numbers_of_detached_parts(started_cluster):
    cluster.start()
    query_create = """
    CREATE TABLE t
    (
       id Int64,
       event_time Date
    )
    Engine=MergeTree()
    PARTITION BY toYYYYMMDD(event_time)
    ORDER BY id;
    """
    node1.query(query_create)

    # gives us 2 partitions with 3 parts in total
    node1.query("INSERT INTO t VALUES (1, toDate('2022-09-01'));")
    node1.query("INSERT INTO t VALUES (2, toDate('2022-08-29'));")
    node1.query("INSERT INTO t VALUES (3, toDate('2022-09-01'));")

    query_number_detached_parts_in_async_metric = """
    SELECT value
    FROM system.asynchronous_metrics
    WHERE metric LIKE 'NumberOfDetachedParts';
    """
    query_number_detached_by_user_parts_in_async_metric = """
    SELECT value
    FROM system.asynchronous_metrics
    WHERE metric LIKE 'NumberOfDetachedByUserParts';
    """
    query_count_active_parts = """
    SELECT count(*) FROM system.parts WHERE table = 't' AND active
    """
    query_count_detached_parts = """
    SELECT count(*) FROM system.detached_parts WHERE table = 't'
    """

    query_one_partition_name = """
    SELECT name FROM system.parts WHERE table = 't' AND active AND partition = '20220829'
    """
    partition_name = node1.query(query_one_partition_name).strip()

    assert 0 == int(node1.query(query_count_detached_parts))
    assert 3 == int(node1.query(query_count_active_parts))
    assert 0 == int(node1.query(query_number_detached_parts_in_async_metric))
    assert 0 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # detach some parts and wait until asynchronous metrics notice it
    node1.query("ALTER TABLE t DETACH PARTITION '20220901';")
    wait_for_delete_empty_parts(node1, "t")

    assert 2 == int(node1.query(query_count_detached_parts))
    assert 1 == int(node1.query(query_count_active_parts))

    assert_eq_with_retry(
        node1,
        query_number_detached_parts_in_async_metric,
        "2\n",
    )
    assert 2 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # detach the rest parts and wait until asynchronous metrics notice it
    node1.query("ALTER TABLE t DETACH PARTITION ALL")
    wait_for_delete_empty_parts(node1, "t")

    assert 3 == int(node1.query(query_count_detached_parts))
    assert 0 == int(node1.query(query_count_active_parts))

    assert_eq_with_retry(
        node1,
        query_number_detached_parts_in_async_metric,
        "3\n",
    )
    assert 3 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # inject some data directly and wait until asynchronous metrics notice it
    node1.exec_in_container(
        [
            "bash",
            "-c",
            "mkdir /var/lib/clickhouse/data/default/t/detached/unexpected_all_0_0_0",
        ]
    )

    assert 4 == int(node1.query(query_count_detached_parts))
    assert 0 == int(node1.query(query_count_active_parts))

    assert_eq_with_retry(
        node1,
        query_number_detached_parts_in_async_metric,
        "4\n",
    )
    assert 3 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # drop some data directly and wait asynchronous metrics notice it
    node1.exec_in_container(
        [
            "bash",
            "-c",
            "rm -rf /var/lib/clickhouse/data/default/t/detached/{}".format(
                partition_name
            ),
        ]
    )

    assert 3 == int(node1.query(query_count_detached_parts))
    assert 0 == int(node1.query(query_count_active_parts))

    assert_eq_with_retry(
        node1,
        query_number_detached_parts_in_async_metric,
        "3\n",
    )
    assert 2 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

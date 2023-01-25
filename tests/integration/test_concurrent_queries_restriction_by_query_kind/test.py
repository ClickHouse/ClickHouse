import time
from multiprocessing.dummy import Pool

import pytest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node_insert = cluster.add_instance(
    "node_insert", main_configs=["configs/concurrent_insert_restriction.xml"]
)
node_select = cluster.add_instance(
    "node_select", main_configs=["configs/concurrent_select_restriction.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node_select.query(
            "create table test_concurrent_insert (x UInt64) ENGINE = MergeTree() order by tuple()"
        )
        node_insert.query(
            "create table test_concurrent_insert (x UInt64) ENGINE = MergeTree() order by tuple()"
        )
        yield cluster
    finally:
        cluster.shutdown()


def execute_with_background(node, sql, background_sql, background_times, wait_times=3):
    r = None
    for _ in range(wait_times):
        r = node.query("show processlist", stdin="")
        if not r.strip():
            break
        time.sleep(1)
    else:
        assert False, "there are unknown background queries: {}".format(r)
    for _ in range(background_times):
        node.get_query_request(background_sql, stdin="")
    time.sleep(0.5)  # wait background to start.
    return node.query(sql, stdin="")


def common_pattern(node, query_kind, restricted_sql, normal_sql, limit, wait_times):
    # restriction is working
    with pytest.raises(
        Exception, match=r".*Too many simultaneous {} queries.*".format(query_kind)
    ):
        execute_with_background(node, restricted_sql, restricted_sql, limit, wait_times)

    # different query kind is independent
    execute_with_background(node, normal_sql, restricted_sql, limit, wait_times)

    # normal
    execute_with_background(node, restricted_sql, "", 0, wait_times)


def test_select(started_cluster):
    common_pattern(
        node_select,
        "select",
        "select sleep(3)",
        "insert into test_concurrent_insert values (0)",
        2,
        10,
    )

    # subquery is not counted
    execute_with_background(
        node_select,
        "select sleep(3)",
        "insert into test_concurrent_insert select sleep(3)",
        2,
        10,
    )


def test_insert(started_cluster):
    common_pattern(
        node_insert,
        "insert",
        "insert into test_concurrent_insert select sleep(3)",
        "select 1",
        2,
        10,
    )

import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_insert = cluster.add_instance(
    "node_insert", main_configs=["configs/concurrent_insert_restriction.xml"]
)
node_select1 = cluster.add_instance(
    "node_select1", main_configs=["configs/concurrent_select_restriction.xml"]
)

node_select2 = cluster.add_instance(
    "node_select2", main_configs=["configs/concurrent_select_restriction.xml"]
)

node_select3 = cluster.add_instance(
    "node_select3", main_configs=["configs/concurrent_select_restriction.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node_select1.query(
            "create table test_concurrent_insert (x UInt64) ENGINE = MergeTree() order by tuple()"
        )
        node_select2.query(
            "create table test_concurrent_insert (x UInt64) ENGINE = MergeTree() order by tuple()"
        )
        node_select3.query(
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

    query_results = []
    for _ in range(background_times):
        query_results.append(node.get_query_request(background_sql, stdin=""))

    query_results.append(node.get_query_request(sql, stdin=""))
    return query_results


def common_pattern(node, query_kind, restricted_sql, normal_sql, limit, wait_times):
    query_results = execute_with_background(
        node, restricted_sql, restricted_sql, limit, wait_times
    )

    errors = [query_result.get_answer_and_error()[1] for query_result in query_results]
    assert (
        sum(1 for e in errors if f"Too many simultaneous {query_kind} queries" in e)
        == 1
    ), f"Expected exactly 1 query to fail because of too many simultaneous queries, got errors: {errors}"

    def assert_all_queries_passed(query_resuts):
        errors = [
            query_result.get_answer_and_error()[1] for query_result in query_results
        ]
        assert all(
            len(e) == 0 for e in errors
        ), f"Expected for all queries to pass, got errors: {errors}"

    # different query kind is independent
    query_results = execute_with_background(
        node, normal_sql, restricted_sql, limit, wait_times
    )
    assert_all_queries_passed(query_results)

    # normal
    query_results = execute_with_background(node, restricted_sql, "", 0, wait_times)
    assert_all_queries_passed(query_results)


def test_select(started_cluster):
    common_pattern(
        node_select1,
        "select",
        "select sleep(3)",
        "insert into test_concurrent_insert values (0)",
        2,
        10,
    )

    # subquery is not counted
    execute_with_background(
        node_select2,
        "select sleep(3)",
        "insert into test_concurrent_insert select sleep(3)",
        2,
        10,
    )

    # intersect and except are counted
    common_pattern(
        node_select3,
        "select",
        "select sleep(1) INTERSECT select sleep(1) EXCEPT select sleep(1)",
        "insert into test_concurrent_insert values (0)",
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

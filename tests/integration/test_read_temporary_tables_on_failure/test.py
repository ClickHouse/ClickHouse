import uuid

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_different_versions(start_cluster):
    # A query keeps its slot only while its client is connected, so the slot is held by a running request.
    query_id = uuid.uuid4().hex
    long_query = node.get_query_request(
        "SELECT sleepEachRow(3) FROM numbers(200) SETTINGS function_sleep_max_microseconds_per_block = 0",
        query_id=query_id,
    )
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'",
        "1",
        retry_count=120,
    )

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT 1", settings={"max_concurrent_queries_for_user": 1})

    node.query(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
    long_query.get_error()

    assert node.contains_in_log("Too many simultaneous queries for user")
    assert not node.contains_in_log("Unknown packet")
    assert not node.contains_in_log("Unexpected packet")

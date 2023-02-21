import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_usage():
    node.query(
        "CREATE TABLE async_table(data Array(UInt64)) ENGINE=MergeTree() ORDER BY data"
    )

    response = node.get_query_request(
        "SELECT groupArray(number + sleepEachRow(0.0001)) FROM numbers(1000000) SETTINGS max_memory_usage_for_user={}".format(30 * (2 ** 23))
    )

    INSERT_QUERY = "INSERT INTO async_table SETTINGS async_insert=1, wait_for_async_insert=1 VALUES ({})"
    for i in range(10):
        node.query(INSERT_QUERY.format([i in range(i * 5000000, (i + 1) * 5000000)]))

    _, err = response.get_answer_and_error()
    assert err == "", "Query failed"

    node.query("DROP TABLE async_table")

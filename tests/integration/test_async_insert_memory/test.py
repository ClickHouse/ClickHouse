import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_usage():
    node.query("CREATE USER IF NOT EXISTS A")
    node.query("GRANT ALL ON *.* TO A")

    node.query(
        "CREATE TABLE async_table(data Array(UInt64)) ENGINE=MergeTree() ORDER BY data"
    )

    node.get_query_request("SELECT count() FROM system.numbers", user="A")

    INSERT_QUERY = "INSERT INTO async_table SETTINGS async_insert=1, wait_for_async_insert=1,async_insert_max_data_size=150000000 VALUES ({})"
    for iter in range(10):
        values = list(range(iter * 5000000, (iter + 1) * 5000000))
        node.query(INSERT_QUERY.format(values), user="A")

    # Wait until buffers are freed
    time.sleep(5)

    response = node.get_query_request(
        "SELECT groupArray(number) FROM numbers(1000000) SETTINGS max_memory_usage_for_user={}".format(
            30 * (2**23)
        ),
        user="A",
    )

    _, err = response.get_answer_and_error()
    assert err == "", "Query failed with error {}".format(err)

    node.query("DROP TABLE async_table")
    node.query("DROP USER IF EXISTS A")

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node", main_configs=["configs/global_overcommit_tracker.xml"]
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


TEST_QUERY_A = "SELECT number FROM numbers(1000) GROUP BY number SETTINGS max_guaranteed_memory_usage_for_user=1"
TEST_QUERY_B = "SELECT number FROM numbers(1000) GROUP BY number SETTINGS max_guaranteed_memory_usage_for_user=2"


def test_overcommited_is_killed():
    node.query("CREATE USER A")
    node.query("GRANT ALL ON *.* TO A")
    node.query("CREATE USER B")
    node.query("GRANT ALL ON *.* TO B")

    responses_A = list()
    responses_B = list()
    for _ in range(100):
        responses_A.append(node.get_query_request(TEST_QUERY_A, user="A"))
        responses_B.append(node.get_query_request(TEST_QUERY_B, user="B"))

    overcommited_killed = False
    for response in responses_A:
        _, err = response.get_answer_and_error()
        if "MEMORY_LIMIT_EXCEEDED" in err:
            overcommited_killed = True
    finished = False
    for response in responses_B:
        _, err = response.get_answer_and_error()
        if err == "":
            finished = True

    assert (
        overcommited_killed and finished
    ), "no overcommited task was killed or all tasks are killed"

    node.query("DROP USER IF EXISTS A")
    node.query("DROP USER IF EXISTS B")

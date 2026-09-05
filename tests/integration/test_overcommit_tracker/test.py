import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/config.xml"],
    user_configs=[
        "configs/users.d/users.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


USER_TEST_QUERY_A = "SELECT groupArray(number) FROM numbers(2500000) SETTINGS max_memory_usage_for_user=2000000000,memory_overcommit_ratio_denominator=1"
USER_TEST_QUERY_B = "SELECT groupArray(number) FROM numbers(2500000) SETTINGS max_memory_usage_for_user=2000000000,memory_overcommit_ratio_denominator=80000000"


def test_user_overcommit():
    node.query("CREATE USER IF NOT EXISTS A")
    node.query("GRANT ALL ON *.* TO A")

    responses_A = list()
    responses_B = list()
    for i in range(100):
        if i % 2 == 0:
            responses_A.append(node.get_query_request(USER_TEST_QUERY_A, user="A"))
        else:
            responses_B.append(node.get_query_request(USER_TEST_QUERY_B, user="A"))

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

    assert finished, "all tasks are killed"

    node.query("DROP USER IF EXISTS A")

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/global_overcommit_tracker.xml"],
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


GLOBAL_TEST_QUERY_A = "SELECT groupArray(number) FROM numbers(5000000) SETTINGS memory_overcommit_ratio_denominator_for_user=1"
GLOBAL_TEST_QUERY_B = "SELECT groupArray(number) FROM numbers(2500000) SETTINGS memory_overcommit_ratio_denominator_for_user=80000000"


def test_global_overcommit():
    # NOTE: another option is to increase waiting time.
    if (
        node.is_built_with_thread_sanitizer()
        or node.is_built_with_address_sanitizer()
        or node.is_built_with_memory_sanitizer()
    ):
        pytest.skip("doesn't fit in memory limits")

    node.query("CREATE USER IF NOT EXISTS A")
    node.query("GRANT ALL ON *.* TO A")
    node.query("CREATE USER IF NOT EXISTS B")
    node.query("GRANT ALL ON *.* TO B")

    responses_A = list()
    responses_B = list()
    for i in range(50):
        responses_A.append(node.get_query_request(GLOBAL_TEST_QUERY_A, user="A"))
        responses_B.append(node.get_query_request(GLOBAL_TEST_QUERY_B, user="B"))

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

    assert overcommited_killed, "no overcommited task was killed"
    assert finished, "all tasks are killed"

    node.query("DROP USER IF EXISTS A")
    node.query("DROP USER IF EXISTS B")

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
    if node.is_built_with_memory_sanitizer() or node.is_built_with_address_sanitizer():
        pytest.skip(
            "doesn't fit in memory limits under sanitizers (memory overhead causes timeouts)"
        )

    node.query("CREATE USER IF NOT EXISTS A")
    node.query("GRANT ALL ON *.* TO A")

    # The test relies on memory pressure to selectively kill queries based on
    # memory_overcommit_ratio_denominator. This is nondeterministic, so retry.
    overcommitted_killed = False
    finished = False
    last_error = ""

    for attempt in range(3):
        responses_A = list()
        responses_B = list()
        for i in range(100):
            if i % 2 == 0:
                responses_A.append(
                    node.get_query_request(USER_TEST_QUERY_A, user="A")
                )
            else:
                responses_B.append(
                    node.get_query_request(USER_TEST_QUERY_B, user="A")
                )

        overcommitted_killed = False
        for response in responses_A:
            _, err = response.get_answer_and_error()
            if "MEMORY_LIMIT_EXCEEDED" in err:
                overcommitted_killed = True
        finished = False
        for response in responses_B:
            _, err = response.get_answer_and_error()
            if err == "":
                finished = True

        if overcommitted_killed and finished:
            break

        last_error = (
            f"attempt {attempt + 1}: overcommitted_killed={overcommitted_killed}, finished={finished}"
        )

    assert overcommitted_killed, f"overcommitted query is not killed ({last_error})"
    assert finished, f"all tasks are killed ({last_error})"

    node.query("DROP USER IF EXISTS A")

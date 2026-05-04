import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
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


# Use larger queries (128 MB each) and a lower user memory limit (300 MB) so that
# memory pressure is reached even when only a handful of queries overlap in time.
# This prevents flakiness on slow builds (coverage, ARM) where fewer queries run
# concurrently and the old 2 GB limit was rarely hit.
USER_TEST_QUERY_A = "SELECT groupArray(number) FROM numbers(5000000) SETTINGS max_threads=1, max_memory_usage_for_user=300000000, memory_overcommit_ratio_denominator=1"
USER_TEST_QUERY_B = "SELECT groupArray(number) FROM numbers(5000000) SETTINGS max_threads=1, max_memory_usage_for_user=300000000, memory_overcommit_ratio_denominator=80000000"

QUERY_COUNT = 40  # 20 × A + 20 × B


def test_user_overcommit():
    if (
        node.is_built_with_memory_sanitizer()
        or node.is_built_with_address_sanitizer()
        or node.is_built_with_thread_sanitizer()
    ):
        pytest.skip(
            "sanitizers inflate per-query memory 2-10x, making individual queries exceed the 300 MB user limit"
        )

    node.query("CREATE USER IF NOT EXISTS A")
    node.query("GRANT ALL ON *.* TO A")

    # The test relies on memory pressure to selectively kill queries based on
    # memory_overcommit_ratio_denominator. This is nondeterministic, so retry.
    overcommitted_killed = False
    finished = False
    last_error = ""

    for attempt in range(5):
        responses_A = list()
        responses_B = list()
        for i in range(QUERY_COUNT):
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

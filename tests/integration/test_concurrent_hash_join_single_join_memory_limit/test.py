import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/server.yaml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


JOIN_QUERY = """
EXPLAIN
SELECT count() FROM (SELECT number AS id, number AS val FROM numbers(1)) AS a
INNER JOIN (SELECT number AS id, number AS val FROM numbers(1)) AS b USING (id)
"""


def test_single_join_exceeding_limit_is_caught():
    error = instance.query_and_get_error(
        JOIN_QUERY,
        settings={
            "max_threads": 256,
            "join_algorithm": "parallel_hash",
            "max_memory_usage": "16Mi",
        },
    )
    assert "MEMORY_LIMIT_EXCEEDED" in error


def test_single_join_within_limit_succeeds():
    instance.query(
        JOIN_QUERY,
        settings={
            "max_threads": 256,
            "join_algorithm": "parallel_hash",
            "max_memory_usage": "512Mi",
        },
    )

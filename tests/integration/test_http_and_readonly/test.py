import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_http_get_is_readonly():
    assert "Cannot execute query in readonly mode" in instance.http_query_and_get_error(
        "CREATE TABLE xxx (a Date) ENGINE = MergeTree(a, a, 256)"
    )
    assert (
        "Cannot modify 'readonly' setting in readonly mode"
        in instance.http_query_and_get_error(
            "CREATE TABLE xxx (a Date) ENGINE = MergeTree(a, a, 256)",
            params={"readonly": 0},
        )
    )

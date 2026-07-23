import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_server = cluster.add_instance(
    "node_server", main_configs=["config.d/memory_overrides.yaml"]
)
node_user = cluster.add_instance(
    "node_user", user_configs=["users.d/memory_overrides.yaml"]
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "node",
    [
        pytest.param(node_server, id="server"),
        pytest.param(node_user, id="user"),
    ],
)
def test_max_bytes_ratio_before_external_group_by(node):
    if node.is_built_with_thread_sanitizer():
        pytest.skip("TSan build is skipped due to memory overhead")

    # Peak memory usage: 15-16GiB
    query = """
    SELECT
        uniqExact(number::String),
        uniqExact((number,number))
    FROM numbers(100e6) GROUP BY (number%1000)::String FORMAT Null
    """

    settings = {
        "max_memory_usage": "0",
        "max_bytes_before_external_group_by": 0,
        "max_bytes_ratio_before_external_group_by": 0.3,
    }
    node.query(query, settings=settings)

    settings["max_bytes_ratio_before_external_group_by"] = 0
    with pytest.raises(QueryRuntimeException):
        node.query(query, settings=settings)


@pytest.mark.parametrize(
    "node",
    [
        pytest.param(node_server, id="server"),
        pytest.param(node_user, id="user"),
    ],
)
def test_max_bytes_ratio_before_external_sort(node):
    if node.is_built_with_thread_sanitizer():
        pytest.skip("TSan build is skipped due to memory overhead")

    # Peak memory usage: 12GiB (each column in ORDER BY eats ~2GiB)
    query = """
    SELECT number FROM numbers(100e6) ORDER BY (
        number::String,
        (number+1)::String,
        (number+2)::String,
        (number+3)::String,
        (number+4)::String
    ) FORMAT Null
    """

    settings = {
        "max_memory_usage": "0",
        "max_bytes_before_external_sort": "1Gi",
        "max_bytes_ratio_before_external_sort": 0.3,
    }
    node.query(query, settings=settings)

    settings["max_bytes_before_external_sort"] = 0
    settings["max_bytes_ratio_before_external_sort"] = 0
    with pytest.raises(QueryRuntimeException):
        node.query(query, settings=settings)

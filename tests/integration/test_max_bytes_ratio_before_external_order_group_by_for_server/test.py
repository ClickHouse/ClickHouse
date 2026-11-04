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
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer uses more memory, making precise memory limit testing unreliable")

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


@pytest.mark.parametrize(
    "node,query",
    [
        # Peak memory usage: ~14GiB (the `DISTINCT` hash set of 100M unique ~85-byte strings)
        pytest.param(
            node_server,
            "SELECT count() FROM (SELECT DISTINCT repeat(number::String, 10) AS k FROM numbers(100e6)) FORMAT Null",
            id="server",
        ),
        # Peak memory usage: ~5.7GiB (7M unique 800-byte strings) against a 4GiB user limit. The final merge
        # holds a block of every spilled run at once, so `max_block_size` is pinned to its default.
        pytest.param(
            node_user,
            "SELECT count() FROM (SELECT DISTINCT repeat(number::String, 100) AS k FROM numbers(10000000, 7000000)) "
            "SETTINGS max_memory_usage_for_user = '4Gi', max_block_size = 65409 FORMAT Null",
            id="user",
        ),
    ],
)
def test_max_bytes_ratio_before_external_distinct(node, query):
    if node.is_built_with_thread_sanitizer():
        pytest.skip("TSan build is skipped due to memory overhead")
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer uses more memory, making precise memory limit testing unreliable")

    settings = {
        "max_memory_usage": "0",
        "max_bytes_before_external_distinct": 0,
        "max_bytes_ratio_before_external_distinct": 0.3,
    }
    node.query(query, settings=settings)

    settings["max_bytes_ratio_before_external_distinct"] = 0
    with pytest.raises(QueryRuntimeException):
        node.query(query, settings=settings)

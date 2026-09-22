import uuid

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
# The runtime of each test below has a floor set by the memory limit it has to reach.
node_server_small = cluster.add_instance(
    "node_server_small", main_configs=["config.d/memory_overrides_small.yaml"]
)
node_user_small = cluster.add_instance(
    "node_user_small", user_configs=["users.d/memory_overrides_small.yaml"]
)


sanitizer_build = {}


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        # A server whose RSS is over max_server_memory_usage rejects even the connection
        # handshake, and the tests below drive the nodes to that limit on purpose.
        sanitizer_build["thread"] = node_server.is_built_with_thread_sanitizer()
        sanitizer_build["memory"] = node_server.is_built_with_memory_sanitizer()
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
    if sanitizer_build["thread"]:
        pytest.skip("TSan build is skipped due to memory overhead")
    if sanitizer_build["memory"]:
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
    if sanitizer_build["thread"]:
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
    "node,rejected_by",
    [
        pytest.param(node_server_small, "(total) memory limit exceeded", id="server"),
        pytest.param(node_user_small, "User memory limit exceeded", id="user"),
    ],
)
def test_max_bytes_ratio_before_external_distinct(node, rejected_by):
    if sanitizer_build["thread"]:
        pytest.skip("TSan build is skipped due to memory overhead")
    if sanitizer_build["memory"]:
        pytest.skip("Memory Sanitizer uses more memory, making precise memory limit testing unreliable")

    # Peak memory usage: ~5.8GiB (7M unique 800-byte keys) against the nodes' 4Gi limit.
    # Every number in the range has 8 digits, so every key is exactly 800 bytes.
    query = """
    SELECT count() FROM (SELECT DISTINCT repeat(number::String, 100) AS k FROM numbers(10000000, 7000000)) FORMAT Null
    """

    settings = {
        "max_memory_usage": "0",
        "max_bytes_before_external_distinct": 0,
        "max_bytes_ratio_before_external_distinct": 0.3,
    }
    query_id = str(uuid.uuid4())
    node.query(query, settings=settings, query_id=query_id)
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        node.query(
            f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' "
            "AND type = 'QueryFinish' AND ProfileEvents['ExternalDistinctWritePart'] > 0"
        )
        == "1\n"
    )

    settings["max_bytes_ratio_before_external_distinct"] = 0
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(query, settings=settings)
    assert rejected_by in str(exc_info.value)
    assert "maximum: 4.00 GiB" in str(exc_info.value)

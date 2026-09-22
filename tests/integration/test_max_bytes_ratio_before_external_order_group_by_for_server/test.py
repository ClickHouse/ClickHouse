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


sanitizer_build = {}


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        # A server whose RSS is over max_server_memory_usage rejects even the connection
        # handshake, and the tests below drive the nodes to that limit on purpose.
        sanitizer_build["thread"] = node_server.is_built_with_thread_sanitizer()
        sanitizer_build["memory"] = node_server.is_built_with_memory_sanitizer()
        sanitizer_build["address"] = node_server.is_built_with_address_sanitizer()
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
    "node,limit_follows_rss",
    [
        pytest.param(node_server, True, id="server"),
        pytest.param(node_user, False, id="user"),
    ],
)
def test_max_bytes_ratio_before_external_distinct(node, limit_follows_rss):
    if sanitizer_build["thread"]:
        pytest.skip("TSan build is skipped due to memory overhead")
    if sanitizer_build["memory"]:
        pytest.skip("Memory Sanitizer uses more memory, making precise memory limit testing unreliable")
    if limit_follows_rss and sanitizer_build["address"]:
        # `max_server_memory_usage` is enforced against RSS, and an Address Sanitizer build's RSS
        # carries redzones and quarantined chunks that the memory tracker never sees: the query
        # below was rejected at `current RSS: 3.95 GiB` while its tracked total was 2.65 GiB.
        # The final `DISTINCT` merge legitimately needs about twice the spill threshold, so the
        # remaining 1.3 GiB of headroom is what this build's overhead consumes. The limit cannot
        # be raised to compensate, because the unspilled peak has to stay above it and still fit
        # into the harness's 600 second per-query cap. The user-limit parameter is unaffected:
        # `max_memory_usage_for_user` counts tracked bytes rather than RSS.
        pytest.skip("Address Sanitizer RSS overhead leaves no headroom under max_server_memory_usage")

    # Peak memory usage: ~14GiB (the `DISTINCT` hash set of 100M unique ~85-byte strings)
    query = """
    SELECT count() FROM (SELECT DISTINCT repeat(number::String, 10) AS k FROM numbers(100e6)) FORMAT Null
    """

    settings = {
        "max_memory_usage": "0",
        "max_bytes_before_external_distinct": 0,
        "max_bytes_ratio_before_external_distinct": 0.3,
    }
    node.query(query, settings=settings)

    settings["max_bytes_ratio_before_external_distinct"] = 0
    with pytest.raises(QueryRuntimeException):
        node.query(query, settings=settings)

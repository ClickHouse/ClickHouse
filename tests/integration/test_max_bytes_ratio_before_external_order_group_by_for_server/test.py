import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Nodes that allows to use only 10Gi of RAM
node1 = cluster.add_instance("node1", main_configs=["config.d/memory_overrides.yaml"])
# In addition to 10Gi RAM limitation, this node configured to use external
# GROUP BY/ORDER BY when used memory reaches threshold
node2 = cluster.add_instance(
    "node2",
    main_configs=["config.d/memory_overrides.yaml", "config.d/ratio_overrides.yaml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_max_bytes_ratio_before_external_group_by_for_server():
    if node1.is_built_with_thread_sanitizer():
        pytest.skip("TSan build is skipped due to memory overhead")

    # Peak memory usage: 15-16GiB
    query = """
    SELECT
        uniqExact(number::String),
        uniqExact((number,number))
    FROM numbers(100e6) GROUP BY (number%1000)::String FORMAT Null
    """
    settings = {
        "max_bytes_before_external_group_by": 0,
        "max_bytes_ratio_before_external_group_by": 0,
        "max_memory_usage": "0",
    }
    with pytest.raises(QueryRuntimeException):
        node1.query(query, settings=settings)
    node2.query(query, settings=settings)


def test_max_bytes_ratio_before_external_sort_for_server():
    if node1.is_built_with_thread_sanitizer():
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
        "max_bytes_before_external_sort": 0,
        "max_bytes_ratio_before_external_sort": 0,
        "max_memory_usage": "0",
    }
    with pytest.raises(QueryRuntimeException):
        node1.query(query, settings=settings)
    node2.query(query, settings=settings)

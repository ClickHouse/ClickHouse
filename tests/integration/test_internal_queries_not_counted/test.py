"""
Test that internal queries do not count towards query limits.
"""

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_dictionary_internal_queries_not_counted(started_cluster):
    """
    This test verifies that when max_concurrent_queries, max_concurrent_select_queries,
    and max_concurrent_queries_for_user are all set to 1, a user query that triggers
    internal dictionary select can still execute successfully.

    If internal queries counted towards limits, the test would fail because:
    1. User runs SELECT * FROM dict (counts as 1 query)
    2. Dictionary runs internal SELECT * FROM system.one (would be 2nd query)

    This would exceed all three limits of 1.
    """

    node.query("""
        CREATE DICTIONARY test_dict (dummy UInt8)
        PRIMARY KEY dummy
        SOURCE(CLICKHOUSE(QUERY 'SELECT dummy from system.one'))
        LAYOUT(DIRECT())
    """)

    # should not get TOO_MANY_SIMULTANEOUS_QUERIES
    result = node.query("SELECT * FROM test_dict")
    assert result == "0\n"

    node.query("DROP DICTIONARY test_dict")

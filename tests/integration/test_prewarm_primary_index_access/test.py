"""
Proof test for bug in getRequiredAccessForDDLOnCluster: PREWARM_PRIMARY_INDEX_CACHE case
checks AccessType::SYSTEM_PREWARM_MARK_CACHE instead of SYSTEM_PREWARM_PRIMARY_INDEX_CACHE.

Bug location: InterpreterSystemQuery.cpp:2539 (copy-paste error from PREWARM_MARK_CACHE case)

Test scenario:
1. Create a user with ONLY SYSTEM_PREWARM_MARK_CACHE privilege (NOT PRIMARY_INDEX_CACHE)
2. Try to run SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER as that user
3. BUG: Access check uses SYSTEM_PREWARM_MARK_CACHE (user has it) -> passes permission check
       -> continues execution -> BAD_ARGUMENTS (cache not configured)
   FIXED: Access check uses SYSTEM_PREWARM_PRIMARY_INDEX_CACHE (user lacks it) -> ACCESS_DENIED

Test expects ACCESS_DENIED (correct behavior after fix).
On buggy master: passes permission check, fails with BAD_ARGUMENTS -> test FAILS
After fix: ACCESS_DENIED -> test PASSES
"""

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_prewarm_primary_index_cache_permission_on_cluster(started_cluster):
    """
    Test that SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER requires
    SYSTEM_PREWARM_PRIMARY_INDEX_CACHE permission, not SYSTEM_PREWARM_MARK_CACHE.
    """
    # Cleanup any existing state
    node1.query("DROP USER IF EXISTS test_user_prewarm")
    node1.query("DROP TABLE IF EXISTS test_table_prewarm")

    # Create test table
    node1.query(
        "CREATE TABLE test_table_prewarm (a UInt64) ENGINE = MergeTree ORDER BY a"
    )
    node1.query("INSERT INTO test_table_prewarm VALUES (1)")

    # Create user with ONLY SYSTEM_PREWARM_MARK_CACHE privilege (NOT PRIMARY_INDEX_CACHE)
    # This is the WRONG permission for PREWARM PRIMARY INDEX CACHE command
    node1.query("CREATE USER test_user_prewarm")
    node1.query("GRANT SYSTEM PREWARM MARK CACHE ON *.* TO test_user_prewarm")

    # Try to run SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER as the test user
    # Expected: ACCESS_DENIED (user lacks SYSTEM_PREWARM_PRIMARY_INDEX_CACHE)
    # Bug behavior: Passes permission check (wrong permission checked), then fails with BAD_ARGUMENTS
    error = node1.query_and_get_error(
        "SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER test_cluster default.test_table_prewarm",
        user="test_user_prewarm",
    )

    # Verify we get ACCESS_DENIED (correct permission check)
    # The bug would cause BAD_ARGUMENTS instead (permission check passes, but cache not configured)
    assert "ACCESS_DENIED" in error, f"Expected ACCESS_DENIED error, got: {error}"

    # Cleanup
    node1.query("DROP USER test_user_prewarm")
    node1.query("DROP TABLE test_table_prewarm")

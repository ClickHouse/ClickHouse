import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_ddl_ctas_from_current_roles():
    """
    Regression test for `BuzzHouse` (amd_tsan) finding `STID: 2436-3d64`:
        Logical error: 'No user in current context, it's a bug'

    Reading from `system.current_roles` or `system.enabled_roles` inside a
    `CREATE TABLE ... AS SELECT` executed by the replicated DDL worker used
    to throw `LOGICAL_ERROR` because the DDL worker's query context has no
    user attached when `distributed_ddl_use_initial_user_and_roles` is
    disabled (the default). In debug/sanitizer builds the exception aborted
    the server; in release builds the DDL returned the exception to the
    client. Both modes here surface as a query exception that fails the
    test, which is what the bugfix-validation framework relies on.
    """
    instance.query(
        "CREATE DATABASE bug_roles "
        "ENGINE = Replicated('/clickhouse/databases/bug_roles', 'shard1', 'replica1')"
    )

    try:
        # CTAS reading from `system.current_roles` inside the replicated DDL
        # worker context. Exercises `StorageSystemCurrentRoles::fillData`.
        instance.query(
            "CREATE TABLE bug_roles.t_current "
            "(role_name String, with_admin_option UInt8, is_default UInt8) "
            "ENGINE = MergeTree() ORDER BY role_name "
            "AS SELECT * FROM system.current_roles"
        )

        # CTAS reading from `system.enabled_roles` inside the replicated DDL
        # worker context. Exercises `StorageSystemEnabledRoles::fillData`.
        instance.query(
            "CREATE TABLE bug_roles.t_enabled "
            "(role_name String, with_admin_option UInt8, is_current UInt8, is_default UInt8) "
            "ENGINE = MergeTree() ORDER BY role_name "
            "AS SELECT * FROM system.enabled_roles"
        )

        # With the fix in place the DDL worker context has no roles and the
        # tables come out empty.
        assert instance.query("SELECT count() FROM bug_roles.t_current").strip() == "0"
        assert instance.query("SELECT count() FROM bug_roles.t_enabled").strip() == "0"
    finally:
        instance.query("DROP DATABASE IF EXISTS bug_roles SYNC")

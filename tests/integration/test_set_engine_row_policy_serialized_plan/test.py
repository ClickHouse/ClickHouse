import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
initiator = cluster.add_instance(
    "initiator",
    main_configs=["configs/config.d/clusters.xml"],
    with_zookeeper=True,
)
worker = cluster.add_instance(
    "worker",
    main_configs=["configs/config.d/clusters.xml"],
    with_zookeeper=True,
)
initial_user_initiator = cluster.add_instance(
    "initial_user_initiator",
    main_configs=["configs/config.d/clusters_initial_user.xml"],
    with_zookeeper=True,
)
initial_user_worker = cluster.add_instance(
    "initial_user_worker",
    main_configs=["configs/config.d/clusters_initial_user.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def assert_set_policy_error(error, table):
    assert "ACCESS_DENIED" in error
    assert f"Cannot use table {table}" in error
    assert "because a row policy applies to it" in error


def assert_privilege_error(error, privilege, table=None):
    assert "ACCESS_DENIED" in error
    assert privilege in error
    if table:
        assert table in error
    assert "because a row policy applies to it" not in error


def assert_missing_on_initiator_error(error, table):
    assert "UNKNOWN_TABLE" in error
    assert (
        f"Table {table} on the right side of IN does not exist on the initiator"
        in error
    )
    assert "distributed_ddl_use_initial_user_and_roles" in error


def test_worker_checks_set_row_policy(started_cluster):
    for node in (initiator, worker):
        node.query("CREATE TABLE set_rp (k UInt64) ENGINE = Set")
        node.query("INSERT INTO set_rp VALUES (1), (2)")
        node.query("CREATE TABLE data_rp (k UInt64) ENGINE = MergeTree ORDER BY k")
        node.query("INSERT INTO data_rp VALUES (1), (2)")

    serialized_query = """
        SELECT count()
        FROM remote('worker', currentDatabase(), data_rp)
        WHERE k IN set_rp
        SETTINGS serialize_query_plan = 1,
                 enable_parallel_replicas = 0,
                 automatic_parallel_replicas_mode = 0
        """

    assert initiator.query(serialized_query) == "2\n"

    worker.query("CREATE ROW POLICY set_rp_filter ON set_rp USING k = 1 TO default")

    error = initiator.query_and_get_error(serialized_query)

    assert_set_policy_error(error, "default.set_rp")


def test_mutation_checks_set_row_policy_without_validation(started_cluster):
    initiator.query("CREATE TABLE local_set_rp (k UInt64) ENGINE = Set")
    initiator.query("INSERT INTO local_set_rp VALUES (1), (2)")
    initiator.query(
        "CREATE TABLE local_data_rp (k UInt64) ENGINE = MergeTree ORDER BY k"
    )
    initiator.query("INSERT INTO local_data_rp VALUES (1), (2)")
    initiator.query(
        "CREATE ROW POLICY local_set_rp_filter ON local_set_rp "
        "USING k = 1 TO default"
    )

    error = initiator.query_and_get_error(
        "ALTER TABLE local_data_rp DELETE WHERE k IN local_set_rp",
        settings={"validate_mutation_query": 0},
    )

    assert_set_policy_error(error, "default.local_set_rp")
    assert initiator.query("SELECT count() FROM local_data_rp") == "2\n"


def test_replicated_mutation_checks_set_row_policy(started_cluster):
    initiator.query(
        "CREATE DATABASE replicated_rp "
        "ENGINE = Replicated('/test/replicated_rp', 'shard1', 'replica1')"
    )
    initiator.query("CREATE TABLE replicated_rp.set_rp (k UInt64) ENGINE = Set")
    initiator.query("INSERT INTO replicated_rp.set_rp VALUES (1), (2)")
    initiator.query(
        "CREATE TABLE replicated_rp.data_rp (k UInt64, v UInt64) "
        "ENGINE = MergeTree ORDER BY k "
        "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
    )
    initiator.query("INSERT INTO replicated_rp.data_rp VALUES (1, 10), (2, 20)")
    initiator.query(
        "CREATE ROW POLICY replicated_set_rp_filter ON replicated_rp.set_rp "
        "USING k = 1 TO default"
    )

    alter_error = initiator.query_and_get_error(
        "ALTER TABLE replicated_rp.data_rp DELETE WHERE k IN set_rp"
    )
    delete_error = initiator.query_and_get_error(
        "DELETE FROM replicated_rp.data_rp WHERE k IN set_rp"
    )
    update_error = initiator.query_and_get_error(
        "UPDATE replicated_rp.data_rp SET v = v + 1 WHERE k IN set_rp",
        settings={"enable_lightweight_update": 1},
    )

    for error in (alter_error, delete_error, update_error):
        assert_set_policy_error(error, "replicated_rp.set_rp")
    assert (
        initiator.query("SELECT count(), sum(v) FROM replicated_rp.data_rp")
        == "2\t30\n"
    )


@pytest.fixture(scope="module")
def replicated_set_with_row_policy(started_cluster):
    initiator.query(
        "CREATE DATABASE replicated_view_rp "
        "ENGINE = Replicated('/test/replicated_view_rp', 'shard1', 'replica1')"
    )
    initiator.query("CREATE TABLE replicated_view_rp.set_rp (k UInt64) ENGINE = Set")
    initiator.query("INSERT INTO replicated_view_rp.set_rp VALUES (1), (2)")
    initiator.query(
        "CREATE VIEW replicated_view_rp.v SQL SECURITY INVOKER AS "
        "SELECT number FROM numbers(10) WHERE number IN replicated_view_rp.set_rp"
    )
    initiator.query(
        "CREATE TABLE replicated_view_rp.data_rp (k UInt64, v UInt64) "
        "ENGINE = MergeTree ORDER BY k "
        "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
    )
    initiator.query(
        "INSERT INTO replicated_view_rp.data_rp VALUES (1, 10), (2, 20), (3, 30)"
    )
    initiator.query("CREATE USER view_mutator")
    initiator.query("GRANT CREATE TEMPORARY TABLE ON *.* TO view_mutator")
    initiator.query("GRANT SELECT ON replicated_view_rp.v TO view_mutator")
    initiator.query("GRANT SELECT ON replicated_view_rp.set_rp TO view_mutator")
    initiator.query(
        "GRANT ALTER DELETE, ALTER UPDATE ON replicated_view_rp.data_rp "
        "TO view_mutator"
    )
    initiator.query(
        "CREATE ROW POLICY view_set_rp_filter ON replicated_view_rp.set_rp "
        "USING k = 1 TO view_mutator"
    )

    select_error = initiator.query_and_get_error(
        "SELECT * FROM replicated_view_rp.v", user="view_mutator"
    )
    assert_set_policy_error(select_error, "replicated_view_rp.set_rp")


REPLICATED_MUTATIONS = [
    pytest.param(
        "ALTER TABLE replicated_view_rp.data_rp DELETE WHERE {}", id="alter_delete"
    ),
    pytest.param("DELETE FROM replicated_view_rp.data_rp WHERE {}", id="delete"),
    pytest.param(
        "UPDATE replicated_view_rp.data_rp SET v = v + 1 WHERE {}", id="update"
    ),
]


def assert_replicated_data_unchanged():
    assert (
        initiator.query("SELECT count(), sum(v) FROM replicated_view_rp.data_rp")
        == "3\t60\n"
    )


@pytest.mark.parametrize("mutation", REPLICATED_MUTATIONS)
def test_replicated_mutation_checks_set_row_policy_through_view(
    replicated_set_with_row_policy, mutation
):
    # The DDL worker of a `Replicated` database would run the mutation with full access.
    error = initiator.query_and_get_error(
        mutation.format("k IN (SELECT number FROM replicated_view_rp.v)"),
        user="view_mutator",
        settings={"enable_lightweight_update": 1},
    )

    assert_set_policy_error(error, "replicated_view_rp.set_rp")
    assert_replicated_data_unchanged()


@pytest.mark.parametrize("mutation", REPLICATED_MUTATIONS)
def test_replicated_mutation_checks_set_row_policy_behind_temporary_table(
    replicated_set_with_row_policy, mutation
):
    # The session sees its temporary `set_rp`, the DDL worker sees the `Set` table.
    error = initiator.query_and_get_error(
        "CREATE TEMPORARY TABLE set_rp (k UInt64); " + mutation.format("k IN set_rp"),
        user="view_mutator",
        settings={"enable_lightweight_update": 1},
    )

    assert_set_policy_error(error, "replicated_view_rp.set_rp")
    assert_replicated_data_unchanged()


def test_replicated_mutation_checks_set_row_policy_in_assigned_value(
    replicated_set_with_row_policy,
):
    error = initiator.query_and_get_error(
        "UPDATE replicated_view_rp.data_rp "
        "SET v = (SELECT max(number) FROM replicated_view_rp.v) WHERE k = 3",
        user="view_mutator",
        settings={"enable_lightweight_update": 1},
    )

    assert_set_policy_error(error, "replicated_view_rp.set_rp")
    assert_replicated_data_unchanged()


def test_on_cluster_mutation_checks_initiator_row_policy(started_cluster):
    assert (
        initiator.query(
            "SELECT value FROM system.server_settings "
            "WHERE name = 'distributed_ddl_use_initial_user_and_roles'"
        )
        == "0\n"
    )

    for node in (initiator, worker):
        node.query("CREATE TABLE cluster_set_rp (k UInt64, v UInt64) ENGINE = Set")
        node.query("INSERT INTO cluster_set_rp VALUES (1, 10), (2, 20)")
        node.query(
            "CREATE TABLE cluster_data_rp (k UInt64, v UInt64) "
            "ENGINE = MergeTree ORDER BY k "
            "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
        )
        node.query("INSERT INTO cluster_data_rp VALUES (1, 10), (2, 20)")

    initiator.query("CREATE ROLE cluster_mutator_role")
    initiator.query("CREATE USER cluster_mutator DEFAULT ROLE cluster_mutator_role")
    initiator.query("GRANT CLUSTER ON *.* TO cluster_mutator_role")
    initiator.query(
        "GRANT ALTER DELETE, ALTER UPDATE ON default.cluster_data_rp "
        "TO cluster_mutator_role"
    )
    initiator.query(
        "CREATE ROW POLICY cluster_set_rp_filter ON cluster_set_rp "
        "USING k = 1 TO cluster_mutator_role"
    )
    initiator.query("GRANT SELECT(k) ON default.cluster_set_rp TO cluster_mutator_role")

    analyzer_source_access_error = initiator.query_and_get_error(
        "SELECT tuple(1, 10) IN cluster_set_rp",
        user="cluster_mutator",
    )
    assert_privilege_error(
        analyzer_source_access_error, "SELECT", "default.cluster_set_rp"
    )
    assert (
        initiator.query(
            "SELECT inIgnoreSet(tuple(1, 10), cluster_set_rp)",
            user="cluster_mutator",
        )
        == "0\n"
    )

    source_access_error = initiator.query_and_get_error(
        "ALTER TABLE cluster_data_rp ON CLUSTER cluster "
        "DELETE WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
    )
    assert_privilege_error(source_access_error, "SELECT", "default.cluster_set_rp")

    initiator.query("GRANT SELECT(v) ON default.cluster_set_rp TO cluster_mutator_role")
    analyzer_policy_error = initiator.query_and_get_error(
        "SELECT tuple(1, 10) IN cluster_set_rp",
        user="cluster_mutator",
    )
    assert_set_policy_error(analyzer_policy_error, "default.cluster_set_rp")
    initiator.query(
        "REVOKE ALTER DELETE ON default.cluster_data_rp FROM cluster_mutator_role"
    )
    target_access_error = initiator.query_and_get_error(
        "ALTER TABLE cluster_data_rp ON CLUSTER cluster "
        "DELETE WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
    )
    assert_privilege_error(
        target_access_error, "ALTER DELETE", "default.cluster_data_rp"
    )
    initiator.query(
        "GRANT ALTER DELETE ON default.cluster_data_rp TO cluster_mutator_role"
    )

    initiator.query("REVOKE CLUSTER ON *.* FROM cluster_mutator_role")
    cluster_access_error = initiator.query_and_get_error(
        "DELETE FROM cluster_data_rp ON CLUSTER cluster "
        "WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
    )
    assert_privilege_error(cluster_access_error, "CLUSTER")
    initiator.query("GRANT CLUSTER ON *.* TO cluster_mutator_role")

    alter_error = initiator.query_and_get_error(
        "ALTER TABLE cluster_data_rp ON CLUSTER cluster "
        "DELETE WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
    )
    update_error = initiator.query_and_get_error(
        "UPDATE cluster_data_rp ON CLUSTER cluster "
        "SET v = v + 1 WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
        settings={"enable_lightweight_update": 1},
    )
    delete_error = initiator.query_and_get_error(
        "DELETE FROM cluster_data_rp ON CLUSTER cluster "
        "WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
    )

    for error in (alter_error, update_error, delete_error):
        assert_set_policy_error(error, "default.cluster_set_rp")
    for node in (initiator, worker):
        assert node.query("SELECT count(), sum(v) FROM cluster_data_rp") == "2\t30\n"

    worker.query(
        "CREATE TABLE remote_cluster_data_rp (k UInt64, v UInt64) "
        "ENGINE = MergeTree ORDER BY k "
        "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
    )
    worker.query("INSERT INTO remote_cluster_data_rp VALUES (1, 10), (2, 20)")
    worker.query("CREATE TABLE remote_only_set_rp (k UInt64) ENGINE = Set")
    worker.query("INSERT INTO remote_only_set_rp VALUES (1), (2)")
    initiator.query(
        "GRANT ALTER UPDATE ON default.remote_cluster_data_rp TO cluster_mutator_role"
    )
    remote_delete_access_error = initiator.query_and_get_error(
        "UPDATE default.remote_cluster_data_rp ON CLUSTER worker_only "
        "SET _row_exists = 0 WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
        settings={"enable_lightweight_update": 1},
    )
    assert_privilege_error(
        remote_delete_access_error,
        "ALTER DELETE",
        "default.remote_cluster_data_rp",
    )
    initiator.query(
        "GRANT ALTER DELETE ON default.remote_cluster_data_rp TO cluster_mutator_role"
    )

    remote_alter_error = initiator.query_and_get_error(
        "ALTER TABLE default.remote_cluster_data_rp ON CLUSTER worker_only "
        "DELETE WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
    )
    remote_update_error = initiator.query_and_get_error(
        "UPDATE default.remote_cluster_data_rp ON CLUSTER worker_only "
        "SET v = v + 1 WHERE (k, v) IN cluster_set_rp",
        user="cluster_mutator",
        settings={"enable_lightweight_update": 1},
    )
    for error in (remote_alter_error, remote_update_error):
        assert_set_policy_error(error, "default.cluster_set_rp")

    unresolved_source_error = initiator.query_and_get_error(
        "ALTER TABLE default.remote_cluster_data_rp ON CLUSTER worker_only "
        "DELETE WHERE k IN remote_only_set_rp",
        user="cluster_mutator",
    )
    assert_missing_on_initiator_error(
        unresolved_source_error, "default.remote_only_set_rp"
    )
    # The initiator cannot tell the engine of a table it does not have.
    unresolved_merge_tree_error = initiator.query_and_get_error(
        "ALTER TABLE default.remote_cluster_data_rp ON CLUSTER worker_only "
        "DELETE WHERE (k, v) IN remote_cluster_data_rp",
        user="cluster_mutator",
    )
    assert_missing_on_initiator_error(
        unresolved_merge_tree_error, "default.remote_cluster_data_rp"
    )
    assert (
        worker.query("SELECT count(), sum(v) FROM remote_cluster_data_rp") == "2\t30\n"
    )


def test_on_cluster_mutation_defers_worker_only_set_to_worker(started_cluster):
    # With `distributed_ddl_use_initial_user_and_roles` and an entry format which carries the
    # initiator's user, the worker runs the mutation as the submitting user, so a `Set` which exists only on the worker is checked there instead of
    # being rejected on the initiator with `UNKNOWN_TABLE`.
    for node in (initial_user_initiator, initial_user_worker):
        node.query("CREATE ROLE iu_mutator_role")
        node.query("CREATE USER iu_mutator DEFAULT ROLE iu_mutator_role")
        node.query("GRANT CLUSTER ON *.* TO iu_mutator_role")
        node.query("GRANT ALTER DELETE ON default.iu_data_rp TO iu_mutator_role")
        node.query("GRANT SELECT ON default.* TO iu_mutator_role")

    initial_user_worker.query(
        "CREATE TABLE iu_data_rp (k UInt64) ENGINE = MergeTree ORDER BY k"
    )
    initial_user_worker.query("INSERT INTO iu_data_rp VALUES (1), (2), (3)")
    for table in ("iu_set_policy", "iu_set_plain"):
        initial_user_worker.query(f"CREATE TABLE {table} (k UInt64) ENGINE = Set")
        initial_user_worker.query(f"INSERT INTO {table} VALUES (1)")
    initial_user_worker.query(
        "CREATE ROW POLICY iu_set_policy_filter ON iu_set_policy "
        "USING k = 1 TO iu_mutator_role"
    )

    policy_error = initial_user_initiator.query_and_get_error(
        "ALTER TABLE default.iu_data_rp ON CLUSTER initial_user_worker_only "
        "DELETE WHERE k IN iu_set_policy "
        "SETTINGS mutations_sync = 2, distributed_ddl_entry_format_version = 8",
        user="iu_mutator",
    )
    assert "UNKNOWN_TABLE" not in policy_error
    assert_set_policy_error(policy_error, "default.iu_set_policy")
    assert initial_user_worker.query("SELECT count() FROM iu_data_rp") == "3\n"

    initial_user_initiator.query(
        "ALTER TABLE default.iu_data_rp ON CLUSTER initial_user_worker_only "
        "DELETE WHERE k IN iu_set_plain "
        "SETTINGS mutations_sync = 2, distributed_ddl_entry_format_version = 8",
        user="iu_mutator",
    )
    assert (
        initial_user_worker.query("SELECT groupArray(k) FROM iu_data_rp") == "[2,3]\n"
    )

    # The default entry format does not carry the initiator's user, so the worker would run the
    # mutation without the submitting user's row policies: the initiator still rejects it.
    unresolved_source_error = initial_user_initiator.query_and_get_error(
        "ALTER TABLE default.iu_data_rp ON CLUSTER initial_user_worker_only "
        "DELETE WHERE k IN iu_set_policy",
        user="iu_mutator",
    )
    assert_missing_on_initiator_error(unresolved_source_error, "default.iu_set_policy")
    assert (
        initial_user_worker.query("SELECT groupArray(k) FROM iu_data_rp") == "[2,3]\n"
    )

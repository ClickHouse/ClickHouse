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

    worker.query(
        "CREATE ROW POLICY set_rp_filter ON set_rp USING k = 1 TO default"
    )

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
    assert initiator.query("SELECT count(), sum(v) FROM replicated_rp.data_rp") == "2\t30\n"


def test_on_cluster_mutation_checks_initiator_row_policy(started_cluster):
    assert (
        initiator.query(
            "SELECT value FROM system.server_settings "
            "WHERE name = 'distributed_ddl_use_initial_user_and_roles'"
        )
        == "0\n"
    )

    for node in (initiator, worker):
        node.query("CREATE TABLE cluster_set_rp (k UInt64) ENGINE = Set")
        node.query("INSERT INTO cluster_set_rp VALUES (1), (2)")
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

    source_access_error = initiator.query_and_get_error(
        "ALTER TABLE cluster_data_rp ON CLUSTER cluster "
        "DELETE WHERE k IN cluster_set_rp",
        user="cluster_mutator",
    )
    assert_privilege_error(source_access_error, "SELECT", "default.cluster_set_rp")

    initiator.query("GRANT SELECT ON default.cluster_set_rp TO cluster_mutator_role")
    initiator.query(
        "REVOKE ALTER DELETE ON default.cluster_data_rp FROM cluster_mutator_role"
    )
    target_access_error = initiator.query_and_get_error(
        "ALTER TABLE cluster_data_rp ON CLUSTER cluster "
        "DELETE WHERE k IN cluster_set_rp",
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
        "UPDATE cluster_data_rp ON CLUSTER cluster "
        "SET v = v + 1 WHERE k IN cluster_set_rp",
        user="cluster_mutator",
        settings={"enable_lightweight_update": 1},
    )
    assert_privilege_error(cluster_access_error, "CLUSTER")
    initiator.query("GRANT CLUSTER ON *.* TO cluster_mutator_role")

    alter_error = initiator.query_and_get_error(
        "ALTER TABLE cluster_data_rp ON CLUSTER cluster "
        "DELETE WHERE k IN cluster_set_rp",
        user="cluster_mutator",
    )
    update_error = initiator.query_and_get_error(
        "UPDATE cluster_data_rp ON CLUSTER cluster "
        "SET v = v + 1 WHERE k IN cluster_set_rp",
        user="cluster_mutator",
        settings={"enable_lightweight_update": 1},
    )

    for error in (alter_error, update_error):
        assert_set_policy_error(error, "default.cluster_set_rp")
    for node in (initiator, worker):
        assert node.query("SELECT count(), sum(v) FROM cluster_data_rp") == "2\t30\n"

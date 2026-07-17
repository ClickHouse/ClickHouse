import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import (
    PostgresManager,
    check_tables_are_synchronized,
)

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "coord_instance1",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users_coordination.xml"],
    with_postgres=True,
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "1", "replica": "coord_instance1"},
)

instance2 = cluster.add_instance(
    "coord_instance2",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users_coordination.xml"],
    with_postgres=True,
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "1", "replica": "coord_instance2"},
)

# {shard} is identical on both replicas, so the coordination path resolves to the same node.
KEEPER_PATH = "/clickhouse/mat_pg/{shard}/test"
KEEPER_PATH_RESOLVED = "/clickhouse/mat_pg/1/test"

COORDINATION_SETTINGS = [
    "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
    f"materialized_postgresql_keeper_path = '{KEEPER_PATH}'",
    "materialized_postgresql_replica_name = '{replica}'",
]

pg_manager = PostgresManager()
pg_manager2 = PostgresManager()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        pg_manager.init(
            instance,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
        pg_manager2.init(
            instance2,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
            postgres_db_exists=True,
        )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    yield
    # Make sure both nodes are up for the next test even if a test stopped one of them.
    for node in (instance, instance2):
        if not node.get_process_pid("clickhouse"):
            node.start_clickhouse()
    for manager in (pg_manager, pg_manager2):
        try:
            manager.drop_materialized_db()
        except Exception:
            pass


def get_leader(node):
    return node.query(
        f"SELECT value FROM system.zookeeper "
        f"WHERE path = '{KEEPER_PATH_RESOLVED}' AND name = 'leader'"
    ).strip()


def count_leader_nodes(node):
    return int(
        node.query(
            f"SELECT count() FROM system.zookeeper "
            f"WHERE path = '{KEEPER_PATH_RESOLVED}' AND name = 'leader'"
        )
    )


def wait_for_leader(node, expected=None, not_equal=None, timeout=90):
    for _ in range(timeout):
        try:
            leader = get_leader(node)
            if (
                leader
                and (expected is None or leader == expected)
                and (not_equal is None or leader != not_equal)
            ):
                return leader
        except Exception:
            pass
        time.sleep(1)
    raise AssertionError(
        f"Leader did not settle (expected={expected}, not_equal={not_equal})"
    )


def create_coordinated_db(tables_list):
    settings = COORDINATION_SETTINGS + [
        f"materialized_postgresql_tables_list = '{tables_list}'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )


def test_replicated_nested_tables_converge_with_single_leader(started_cluster):
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")

    # Both replicas serve the same data, even though only one of them consumes the slot.
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    # The nested table is a ReplicatedReplacingMergeTree on both nodes.
    assert "ReplicatedReplacingMergeTree" in instance.query(
        "SHOW CREATE TABLE test_database.test_table"
    )
    assert "ReplicatedReplacingMergeTree" in instance2.query(
        "SHOW CREATE TABLE test_database.test_table"
    )

    # Exactly one active worker.
    wait_for_leader(instance)
    assert count_leader_nodes(instance) == 1


def test_leader_failover_and_rejoin_without_data_loss(started_cluster):
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    leader_name = wait_for_leader(instance)
    leader_node = instance if leader_name == "coord_instance1" else instance2
    standby_node = instance2 if leader_name == "coord_instance1" else instance

    # Kill the active worker; the standby must take over.
    leader_node.stop_clickhouse()
    new_leader = wait_for_leader(standby_node, not_equal=leader_name)
    assert new_leader != leader_name

    # New changes flow through the new active worker.
    standby_node.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(standby_node, "test_table")
    assert (
        int(standby_node.query("SELECT count() FROM test_database.test_table")) == 200
    )
    # Still exactly one active worker.
    assert count_leader_nodes(standby_node) == 1

    # The old worker rejoins as a standby and catches up via ClickHouse replication.
    leader_node.start_clickhouse()
    check_tables_are_synchronized(leader_node, "test_table")
    assert int(leader_node.query("SELECT count() FROM test_database.test_table")) == 200
    assert count_leader_nodes(standby_node) == 1


def test_replicated_engine_requires_keeper_path(started_cluster):
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_no_keeper "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'"
    )
    assert "materialized_postgresql_keeper_path" in error


def test_coordination_conflicts_with_unique_consumer_identifier(started_cluster):
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_conflict "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{KEEPER_PATH}', "
        f"materialized_postgresql_use_unique_replication_consumer_identifier = 1"
    )
    assert "use_unique_replication_consumer_identifier" in error


def test_unknown_table_engine_is_rejected(started_cluster):
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_bad_engine "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'MergeTree'"
    )
    assert "materialized_postgresql_table_engine" in error

import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import (
    PostgresManager,
    check_tables_are_synchronized,
    create_replication_slot,
    get_postgres_conn,
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


def pg_query(query):
    conn = get_postgres_conn(
        ip=cluster.postgres_ip, port=cluster.postgres_port, database=True
    )
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


# The generated (non-user-managed) slot name for the database engine over the default schema
# is the PostgreSQL database name.
SHARED_SLOT_NAME = "postgres_database"


def replication_slot_exists():
    return (
        len(
            pg_query(
                f"SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{SHARED_SLOT_NAME}'"
            )
        )
        > 0
    )


def publication_exists():
    return len(pg_query("SELECT pubname FROM pg_publication")) > 0


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


def test_keeper_path_requires_replicated_engine(started_cluster):
    # Coordination with a plain (non-replicated) nested engine would leave the standbys without
    # data, so a takeover would lose every row replicated before the failover.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_plain_engine_with_keeper "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_keeper_path = '{KEEPER_PATH}'"
    )
    assert "ReplicatedReplacingMergeTree" in error


def test_takeover_before_snapshot_completion_reloads_all_rows(started_cluster):
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # Simulate an active worker that died after creating the replication slot but before
    # finishing the initial snapshot: the slot exists, but the durable snapshot-completion
    # marker in Keeper is absent.
    replication_conn = get_postgres_conn(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        database=True,
        replication=True,
    )
    create_replication_slot(replication_conn, slot_name=SHARED_SLOT_NAME)
    replication_conn.close()

    create_coordinated_db("test_table")

    # Without the marker the new active worker must redo the snapshot instead of resuming from
    # the slot's confirmed LSN; otherwise the 100 pre-slot rows would be lost permanently.
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 100

    # And ongoing changes keep flowing through the recreated slot.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")


def test_second_create_adopts_publication(started_cluster):
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    check_tables_are_synchronized(instance, "test_table")
    assert publication_exists()

    # The second CREATE must adopt the existing publication (shared state) instead of dropping
    # it from under the active worker.
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    assert publication_exists()

    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200


def test_attach_detach_table_is_rejected_in_coordinated_mode(started_cluster):
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("extra_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    # Dynamically adding/removing tables mutates the shared publication and only takes effect on
    # one replica, so it is refused on every replica (leader and standby alike).
    for node in (instance, instance2):
        error = node.query_and_get_error(
            "ATTACH TABLE test_database.extra_table"
        )
        assert "coordinated MaterializedPostgreSQL" in error

        error = node.query_and_get_error(
            "DETACH TABLE test_database.test_table PERMANENTLY"
        )
        assert "coordinated MaterializedPostgreSQL" in error

    # The refused operations must not have broken replication.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")


def test_drop_keeps_shared_state_until_last_replica(started_cluster):
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert replication_slot_exists()
    assert publication_exists()

    leader_name = wait_for_leader(instance)
    leader_instance = instance if leader_name == "coord_instance1" else instance2
    leader_manager = pg_manager if leader_name == "coord_instance1" else pg_manager2
    standby_manager = pg_manager2 if leader_name == "coord_instance1" else pg_manager

    # Dropping a standby must keep the shared replication slot and publication for the leader.
    standby_manager.drop_materialized_db()
    assert replication_slot_exists()
    assert publication_exists()

    leader_instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(leader_instance, "test_table")

    # Dropping the last replica removes the shared objects from PostgreSQL.
    leader_manager.drop_materialized_db()
    assert not replication_slot_exists()
    assert not publication_exists()


def test_coordination_conflicts_with_user_managed_slot(started_cluster):
    # Coordination owns the shared slot: if the active worker dies before the initial snapshot
    # completes, the next leader must drop and recreate the slot to obtain a fresh exported snapshot,
    # which is impossible for a slot it does not manage. The combination must be rejected up front.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_user_slot "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{KEEPER_PATH}', "
        f"materialized_postgresql_replication_slot = 'user_managed_slot'"
    )
    assert "materialized_postgresql_replication_slot" in error


def test_coordination_conflicts_with_user_provided_snapshot(started_cluster):
    # A fixed snapshot token would become stale as soon as coordination (re)creates the shared slot,
    # so a mid-snapshot takeover could never recover. The combination must be rejected up front.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_user_snapshot "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{KEEPER_PATH}', "
        f"materialized_postgresql_snapshot = 'some-snapshot-token'"
    )
    assert "materialized_postgresql_snapshot" in error


def test_second_coordinated_create_adopts_publication_table_set(started_cluster):
    # A coordinated CREATE with an empty tables list adopts the shared publication instead of dropping
    # it. Its table set must be derived from the publication itself, not from a fresh scan of the live
    # PostgreSQL schema: if the schema drifted after the first replica created the publication, a schema
    # scan would make the joining replica build nested tables that the publication never feeds, so those
    # tables would stay empty forever after a failover.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # No materialized_postgresql_tables_list -> replicate all tables. The first replica creates the
    # shared publication (FOR TABLE ONLY test_table).
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=COORDINATION_SETTINGS
    )
    check_tables_are_synchronized(instance, "test_table")
    assert publication_exists()

    # The PostgreSQL schema drifts: a table is added after the publication already exists. Because the
    # publication lists its tables explicitly (FOR TABLE ONLY), it does not pick the new table up.
    pg_manager.create_postgres_table("late_table")

    # The second replica joins with an empty tables list too. It must adopt the publication's table set
    # rather than the drifted live schema.
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=COORDINATION_SETTINGS
    )
    check_tables_are_synchronized(instance2, "test_table")

    # The joining replica exposes exactly the publication's tables, not the drifted schema.
    tables_on_standby = instance2.query("SHOW TABLES FROM test_database").split()
    assert "test_table" in tables_on_standby
    assert "late_table" not in tables_on_standby

    pg_query('DROP TABLE IF EXISTS "late_table"')

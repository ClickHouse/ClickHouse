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

# A replica with no Keeper/ZooKeeper configured. Coordination needs Keeper, so a coordinated CREATE
# DATABASE must be rejected here up front instead of succeeding and then retrying forever in the
# background startup task.
instance_no_keeper = cluster.add_instance(
    "coord_instance_no_keeper",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users_coordination.xml"],
    with_postgres=True,
    stay_alive=True,
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
        # Statements without a result set (DROP/INSERT/...) leave cursor.description as None,
        # and calling fetchall() on them raises "no results to fetch".
        if cursor.description is None:
            conn.commit()
            return []
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


def test_drop_table_is_rejected_in_coordinated_mode(started_cluster):
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    # Dropping an individual nested table only removes it locally and does not update the shared
    # publication, so the other replicas keep consuming a publication that still contains it. It is
    # refused on every replica (leader and standby alike), before the table is shut down.
    for node in (instance, instance2):
        error = node.query_and_get_error("DROP TABLE test_database.test_table")
        assert "coordinated MaterializedPostgreSQL" in error

    # The refused DROP must not have broken replication of the nested table.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200


def test_rename_and_exchange_table_are_rejected_in_coordinated_mode(started_cluster):
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("other_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    instance.query(
        "INSERT INTO postgres_database.other_table SELECT number, number FROM numbers(50)"
    )

    create_coordinated_db("test_table, other_table")
    for table in ("test_table", "other_table"):
        check_tables_are_synchronized(instance, table)
        check_tables_are_synchronized(instance2, table)

    # Renaming/exchanging a nested table only changes it on the local replica, while the shared publication,
    # the tables-list setting, the cached wrappers and the peer replicas keep the old name - diverging the
    # coordinated setup. Both RENAME and EXCHANGE are refused on every replica (leader and standby alike).
    for node in (instance, instance2):
        error = node.query_and_get_error(
            "RENAME TABLE test_database.test_table TO test_database.renamed_table"
        )
        assert "coordinated MaterializedPostgreSQL" in error

        error = node.query_and_get_error(
            "EXCHANGE TABLES test_database.test_table AND test_database.other_table"
        )
        assert "coordinated MaterializedPostgreSQL" in error

    # The refused RENAME/EXCHANGE must not have broken replication or renamed anything.
    tables = instance2.query("SHOW TABLES FROM test_database").split()
    assert "test_table" in tables
    assert "other_table" in tables
    assert "renamed_table" not in tables

    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200


def test_coordinated_create_adopts_publication_over_mismatching_tables_list(started_cluster):
    # A coordinated CREATE with an explicit `materialized_postgresql_tables_list` that disagrees with the
    # already-existing shared publication must not honor the local list: the publication is authoritative
    # shared state and is adopted (not recreated), so building nested tables for a table the publication
    # never publishes into would leave that table empty forever and make replicas diverge on which tables
    # actually replicate. The joining replica must adopt the publication's table set instead.
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("extra_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # The first replica publishes only test_table (FOR TABLE ONLY test_table).
    first_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=first_settings
    )
    check_tables_are_synchronized(instance, "test_table")
    assert publication_exists()

    # The second replica joins with a superset list. The extra_table is not in the shared publication,
    # so it must be dropped from the effective table set rather than built as an empty nested table.
    second_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table, extra_table'"
    ]
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=second_settings
    )
    check_tables_are_synchronized(instance2, "test_table")

    tables_on_standby = instance2.query("SHOW TABLES FROM test_database").split()
    assert "test_table" in tables_on_standby
    assert "extra_table" not in tables_on_standby


def test_shared_engine_is_rejected_when_unavailable(started_cluster):
    # `SharedReplacingMergeTree` is a ClickHouse Cloud engine that is not registered in the open-source
    # build. Accepting it here would let CREATE DATABASE succeed and only fail much later, when the nested
    # tables are created, leaving the database stuck in a background retry loop. The setting must be
    # rejected up front against the actually-registered table engines.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_shared_engine "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'SharedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{KEEPER_PATH}'"
    )
    assert "is not available in this build" in error


def test_coordination_conflicts_with_column_filtered_tables_list(started_cluster):
    # Coordinated replicas share one set of nested tables on the same Keeper path, so they must agree on
    # the exact column projection. The per-table column list is taken from each replica's local setting
    # rather than from the shared publication, so a column-filtered `materialized_postgresql_tables_list`
    # would let two replicas create diverging schemas on the same shared path. It must be rejected.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_column_filtered "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{KEEPER_PATH}', "
        f"materialized_postgresql_tables_list = 'test_table(key, value)'"
    )
    assert "column-filtered" in error


def test_coordination_requires_keeper_configured(started_cluster):
    # Coordination stores its leader/replica/snapshot nodes in Keeper, and the nested tables are
    # Replicated/SharedReplacingMergeTree, which also need it. On a server with no Keeper configured the
    # first failure would otherwise only happen later inside the background startup task, so CREATE would
    # succeed and the database would sit in a permanent retry loop. It must be rejected synchronously.
    error = instance_no_keeper.query_and_get_error(
        f"CREATE DATABASE test_no_keeper_server "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/no_keeper/test'"
    )
    assert "requires Keeper/ZooKeeper" in error
    # The rejected CREATE must not have left a retrying database behind.
    assert "test_no_keeper_server" not in instance_no_keeper.query("SHOW DATABASES")


def test_takeover_after_partial_snapshot_drops_stale_deleted_rows(started_cluster):
    # A worker that dies mid-snapshot may have already copied rows into the shared nested table before
    # dying. If PostgreSQL then DELETEs (or UPDATEs) one of those rows, redoing the snapshot by merely
    # re-inserting the current PostgreSQL state on top of the existing table would leave the stale copy
    # behind: a deleted row has no counterpart in the new snapshot, so nothing overrides it (a
    # ReplacingMergeTree collapses duplicate keys by version but never turns a now-absent row into a
    # tombstone). The recovery path must clear the nested tables first so the redo produces exactly the
    # current PostgreSQL state.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 100

    wait_for_leader(instance)

    # Stop both replicas so nothing consumes the slot while we set up the "died mid-snapshot" state: the
    # 100 rows are already in the shared nested table, but we drop the durable completion marker and then
    # change PostgreSQL. Because no consumer is running, these changes never reach ClickHouse through WAL
    # replay; the only way they can be reflected is a from-scratch redo of the snapshot on restart.
    instance.stop_clickhouse()
    instance2.stop_clickhouse()

    zk = started_cluster.get_kazoo_client("zoo1")
    marker_path = KEEPER_PATH_RESOLVED + "/snapshot_completed"
    assert zk.exists(marker_path) is not None
    zk.delete(marker_path)
    zk.stop()

    pg_query("DELETE FROM test_table WHERE key >= 50")
    pg_query("UPDATE test_table SET value = 777 WHERE key = 0")

    instance.start_clickhouse()
    instance2.start_clickhouse()

    # The new active worker sees the slot without the completion marker and redoes the snapshot. A correct
    # redo clears the nested tables first, so the 50 now-deleted rows do not survive as stale copies and
    # the updated row carries its new value.
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 50
    assert (
        int(
            instance.query(
                "SELECT count() FROM test_database.test_table WHERE key >= 50"
            )
        )
        == 0
    )
    assert (
        int(instance.query("SELECT value FROM test_database.test_table WHERE key = 0"))
        == 777
    )


def test_keeper_path_rejects_per_replica_macro(started_cluster):
    # The keeper path is both the coordination namespace and the root of the shared nested tables, so it
    # must resolve to the same value on every replica. A per-replica macro like {replica} would put each
    # replica on a disjoint Keeper subtree - each electing its own leader and creating its own nested
    # tables - while they still contend for the same PostgreSQL slot and publication, so the loser would
    # silently never receive data. It must be rejected at CREATE time.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_per_replica_path "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{{shard}}/{{replica}}/test'"
    )
    assert "{replica}" in error
    assert "materialized_postgresql_replica_name" in error
    # The rejected CREATE must not have left a database behind.
    assert "test_per_replica_path" not in instance.query("SHOW DATABASES")


def test_join_with_drifted_schema_reports_error(started_cluster):
    # The shared nested-table schema is authoritative in coordinated mode. A replica joins by declaring a
    # nested table derived from the *current* PostgreSQL schema, and ReplicatedMergeTree compares that
    # against the metadata already stored in Keeper. If the PostgreSQL table drifted after the shared tree
    # was created (MaterializedPostgreSQL continues by column position and does not track PostgreSQL DDL),
    # the join fails. It must report an actionable schema-drift error, not a cryptic metadata mismatch.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # Only the first replica creates the shared tree (schema: key, value).
    create_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=create_settings
    )
    check_tables_are_synchronized(instance, "test_table")

    # Drift the PostgreSQL schema (add a column) without any further DML, so the leader's consumer is not
    # disturbed but a joining replica now derives a different structure.
    pg_query("ALTER TABLE test_table ADD COLUMN extra integer DEFAULT 0")

    # The second replica derives (key, value, extra) and cannot join the shared (key, value) tree.
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=create_settings
    )

    for _ in range(90):
        if instance2.contains_in_log("shared nested-table schema is authoritative"):
            break
        time.sleep(1)
    else:
        raise AssertionError("The joining replica did not report the schema-drift error")


def test_drop_database_fails_when_keeper_is_unavailable(started_cluster):
    # The last-replica decision on DROP DATABASE is made in Keeper. If Keeper is unavailable it must
    # fail-close: DROP DATABASE has to fail before the local nested tables are removed, otherwise it could
    # delete the last actual copy of the data while the shared slot, publication and snapshot_completed
    # marker survive (a later recreate would then resume into empty tables).
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    # Drop the standby (not the leader) so the leader's replication is not disturbed by the outage.
    leader_name = wait_for_leader(instance)
    standby = instance2 if leader_name == "coord_instance1" else instance

    zk_nodes = ["zoo1", "zoo2", "zoo3"]
    try:
        started_cluster.stop_zookeeper_nodes(zk_nodes)

        error = standby.query_and_get_error("DROP DATABASE test_database")
        assert error != ""
        # The database and its local data must survive the refused drop.
        assert "test_database" in standby.query("SHOW DATABASES")
        assert (
            int(standby.query("SELECT count() FROM test_database.test_table")) == 100
        )
    finally:
        started_cluster.start_zookeeper_nodes(zk_nodes)
        # Wait until Keeper is reachable again so the teardown drop can complete.
        for _ in range(120):
            try:
                standby.query("SELECT count() FROM system.zookeeper WHERE path = '/'")
                break
            except Exception:
                time.sleep(1)


def test_database_wide_truncate_is_rejected_in_coordinated_mode(started_cluster):
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("other_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    instance.query(
        "INSERT INTO postgres_database.other_table SELECT number, number FROM numbers(50)"
    )

    create_coordinated_db("test_table, other_table")
    for table in ("test_table", "other_table"):
        check_tables_are_synchronized(instance, table)
        check_tables_are_synchronized(instance2, table)

    # A database-wide TRUNCATE (both `TRUNCATE DATABASE` and `TRUNCATE ALL TABLES FROM`) walks the nested
    # Replicated tables through an internal context and drops/truncates each one directly, bypassing the
    # per-table guards. Without a coordinated truncate path, one replica could locally wipe its copy of the
    # shared data while the shared slot, publication and snapshot_completed marker (and the live consumer) stay
    # in place. Both forms are refused on every replica (leader and standby alike).
    for node in (instance, instance2):
        error = node.query_and_get_error("TRUNCATE DATABASE test_database")
        assert "coordinated MaterializedPostgreSQL" in error

        error = node.query_and_get_error("TRUNCATE ALL TABLES FROM test_database")
        assert "coordinated MaterializedPostgreSQL" in error

    # The refused TRUNCATE must not have removed any data on either replica.
    for node in (instance, instance2):
        assert int(node.query("SELECT count() FROM test_database.test_table")) == 100
        assert int(node.query("SELECT count() FROM test_database.other_table")) == 50

    # Replication still works after the refused truncates.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200

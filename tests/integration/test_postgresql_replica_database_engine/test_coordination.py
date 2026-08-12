import shlex
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.network import PartitionManager
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
    # `coord_replica` duplicates the value of `replica`, but is used only by the coordination settings of the
    # tests that change a macro in the configuration and restart the server. `replica` itself must not be
    # changed: some CI configurations put the database metadata on a remote disk whose endpoint contains
    # {replica}, so renaming it would relocate (and thereby lose) all databases of the instance.
    macros={"shard": "1", "replica": "coord_instance1", "coord_replica": "coord_instance1"},
)

instance2 = cluster.add_instance(
    "coord_instance2",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users_coordination.xml"],
    with_postgres=True,
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "1", "replica": "coord_instance2", "coord_replica": "coord_instance2"},
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


def marker_znode_exists(node):
    # The snapshot_completed marker lives directly under the coordination keeper path. Query the parent so the
    # lookup does not raise "No node" once the whole coordination path has been removed (the parent survives).
    parent, leaf = KEEPER_PATH_RESOLVED.rsplit("/", 1)
    coordination_path_exists = (
        int(
            node.query(
                f"SELECT count() FROM system.zookeeper "
                f"WHERE path = '{parent}' AND name = '{leaf}'"
            )
        )
        > 0
    )
    if not coordination_path_exists:
        return False
    return (
        int(
            node.query(
                f"SELECT count() FROM system.zookeeper "
                f"WHERE path = '{KEEPER_PATH_RESOLVED}' AND name = 'snapshot_completed'"
            )
        )
        > 0
    )


def wait_for_marker(node, timeout=90):
    for _ in range(timeout):
        if marker_znode_exists(node):
            return
        time.sleep(1)
    raise AssertionError("snapshot_completed marker did not appear")


def count_in_all_logs(node, message):
    # `ClickHouseInstance.count_in_log` only greps the current log file, so a log rotation between the
    # baseline and the check makes the count go *down* and a wait against that baseline never succeeds.
    # Count over the rotated (and compressed) files as well, which is monotonic.
    return int(
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"zcat -f /var/log/clickhouse-server/clickhouse-server.log* 2>/dev/null "
                f"| grep -a -c -F {shlex.quote(message)} || true",
            ]
        ).strip()
    )


def wait_for_new_log_occurrence(node, message, baseline, timeout=60):
    # `wait_for_log_line` tails the log from the moment the tail process attaches, so a line that is
    # logged only once, milliseconds after the triggering query returns, can be missed entirely (the
    # background recovery can be faster than `docker exec` starting the tail). Poll the occurrence
    # count against a baseline taken before the trigger instead.
    deadline = time.time() + timeout
    while time.time() < deadline:
        if count_in_all_logs(node, message) > baseline:
            return
        time.sleep(0.5)
    raise AssertionError(f"'{message}' did not appear in the log within {timeout} seconds")


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


def test_last_replica_drop_removes_marker_and_recreate_redoes_snapshot(started_cluster):
    # The last-replica teardown must remove the shared coordination state - in particular the
    # snapshot_completed marker - so that recreating the coordinated database on the same keeper path redoes
    # the initial snapshot instead of resuming from confirmed_flush_lsn into empty tables. The decision is made
    # (and, for the last replica, the marker removed) before the nested tables are dropped, so a failure while
    # dropping them can never leave the marker behind after the last copy is gone. Dropping a non-last replica
    # must keep that state - it is still a live copy of the shared data.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    wait_for_marker(instance)

    leader_name = wait_for_leader(instance)
    leader_instance = instance if leader_name == "coord_instance1" else instance2
    leader_manager = pg_manager if leader_name == "coord_instance1" else pg_manager2
    standby_manager = pg_manager2 if leader_name == "coord_instance1" else pg_manager

    # Dropping a standby (not the last replica) must keep the shared coordination state, including the marker.
    standby_manager.drop_materialized_db()
    assert marker_znode_exists(leader_instance)
    assert replication_slot_exists()
    assert publication_exists()

    # Dropping the last replica removes the whole coordination path (and the marker) from Keeper.
    leader_manager.drop_materialized_db()
    assert not marker_znode_exists(leader_instance)
    assert not replication_slot_exists()
    assert not publication_exists()

    # Recreate on the same keeper path: with the marker gone, the initial snapshot is redone and every row
    # replicated before the drop is copied again (a surviving marker would instead resume into empty tables).
    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 100
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 100


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


def test_adopted_table_set_survives_publication_recreation(started_cluster):
    # A joiner whose explicit `materialized_postgresql_tables_list` disagrees with the shared publication
    # adopts the publication's table set (see the test above). That adoption must also drive a later
    # publication recreation: if the shared publication goes missing and the joiner is the replica that
    # recreates it, the recreated publication must contain the adopted set, not the joiner's stale local
    # list - otherwise the replicas would silently diverge on which tables actually replicate.
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("extra_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # The first replica creates the shared publication for test_table only and becomes the leader.
    first_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=first_settings
    )
    check_tables_are_synchronized(instance, "test_table")
    wait_for_leader(instance, expected="coord_instance1")

    # The joiner's list has an extra table; the publication's set is adopted over it.
    second_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table, extra_table'"
    ]
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=second_settings
    )
    check_tables_are_synchronized(instance2, "test_table")

    # Drop the shared publication behind the setup's back, then kill the leader: the joiner takes over
    # and its `startSynchronization` recreates the missing publication.
    for (pubname,) in pg_query("SELECT pubname FROM pg_publication"):
        pg_query(f'DROP PUBLICATION "{pubname}"')
    assert not publication_exists()
    instance.stop_clickhouse()
    wait_for_leader(instance2, expected="coord_instance2")

    deadline = time.time() + 90
    while time.time() < deadline:
        if publication_exists():
            break
        time.sleep(1)
    assert publication_exists()

    # The recreated publication must carry the adopted set - test_table only, no extra_table.
    published = sorted(
        row[0] for row in pg_query("SELECT tablename FROM pg_publication_tables")
    )
    assert published == ["test_table"]

    # Replication keeps flowing through the recreated publication.
    instance2.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200

    instance.start_clickhouse()
    check_tables_are_synchronized(instance, "test_table")


def test_adopted_table_set_survives_restart_and_publication_recreation(started_cluster):
    # The adoption of the shared publication's table set over a mismatching explicit
    # `materialized_postgresql_tables_list` must also survive a restart of the joiner. A restarted replica
    # rebuilds its handler from the persisted (stale) setting, so if the shared publication happens to be
    # missing at that moment, deriving the table set from the setting would both recreate the publication
    # with the wrong set and be refused by the `<keeper_path>/table_set` fence - wedging the replica in a
    # retry loop exactly in the recovery path that is supposed to repair the publication. The fenced set in
    # Keeper is authoritative in that situation.
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("extra_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    first_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=first_settings
    )
    check_tables_are_synchronized(instance, "test_table")
    wait_for_leader(instance, expected="coord_instance1")

    second_settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table, extra_table'"
    ]
    pg_manager2.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=second_settings
    )
    check_tables_are_synchronized(instance2, "test_table")

    # Take the whole setup down before removing the publication, so that no live replica can recreate it
    # from its in-memory adopted list: the joiner must come back up with only its stale persisted setting.
    instance.stop_clickhouse()
    instance2.stop_clickhouse()
    for (pubname,) in pg_query("SELECT pubname FROM pg_publication"):
        pg_query(f'DROP PUBLICATION "{pubname}"')
    assert not publication_exists()

    instance2.start_clickhouse()

    deadline = time.time() + 90
    while time.time() < deadline:
        if publication_exists():
            break
        time.sleep(1)
    assert publication_exists()

    # The publication is recreated from the fenced shared set - test_table only, no extra_table.
    published = sorted(
        row[0] for row in pg_query("SELECT tablename FROM pg_publication_tables")
    )
    assert published == ["test_table"]
    assert "extra_table" not in instance2.query("SHOW TABLES FROM test_database")

    # Replication keeps flowing through the recreated publication.
    check_tables_are_synchronized(instance2, "test_table")
    instance2.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200

    instance.start_clickhouse()
    check_tables_are_synchronized(instance, "test_table")


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


def test_keeper_path_rejects_uuid_macro_for_a_plain_create(started_cluster):
    # {uuid} expands to the UUID of the database being created. A plain CREATE DATABASE generates that UUID
    # locally, so every replica gets a different one: the replicas would sit on disjoint Keeper subtrees while
    # still contending for the same PostgreSQL replication slot and publication (their names are derived from
    # the PostgreSQL source, not from the keeper path), each believing it is the only active worker. Reject it
    # at CREATE time unless the UUID is carried by the DDL itself.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_uuid_path "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{{shard}}/{{uuid}}/test'"
    )
    assert "{uuid}" in error
    # The rejected CREATE must not have left a database behind.
    assert "test_uuid_path" not in instance.query("SHOW DATABASES")

    # An explicit UUID clause makes the value part of the DDL, so it is identical on every replica that runs
    # the same statement: {uuid} is accepted then, and the setup replicates normally.
    pg_manager.create_postgres_table("uuid_path_table")
    instance.query(
        "INSERT INTO postgres_database.uuid_path_table SELECT number, number FROM numbers(50)"
    )
    try:
        instance.query(
            f"CREATE DATABASE test_uuid_path UUID '11111111-1111-1111-1111-111111111111' "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
            f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
            f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{{shard}}/{{uuid}}/uuid_path_test', "
            f"materialized_postgresql_tables_list = 'uuid_path_table'"
        )
        check_tables_are_synchronized(
            instance,
            "uuid_path_table",
            postgres_database="postgres_database",
            materialized_database="test_uuid_path",
        )
    finally:
        instance.query("DROP DATABASE IF EXISTS test_uuid_path SYNC")


def test_keeper_path_rejects_per_server_macro(started_cluster):
    # The keeper path must also resolve to the same value on every server, so a per-server macro like
    # {server_uuid} is rejected for the same reason as {replica}: it would place each server on a disjoint
    # Keeper subtree. Validation expands the path with different injected macro values and rejects it when the
    # result differs, so it catches {server_uuid} even though the literal path contains no {replica} token.
    error = instance.query_and_get_error(
        f"CREATE DATABASE test_per_server_path "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{{shard}}/{{server_uuid}}/test'"
    )
    assert "{server_uuid}" in error
    assert "materialized_postgresql_replica_name" in error
    # The rejected CREATE must not have left a database behind.
    assert "test_per_server_path" not in instance.query("SHOW DATABASES")


def test_replica_name_must_be_a_single_keeper_component(started_cluster):
    # The replica name becomes a single Keeper node under <keeper_path>/replicas, and the last-replica fence
    # fires by removing that /replicas node once it is empty. A name containing '/' would nest the
    # registration one level deeper, so /replicas would never become empty, the fence could never fire and
    # the shared replication slot, publication and snapshot marker would leak forever. An empty name would
    # collide with the /replicas node itself. Both must be rejected at CREATE time.
    for replica_name in ["{shard}/{replica}", ""]:
        error = instance.query_and_get_error(
            f"CREATE DATABASE test_bad_replica_name "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
            f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
            f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{{shard}}/bad_replica_name_test', "
            f"materialized_postgresql_replica_name = '{replica_name}'"
        )
        assert "materialized_postgresql_replica_name" in error
        # The rejected CREATE must not have left a database behind.
        assert "test_bad_replica_name" not in instance.query("SHOW DATABASES")

    # A name that is a single Keeper component is accepted on the very same keeper path and replicates.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        settings=[
            "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
            "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{shard}/bad_replica_name_test'",
            "materialized_postgresql_replica_name = 'good_name'",
            "materialized_postgresql_tables_list = 'test_table'",
        ],
    )
    check_tables_are_synchronized(instance, "test_table")


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


def replica_registered(node, replica_name):
    return (
        int(
            node.query(
                f"SELECT count() FROM system.zookeeper "
                f"WHERE path = '{KEEPER_PATH_RESOLVED}/replicas' AND name = '{replica_name}'"
            )
        )
        > 0
    )


def test_drop_immediately_after_restart_unregisters_replica(started_cluster):
    # On attach/restart the persistent <keeper_path>/replicas/<name> registration and the coordinated nested
    # tables already exist on disk / in Keeper, but the in-memory replication handler is rebuilt only by the
    # background startup task. A DROP DATABASE issued in that window must still run the coordinated teardown
    # (built from the persisted settings) - unregistering this replica and, if it is the last one, removing the
    # shared state - rather than deleting the local nested tables while leaving the /replicas node and the shared
    # slot/publication/marker behind (a later last-replica drop would then keep the shared state around a ghost).
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    leader_name = wait_for_leader(instance)
    leader_instance = instance if leader_name == "coord_instance1" else instance2
    leader_manager = pg_manager if leader_name == "coord_instance1" else pg_manager2
    standby = instance2 if leader_name == "coord_instance1" else instance
    standby_name = "coord_instance2" if leader_name == "coord_instance1" else "coord_instance1"

    # The registration is a persistent node, so it survives the restart.
    assert replica_registered(leader_instance, standby_name)

    # Restart the standby and drop the database immediately, before the background startup task has reconnected
    # to PostgreSQL and rebuilt the replication handler.
    standby.restart_clickhouse()
    # SYNC so the nested table's Keeper subtree is fully removed before the assertions (and before any
    # following test recreates a database on the same keeper path).
    standby.query("DROP DATABASE test_database SYNC")

    # The standby must have unregistered itself even though its handler had not been rebuilt from a running
    # startup - the teardown was constructed from the persisted settings.
    assert not replica_registered(leader_instance, standby_name)
    # It was not the last replica, so the shared state is kept for the leader.
    assert replication_slot_exists()
    assert publication_exists()
    assert marker_znode_exists(leader_instance)
    check_tables_are_synchronized(leader_instance, "test_table")

    # Dropping the leader (now the last replica) removes the shared state. This only holds if the standby's
    # registration was correctly cleaned above; otherwise the leader would see a ghost replica and keep it.
    leader_manager.drop_materialized_db()
    assert not replication_slot_exists()
    assert not publication_exists()
    assert not marker_znode_exists(leader_instance)


def test_concurrent_drop_on_both_replicas_removes_shared_state(started_cluster):
    # Two replicas dropping the coordinated database at the same time must not both decide they are the last
    # replica. The decision is fenced on the shared /replicas node (removing the empty parent succeeds for
    # exactly one caller), so after both drops complete the shared slot, publication and snapshot marker are
    # removed exactly once and no ghost registration is left behind.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    wait_for_marker(instance)

    errors = {}

    def drop(node, key):
        try:
            # SYNC so both drops have fully removed their nested tables' Keeper subtrees before the
            # assertions (and before any following test recreates a database on the same keeper path).
            node.query("DROP DATABASE test_database SYNC")
            errors[key] = ""
        except Exception as e:  # noqa: BLE001
            errors[key] = str(e)

    threads = [
        threading.Thread(target=drop, args=(instance, "n1")),
        threading.Thread(target=drop, args=(instance2, "n2")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors["n1"] == "", errors["n1"]
    assert errors["n2"] == "", errors["n2"]
    assert "test_database" not in instance.query("SHOW DATABASES")
    assert "test_database" not in instance2.query("SHOW DATABASES")
    # The shared state was torn down (exactly one replica acted as the last one, so it was removed once).
    assert not replication_slot_exists()
    assert not publication_exists()
    assert not marker_znode_exists(instance)


def test_leaked_publication_is_not_adopted_by_fresh_coordinated_create(started_cluster):
    # If the last-replica teardown removed the Keeper coordination nodes but then failed to drop the shared
    # publication in PostgreSQL, the publication leaks - with the table set of the OLD setup. A fresh
    # coordinated CREATE on the same keeper path must not silently adopt that stale table set: with no
    # surviving coordination state (no snapshot marker and no registered replicas) nothing can be consuming
    # through the publication, so it is dropped and recreated for the new setup.
    pg_manager.create_postgres_table("test_table")
    pg_manager.create_postgres_table("old_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    instance.query(
        "INSERT INTO postgres_database.old_table SELECT number, number FROM numbers(10)"
    )

    # Learn the engine's generated publication name from a real setup, then drop the setup cleanly.
    create_coordinated_db("old_table")
    check_tables_are_synchronized(instance, "old_table")
    publications = pg_query("SELECT pubname FROM pg_publication")
    assert len(publications) == 1
    publication_name = publications[0][0]
    pg_manager2.drop_materialized_db()
    pg_manager.drop_materialized_db()
    assert not publication_exists()
    assert not marker_znode_exists(instance)

    # Simulate the leak: the old setup's publication (FOR TABLE ONLY old_table) survives while all of its
    # coordination state in Keeper is gone.
    pg_query(f'CREATE PUBLICATION "{publication_name}" FOR TABLE ONLY "old_table"')

    # A fresh coordinated CREATE with a different tables list must replace the leaked publication instead
    # of adopting its stale table set.
    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    publication_tables = {
        row[0]
        for row in pg_query(
            f"SELECT tablename FROM pg_publication_tables WHERE pubname = '{publication_name}'"
        )
    }
    assert publication_tables == {"test_table"}
    assert "old_table" not in instance.query("SHOW TABLES FROM test_database").split()

    pg_query('DROP TABLE IF EXISTS "old_table"')


def test_refused_drop_in_restart_window_does_not_disable_startup(started_cluster):
    # A fail-close refused DROP DATABASE deactivates the background startup task up front. In the
    # attach/restart window - where the replication handler has not been rebuilt yet - the refusal must
    # reactivate the task, otherwise the database stays mounted but never rebuilds its handler (no leader
    # election, no failover) until a server restart.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    leader_name = wait_for_leader(instance)
    leader_instance = instance if leader_name == "coord_instance1" else instance2
    standby = instance2 if leader_name == "coord_instance1" else instance
    standby_name = "coord_instance2" if leader_name == "coord_instance1" else "coord_instance1"

    zk_nodes = ["zoo1", "zoo2", "zoo3"]
    try:
        # Restart the standby while Keeper is down, so its background startup task cannot finish rebuilding
        # the replication handler before the drop arrives.
        standby.stop_clickhouse()
        started_cluster.stop_zookeeper_nodes(zk_nodes)
        standby.start_clickhouse()

        error = standby.query_and_get_error("DROP DATABASE test_database")
        assert error != ""
        assert "test_database" in standby.query("SHOW DATABASES")
    finally:
        started_cluster.start_zookeeper_nodes(zk_nodes)
        for _ in range(120):
            try:
                standby.query("SELECT count() FROM system.zookeeper WHERE path = '/'")
                break
            except Exception:
                time.sleep(1)

    # With Keeper reachable again, the reactivated startup task must rebuild the standby's handler without
    # a server restart: when the leader goes away, the standby takes over the leadership and keeps
    # replicating from PostgreSQL.
    leader_instance.stop_clickhouse()
    wait_for_leader(standby, expected=standby_name)
    standby.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(standby, "test_table")


def test_refused_drop_after_handler_shutdown_recovers_database(started_cluster):
    # The only teardown step that runs after the replication handler has been stopped is the last
    # replica's removal of the shared coordination nodes. If it fails, the refused DROP DATABASE must
    # not leave the database mounted but dead: the stopped handler is discarded and the startup task
    # rebuilds replication from scratch.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # A single replica, so its DROP DATABASE takes the last-replica path.
    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    check_tables_are_synchronized(instance, "test_table")
    wait_for_marker(instance)

    try:
        instance.query(
            "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_teardown_after_shutdown"
        )
        error = instance.query_and_get_error("DROP DATABASE test_database SYNC")
        assert "Injected failure" in error
        assert "test_database" in instance.query("SHOW DATABASES")
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_teardown_after_shutdown"
        )

    # Replication must recover without a server restart. There is no peer to receive the rows from,
    # so their arrival proves this replica's own rebuilt handler is consuming again.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 200

    # A retried drop succeeds and removes the shared state.
    instance.query("DROP DATABASE test_database SYNC")
    assert not replication_slot_exists()
    assert not publication_exists()


def test_refused_drop_after_handler_shutdown_recovers_single_table_engine(started_cluster):
    # Same scenario for the coordinated single-table engine: a DROP TABLE refused after the handler
    # was stopped re-arms the handler's retrying startup path, so the table resumes replicating
    # instead of staying mounted but dead until a server restart.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    single_table_keeper_path = "/clickhouse/mat_pg/{shard}/single_table"
    instance.query(
        f"CREATE TABLE test_single_table (key Int64, value Int64) "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
        f"PRIMARY KEY key "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{single_table_keeper_path}', "
        f"materialized_postgresql_replica_name = '{{replica}}'",
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )

    def wait_for_count(expected, timeout=90):
        for _ in range(timeout):
            try:
                if int(instance.query("SELECT count() FROM test_single_table")) == expected:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise AssertionError(f"test_single_table did not reach {expected} rows")

    try:
        wait_for_count(100)

        try:
            instance.query(
                "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_teardown_after_shutdown"
            )
            error = instance.query_and_get_error("DROP TABLE test_single_table SYNC")
            assert "Injected failure" in error
            assert "test_single_table" in instance.query("SHOW TABLES").split()
        finally:
            instance.query(
                "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_teardown_after_shutdown"
            )

        # The single replica's own handler must resume consuming (no peer to replicate from).
        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        wait_for_count(200)

        # A retried drop succeeds and removes the shared state.
        instance.query("DROP TABLE test_single_table SYNC")
    finally:
        instance.query("DROP TABLE IF EXISTS test_single_table SYNC")

    assert len(pg_query("SELECT slot_name FROM pg_replication_slots")) == 0
    assert not publication_exists()


def test_refused_drop_when_nested_table_drop_fails_recovers_database(started_cluster):
    # The coordinated teardown stops the replication handler before the generic DROP DATABASE path removes
    # this replica's local nested tables. If that later nested-table drop itself throws (e.g. Keeper
    # disappears while the nested ReplicatedReplacingMergeTree deletes its own Keeper metadata), the refused
    # DROP must not leave the database mounted but dead: onDropDatabaseFailed discards the stopped handler
    # and the startup task rebuilds replication from scratch.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    # A single replica, so its DROP DATABASE takes the last-replica path.
    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    check_tables_are_synchronized(instance, "test_table")
    wait_for_marker(instance)

    try:
        instance.query(
            "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
        )
        error = instance.query_and_get_error("DROP DATABASE test_database SYNC")
        assert "Injected failure while dropping a nested table" in error
        assert "test_database" in instance.query("SHOW DATABASES")
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
        )

    # Replication must recover without a server restart. There is no peer to receive the rows from,
    # so their arrival proves this replica's own rebuilt handler is consuming again.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )

    # The recovery drops the shut-down nested table and recreates it, so the table is transiently
    # absent (UNKNOWN_TABLE), and the recreate on the just-dropped Keeper path can itself hit a
    # transient "dropped right now" error that the retrying startup resolves - poll instead of
    # failing on the first error.
    def wait_for_database_count(expected, timeout=120):
        for _ in range(timeout):
            try:
                count = int(
                    instance.query("SELECT count() FROM test_database.test_table")
                )
                if count == expected:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise AssertionError(f"test_database.test_table did not reach {expected} rows")

    wait_for_database_count(200)
    check_tables_are_synchronized(instance, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 200

    # A retried drop succeeds and removes the shared state.
    instance.query("DROP DATABASE test_database SYNC")
    assert not replication_slot_exists()
    assert not publication_exists()


def test_refused_drop_when_nested_table_drop_fails_recovers_single_table_engine(started_cluster):
    # Same scenario for the coordinated single-table engine: the teardown runs in shutdown(is_drop) and stops
    # the handler, then dropInnerTableIfAny removes the local nested table. If that nested-table drop throws,
    # the refused DROP TABLE re-arms the handler's retrying startup path instead of leaving the table dead.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    single_table_keeper_path = "/clickhouse/mat_pg/{shard}/single_table"
    instance.query(
        f"CREATE TABLE test_single_table (key Int64, value Int64) "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
        f"PRIMARY KEY key "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{single_table_keeper_path}', "
        f"materialized_postgresql_replica_name = '{{replica}}'",
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )

    def wait_for_count(expected, timeout=90):
        for _ in range(timeout):
            try:
                if int(instance.query("SELECT count() FROM test_single_table")) == expected:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise AssertionError(f"test_single_table did not reach {expected} rows")

    try:
        wait_for_count(100)

        try:
            instance.query(
                "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
            )
            error = instance.query_and_get_error("DROP TABLE test_single_table SYNC")
            assert "Injected failure while dropping the local nested table" in error
            assert "test_single_table" in instance.query("SHOW TABLES").split()
        finally:
            instance.query(
                "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
            )

        # The single replica's own handler must resume consuming (no peer to replicate from).
        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        wait_for_count(200)

        # A retried drop succeeds and removes the shared state.
        instance.query("DROP TABLE test_single_table SYNC")
    finally:
        instance.query("DROP TABLE IF EXISTS test_single_table SYNC")

    assert len(pg_query("SELECT slot_name FROM pg_replication_slots")) == 0
    assert not publication_exists()


def test_join_with_different_naming_settings_is_rejected(started_cluster):
    # All coordinated replicas derive the ClickHouse names of the shared nested tables from the shared
    # publication through their local naming settings (materialized_postgresql_tables_list_with_schema,
    # materialized_postgresql_schema, materialized_postgresql_schema_list). A joining replica that
    # disagrees on them would adopt the same publication and slot but build a differently named -
    # disjoint - replicated tree: it would never receive the leader's data through ClickHouse
    # replication, yet on failover it would still resume the shared slot from confirmed_flush_lsn,
    # silently losing every pre-failover row. Such a join must be rejected synchronously at CREATE time.
    # A dedicated keeper path: this test publishes a schema-qualified naming fingerprint there, which
    # must not interact with the other tests sharing KEEPER_PATH.
    naming_keeper_settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{shard}/naming_test'",
        "materialized_postgresql_replica_name = '{replica}'",
        "materialized_postgresql_tables_list = 'naming_schema.naming_table'",
    ]
    naming_settings = naming_keeper_settings + [
        "materialized_postgresql_tables_list_with_schema = 1"
    ]

    pg_manager.create_postgres_schema("naming_schema")
    try:
        # check_tables_are_synchronized reads the expected rows through a `PostgreSQL` database engine that
        # is scoped to the default (public) schema, but this table lives in `naming_schema` - so give each
        # instance a comparison database scoped to that schema.
        pg_manager.create_clickhouse_postgres_db(
            database_name="postgres_database_naming",
            schema_name="naming_schema",
            postgres_database="postgres_database",
        )
        pg_manager2.create_clickhouse_postgres_db(
            database_name="postgres_database_naming",
            schema_name="naming_schema",
            postgres_database="postgres_database",
        )
        pg_query(
            'CREATE TABLE "naming_schema"."naming_table" '
            "(key Integer NOT NULL, value Integer, PRIMARY KEY(key))"
        )
        pg_query(
            'INSERT INTO "naming_schema"."naming_table" SELECT i, i FROM generate_series(0, 99) AS i'
        )

        # The first replica names the nested table `naming_schema.naming_table`.
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=naming_settings,
        )
        check_tables_are_synchronized(
            instance,
            "naming_table",
            schema_name="naming_schema",
            postgres_database="postgres_database_naming",
        )

        # The joining replica omits materialized_postgresql_tables_list_with_schema, so from the very
        # same publication it would derive `naming_table` instead of `naming_schema.naming_table`.
        error = instance2.query_and_get_error(
            f"CREATE DATABASE test_database "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
            f"SETTINGS {', '.join(naming_keeper_settings)}"
        )
        assert "naming-affecting settings" in error
        assert "materialized_postgresql_tables_list_with_schema" in error
        assert "test_database" not in instance2.query("SHOW DATABASES").split()

        # With the identical naming settings the join converges on the same table names.
        pg_manager2.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=naming_settings,
        )
        check_tables_are_synchronized(
            instance2,
            "naming_table",
            schema_name="naming_schema",
            postgres_database="postgres_database_naming",
        )
    finally:
        pg_manager.drop_materialized_db()
        pg_manager2.drop_materialized_db()
        pg_manager.drop_clickhouse_postgres_db("postgres_database_naming")
        pg_manager2.drop_clickhouse_postgres_db("postgres_database_naming")
        pg_query("DROP SCHEMA IF EXISTS naming_schema CASCADE")


def test_single_table_engine_cannot_join_database_engine_keeper_path(started_cluster):
    # The /naming fingerprint carries the PostgreSQL source identity, not only the ClickHouse-side
    # naming settings. A coordinated single-table engine on `db.table` and a coordinated database
    # engine with materialized_postgresql_tables_list = 'table' derive the same table set, but
    # different PostgreSQL slot/publication names (`db_table_*` vs `db_*`): sharing one keeper path
    # they would share the /leader and /replicas bookkeeping without sharing the underlying
    # slot/publication, so dropping one setup could tear down or leak the other's PostgreSQL objects.
    # Such a CREATE must be rejected synchronously.
    identity_keeper_path = "/clickhouse/mat_pg/{shard}/source_identity_test"
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        f"materialized_postgresql_keeper_path = '{identity_keeper_path}'",
        "materialized_postgresql_replica_name = '{replica}'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]
    try:
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=settings,
        )
        check_tables_are_synchronized(instance, "test_table")

        # The database engine has published its source identity (an empty source table name) at
        # /naming. The single-table engine replicating postgres_database.test_table would derive a
        # different slot and publication, so it must not join the same keeper path.
        error = instance2.query_and_get_error(
            f"CREATE TABLE test_single_table_identity (key Int64, value Int64) "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
            f"PRIMARY KEY key "
            f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
            f"materialized_postgresql_keeper_path = '{identity_keeper_path}', "
            f"materialized_postgresql_replica_name = '{{replica}}'",
            settings={"allow_experimental_materialized_postgresql_table": 1},
        )
        assert "source identity" in error
        assert (
            "test_single_table_identity"
            not in instance2.query("SHOW TABLES").split()
        )

        # A database engine with the identical source identity still joins.
        pg_manager2.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=settings,
        )
        check_tables_are_synchronized(instance2, "test_table")
    finally:
        pg_manager.drop_materialized_db()
        pg_manager2.drop_materialized_db()


def test_bad_macro_in_coordination_settings_is_rejected_at_create(started_cluster):
    # A misspelled or unsupported macro in materialized_postgresql_keeper_path or
    # materialized_postgresql_replica_name must fail the CREATE synchronously. Without CREATE-time
    # expansion it would only surface in the background startup task (which constructs the replication
    # handler), leaving a mounted database stuck retrying forever.
    base = (
        f"CREATE DATABASE test_bad_macro "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
    )

    error = instance.query_and_get_error(
        base
        + "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{no_such_macro}/test'"
    )
    assert "no_such_macro" in error

    error = instance.query_and_get_error(
        base
        + f"materialized_postgresql_keeper_path = '{KEEPER_PATH}', "
        + "materialized_postgresql_replica_name = '{no_such_macro}'"
    )
    assert "no_such_macro" in error

    assert "test_bad_macro" not in instance.query("SHOW DATABASES").split()


def test_duplicate_replica_name_is_rejected(started_cluster):
    # materialized_postgresql_replica_name must resolve to a distinct value on every replica: the
    # <keeper_path>/replicas/<name> children are the shared bookkeeping behind the last-replica
    # decision. The registration node stores the owning replica's identity, so a second replica that
    # resolves the setting to an already-registered name is rejected - synchronously at CREATE time,
    # since the peer's registration is already visible in Keeper - instead of silently collapsing both
    # replicas onto one node (where one replica's unregistration would delete the other live replica's
    # registration, and a later drop could tear down the shared slot/publication around it).
    keeper_path_resolved = "/clickhouse/mat_pg/1/dup_name_test"
    dup_settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{shard}/dup_name_test'",
        "materialized_postgresql_replica_name = 'duplicate_name'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    try:
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=dup_settings
        )
        check_tables_are_synchronized(instance, "test_table")

        # The joining replica reuses the first replica's name and must be rejected.
        error = instance2.query_and_get_error(
            f"CREATE DATABASE test_database "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
            f"SETTINGS {', '.join(dup_settings)}"
        )
        assert "already registered" in error
        assert "test_database" not in instance2.query("SHOW DATABASES").split()

        # The rejected CREATE did not disturb the first replica: its registration node survives and
        # replication keeps working.
        assert (
            int(
                instance.query(
                    f"SELECT count() FROM system.zookeeper "
                    f"WHERE path = '{keeper_path_resolved}/replicas' AND name = 'duplicate_name'"
                )
            )
            == 1
        )
        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        check_tables_are_synchronized(instance, "test_table")

        # A distinct replica name joins the very same setup fine.
        pg_manager2.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=[
                s
                for s in dup_settings
                if not s.startswith("materialized_postgresql_replica_name")
            ]
            + ["materialized_postgresql_replica_name = 'distinct_name'"],
        )
        check_tables_are_synchronized(instance2, "test_table")
    finally:
        pg_manager2.drop_materialized_db()
        pg_manager.drop_materialized_db()

    # Both replicas dropped: the last one removed the shared slot and publication.
    assert len(pg_query("SELECT slot_name FROM pg_replication_slots")) == 0
    assert not publication_exists()


def test_join_with_different_table_set_is_rejected(started_cluster):
    # The authoritative shared table set is fenced at <keeper_path>/table_set BEFORE a replica registers or
    # builds any nested table: the shared publication (from which joining replicas derive their set) is only
    # created later, by the elected active worker, so without the fence two fresh replicas starting
    # concurrently could derive different sets (different materialized_postgresql_tables_list values, or the
    # same empty setting around a source schema change) and silently build diverging nested tables on one
    # keeper path. Simulate the pre-publication window: another replica has already fenced a different table
    # set on this keeper path, and no publication exists yet - this replica must refuse to proceed.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    table_set_keeper_path = "/clickhouse/mat_pg/1/table_set_test"
    zk = started_cluster.get_kazoo_client("zoo1")
    try:
        zk.create(
            table_set_keeper_path + "/table_set",
            b"some_other_table\n",
            makepath=True,
        )

        settings = [
            "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
            f"materialized_postgresql_keeper_path = '{table_set_keeper_path}'",
            "materialized_postgresql_replica_name = '{replica}'",
            "materialized_postgresql_tables_list = 'test_table'",
        ]
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )

        # The mismatch is detected by the background startup task (the publication does not exist yet, so
        # the CREATE itself cannot know the fenced set): the join is refused fail-close and retried, and no
        # nested table may appear.
        instance.wait_for_log_line(
            "The table set this replica derived differs from the table set",
            timeout=60,
        )
        assert "test_table" not in instance.query("SHOW TABLES FROM test_database").split()
    finally:
        pg_manager.drop_materialized_db()
        zk.delete(table_set_keeper_path, recursive=True)
        zk.stop()


def test_create_is_rejected_while_teardown_token_is_held(started_cluster):
    # A last-replica drop removes the shared PostgreSQL slot and publication BY NAME only after the local
    # data is gone, so between its pre-data teardown (which removes the coordination nodes) and those drops
    # a fresh CREATE on the same keeper path could build a new setup whose objects the pending teardown
    # would then delete. The teardown therefore holds an ownership token at <keeper_path>/teardown - created
    # atomically with winning the last-replica fence and removed only after the PostgreSQL objects are gone -
    # and a CREATE on a path with a foreign token is rejected. Simulate a teardown that is still pending
    # (e.g. the tearing-down server died mid-way) by placing a foreign token.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    teardown_keeper_path = "/clickhouse/mat_pg/1/teardown_token_test"
    settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        f"materialized_postgresql_keeper_path = '{teardown_keeper_path}'",
        "materialized_postgresql_replica_name = '{replica}'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]

    zk = started_cluster.get_kazoo_client("zoo1")
    try:
        zk.create(
            teardown_keeper_path + "/teardown",
            b"dead-server-uuid|dead-database-uuid",
            makepath=True,
        )

        error = instance.query_and_get_error(
            f"CREATE DATABASE test_teardown_token "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
            f"SETTINGS " + ", ".join(settings)
        )
        assert "still being torn down" in error
        assert "test_teardown_token" not in instance.query("SHOW DATABASES")

        # Once the teardown has finished (the token is gone), the same CREATE succeeds and replicates.
        zk.delete(teardown_keeper_path + "/teardown")
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )
        check_tables_are_synchronized(instance, "test_table")
    finally:
        pg_manager.drop_materialized_db()
        if zk.exists(teardown_keeper_path):
            zk.delete(teardown_keeper_path, recursive=True)
        zk.stop()

    # The last-replica drop finished its teardown: it dropped the shared PostgreSQL objects and released
    # the keeper path by removing its own teardown token (and the then-empty path root).
    assert not replication_slot_exists()
    assert not publication_exists()


def test_coordination_settings_cannot_be_altered(started_cluster):
    # The coordination settings define the engine of the nested tables and this replica's coordination
    # identity, so they are CREATE-time-only: the nested replicated tree and the coordination state in
    # Keeper are already built from them. ALTER DATABASE ... MODIFY SETTING must reject them with an
    # actionable message instead of the generic "Unknown setting".
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50)"
    )

    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    check_tables_are_synchronized(instance, "test_table")

    for setting, value in [
        ("materialized_postgresql_table_engine", "'ReplacingMergeTree'"),
        ("materialized_postgresql_keeper_path", "'/clickhouse/mat_pg/other'"),
        ("materialized_postgresql_replica_name", "'other_replica'"),
    ]:
        error = instance.query_and_get_error(
            f"ALTER DATABASE test_database MODIFY SETTING {setting} = {value}"
        )
        assert setting in error and "can only be set at CREATE time" in error, error

    # The database is untouched and keeps replicating.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50, 50)"
    )
    check_tables_are_synchronized(instance, "test_table")


def test_alter_mutable_setting_on_standby_survives_failover(started_cluster):
    # A coordinated standby builds its replication handler at startup, but the handler creates its
    # consumer only after winning the leader election. `ALTER DATABASE ... MODIFY SETTING` of a mutable
    # setting must be accepted in that state (the handler stores the value for the consumer it creates
    # later) instead of failing with "initialization did not finish", and the standby that becomes the
    # leader after the ALTER must keep replicating.
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

    standby_node.query(
        "ALTER DATABASE test_database MODIFY SETTING materialized_postgresql_max_block_size = 12345"
    )
    assert "materialized_postgresql_max_block_size = 12345" in standby_node.query(
        "SHOW CREATE DATABASE test_database"
    )

    # The standby takes over and consumes with the altered setting.
    leader_node.stop_clickhouse()
    new_leader = wait_for_leader(standby_node, not_equal=leader_name)
    assert new_leader != leader_name
    standby_node.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(standby_node, "test_table")
    assert (
        int(standby_node.query("SELECT count() FROM test_database.test_table")) == 200
    )

    leader_node.start_clickhouse()
    check_tables_are_synchronized(leader_node, "test_table")


def test_plain_database_ddl_and_drop_in_startup_window(started_cluster):
    # In the attach/restart window the background startup task has not built the replication handler
    # yet, but a plain (non-coordinated) database is already mounted and accepts DDL. Public DDL must
    # not dereference the missing handler (ALTER of a mutable setting is applied to the persisted
    # metadata; ATTACH / DETACH TABLE are refused cleanly), and a DROP DATABASE in that window must
    # still remove the PostgreSQL publication and logical replication slot instead of leaking them.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50)"
    )

    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        settings=["materialized_postgresql_tables_list = 'test_table'"],
    )
    check_tables_are_synchronized(instance, "test_table")
    assert replication_slot_exists()
    assert publication_exists()

    # A table that is not replicated yet, for the ATTACH TABLE check below.
    pg_manager.create_postgres_table("late_table")

    failpoint_config_path = (
        "/etc/clickhouse-server/config.d/matpg_startup_failpoint.xml"
    )
    try:
        # Re-enter the startup window: restart the server with the failpoint enabled from the
        # configuration (a runtime `SYSTEM ENABLE FAILPOINT` would not survive the restart), so the
        # re-attached database keeps retrying its background startup before the replication handler
        # is built.
        instance.replace_config(
            failpoint_config_path,
            "<clickhouse><fail_points_active>"
            "<materialized_postgresql_fail_database_startup>1"
            "</materialized_postgresql_fail_database_startup>"
            "</fail_points_active></clickhouse>",
        )
        instance.restart_clickhouse()

        # Applied to the in-memory settings and the on-disk metadata; the handler built later picks
        # the new value up.
        instance.query(
            "ALTER DATABASE test_database MODIFY SETTING materialized_postgresql_max_block_size = 8192"
        )
        assert "materialized_postgresql_max_block_size = 8192" in instance.query(
            "SHOW CREATE DATABASE test_database"
        )

        error = instance.query_and_get_error("ATTACH TABLE test_database.late_table")
        assert "has not finished starting replication yet" in error, error

        error = instance.query_and_get_error(
            "DETACH TABLE test_database.test_table PERMANENTLY"
        )
        assert "has not finished starting replication yet" in error, error

        # The drop runs with the handler still null; the publication and the replication slot must
        # not be leaked in PostgreSQL.
        instance.query("DROP DATABASE test_database SYNC")
        assert not replication_slot_exists()
        assert not publication_exists()
    finally:
        instance.exec_in_container(["rm", "-f", failpoint_config_path])
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_database_startup"
        )


def test_coordinated_detach_in_startup_window_is_a_no_op_rejection(started_cluster):
    # In the attach/restart window `tryGetTable` wraps the nested tables on the fly. That wrapper must
    # carry the coordinated flag exactly like the published wrappers do: otherwise a name-based
    # DETACH TABLE ... PERMANENTLY (or DROP TABLE) in that window passes the storage-level
    # `checkTableCanBeDetached` / `checkTableCanBeDropped` guard, and `InterpreterDropQuery` calls
    # `flushAndShutdown` on the table - shutting the local nested replicated table down - before the
    # database-level method rejects the statement on the settings. The statement still fails, but the
    # promised no-op rejection is lost and replication of the table is silently broken.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50)"
    )

    settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        # An own keeper path so this setup cannot cross-contaminate the tests sharing KEEPER_PATH.
        "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{shard}/startup_window_ddl'",
        "materialized_postgresql_replica_name = '{replica}'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
    )
    check_tables_are_synchronized(instance, "test_table")

    failpoint_config_path = (
        "/etc/clickhouse-server/config.d/matpg_startup_failpoint.xml"
    )
    try:
        # Re-enter the startup window: restart the server with the failpoint enabled from the
        # configuration, so the re-attached database keeps retrying its background startup and the
        # wrapper map stays unpublished.
        instance.replace_config(
            failpoint_config_path,
            "<clickhouse><fail_points_active>"
            "<materialized_postgresql_fail_database_startup>1"
            "</materialized_postgresql_fail_database_startup>"
            "</fail_points_active></clickhouse>",
        )
        instance.restart_clickhouse()

        for query in [
            "DETACH TABLE test_database.test_table PERMANENTLY",
            "DROP TABLE test_database.test_table",
        ]:
            error = instance.query_and_get_error(query)
            assert "not supported for a coordinated MaterializedPostgreSQL" in error, error
    finally:
        instance.exec_in_container(["rm", "-f", failpoint_config_path])
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_database_startup"
        )

    # The rejections above must have been true no-ops: once the startup window closes, the nested
    # table must still accept the consumer's writes. Without the coordinated flag on the on-the-fly
    # wrapper, the refused DETACH has already shut the nested replicated table down, and this
    # convergence never happens.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50, 50)"
    )
    check_tables_are_synchronized(instance, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 100

    pg_manager.drop_materialized_db()


def test_plain_drop_database_quiesces_retrying_startup_task(started_cluster):
    # A plain (non-coordinated) DROP DATABASE must not race the background startup task.
    # `InterpreterDropQuery::executeToDatabaseImpl` calls `stopReplication` (which clears
    # `synchronization_started`) well before the database object is shut down; if `beforeDropDatabase`
    # left the startup task armed, a retry waking inside that window would re-enter
    # `startSynchronization` and recreate the PostgreSQL publication and replication slot (and even
    # nested tables) while the drop is already tearing the database down. Hold the drop inside the
    # window with a pauseable failpoint and let a pending startup retry become able to succeed:
    # nothing may be (re)created.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50)"
    )

    # Keep the background startup failing before it builds the replication handler, so the task
    # keeps retrying every few seconds and neither the slot nor the publication exists yet.
    instance.query(
        "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_database_startup"
    )
    drop_error = []
    drop_thread = None

    def run_drop():
        try:
            instance.query("DROP DATABASE test_database SYNC")
        except Exception as e:
            drop_error.append(e)

    try:
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=["materialized_postgresql_tables_list = 'test_table'"],
        )
        # At least one startup attempt must have failed (and rescheduled itself) before the drop
        # starts, so a retry is genuinely pending during the window below.
        instance.wait_for_log_line(
            "Injected failure of the MaterializedPostgreSQL database startup",
            timeout=30,
            look_behind_lines=1,
        )
        assert not replication_slot_exists()

        instance.query(
            "SYSTEM ENABLE FAILPOINT materialized_postgresql_pause_after_stop_replication"
        )
        pause_line = "Pausing after stopping replication"
        pause_baseline = count_in_all_logs(instance, pause_line)
        drop_thread = threading.Thread(target=run_drop)
        drop_thread.start()
        # The line is logged only once and possibly before a log tail could attach, so count
        # occurrences against the pre-drop baseline instead.
        wait_for_new_log_occurrence(instance, pause_line, pause_baseline, timeout=30)

        # With the drop held between `stopReplication` and the database shutdown, let the next
        # startup retry succeed - if the drop had left the task armed, it would rebuild the handler
        # and recreate the slot and the publication within its 5 second retry cadence.
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_database_startup"
        )
        for _ in range(12):
            assert not replication_slot_exists()
            assert not publication_exists()
            time.sleep(1)
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_database_startup"
        )
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_pause_after_stop_replication"
        )
        if drop_thread is not None:
            drop_thread.join()

    assert not drop_error, drop_error
    assert "test_database" not in instance.query("SHOW DATABASES").split()
    assert not replication_slot_exists()
    assert not publication_exists()


def test_plain_refused_drop_rearms_startup_task(started_cluster):
    # `beforeDropDatabase` deactivates the background startup task in every mode. When a plain
    # (non-coordinated) DROP DATABASE is then refused (here: an injected failure of the nested-table
    # drop), the database stays alive, so `onDropDatabaseFailed` must re-arm the task and discard the
    # handler that the generic drop path had already stopped via `stopReplication` - otherwise the
    # database would stay mounted but permanently not synchronizing until a server restart.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50)"
    )
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        settings=["materialized_postgresql_tables_list = 'test_table'"],
    )
    check_tables_are_synchronized(instance, "test_table")

    instance.query(
        "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
    )
    success_line = "Successfully loaded tables from PostgreSQL and started replication"
    success_baseline = count_in_all_logs(instance, success_line)
    try:
        error = instance.query_and_get_error("DROP DATABASE test_database SYNC")
        assert "Injected failure while dropping a nested table" in error, error

        # The re-armed startup task must rebuild the stopped replication without a server restart.
        # The recovery can complete within milliseconds of the refused drop, so count fresh
        # occurrences against a baseline taken before the drop instead of tailing the log.
        wait_for_new_log_occurrence(instance, success_line, success_baseline)
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
        )

    # The recovered database must be droppable for real, removing the shared PostgreSQL objects.
    instance.query("DROP DATABASE test_database SYNC")
    assert not replication_slot_exists()
    assert not publication_exists()


def test_refused_drop_recovery_window_keeps_wrapped_reads(started_cluster):
    # `stopReplication` in the generic drop path sets `replication_stopped` when it empties the wrapper
    # map, redirecting user-facing reads to the raw nested tables. When the drop is then refused,
    # `recoverAfterRefusedDrop` hands control back to the startup task, so it must also restore the
    # startup-window semantics by clearing `replication_stopped`: until `startSynchronization`
    # republishes the wrappers - indefinitely, if startup keeps failing and retrying - reads must wrap
    # the nested tables on the fly again. Otherwise the recovery window exposes stale and deleted row
    # versions (a PostgreSQL DELETE is a durable `_sign = -1` row in the nested table, hidden only by
    # the wrapper's forced FINAL and `_sign = 1` filter).
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(50)"
    )
    pg_manager.create_materialized_db(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        settings=["materialized_postgresql_tables_list = 'test_table'"],
    )
    check_tables_are_synchronized(instance, "test_table")

    # Leave durable tombstones in the nested table: the raw nested table keeps both the 25 live rows
    # and the 25 `_sign = -1` rows (ReplacingMergeTree keeps the newest version per key even across
    # merges), while a wrapped read sees exactly 25 rows.
    pg_query("DELETE FROM test_table WHERE key >= 25")
    check_tables_are_synchronized(instance, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 25

    startup_fail_line = "Injected failure of the MaterializedPostgreSQL database startup"
    startup_fail_baseline = count_in_all_logs(instance, startup_fail_line)
    success_line = "Successfully loaded tables from PostgreSQL and started replication"
    success_baseline = count_in_all_logs(instance, success_line)
    try:
        # Keep the re-armed startup failing after the refused drop, so the recovery window stays open
        # deterministically instead of closing within milliseconds when the wrappers are republished.
        instance.query(
            "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_database_startup"
        )
        instance.query(
            "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
        )
        error = instance.query_and_get_error("DROP DATABASE test_database SYNC")
        assert "Injected failure while dropping a nested table" in error, error

        # The drop already ran through `stopReplication`; recovery has re-armed the startup task,
        # which is now failing and retrying on the injected startup failure.
        wait_for_new_log_occurrence(instance, startup_fail_line, startup_fail_baseline)

        # A user-facing read in the recovery window must not expose the deleted row versions.
        assert (
            int(instance.query("SELECT count() FROM test_database.test_table")) == 25
        )
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_database_startup"
        )
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_nested_table_drop"
        )

    # Replication must come back once the injected failures are gone, and the database must be
    # droppable for real.
    wait_for_new_log_occurrence(instance, success_line, success_baseline)
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 50)"
    )
    check_tables_are_synchronized(instance, "test_table")
    instance.query("DROP DATABASE test_database SYNC")
    assert not replication_slot_exists()
    assert not publication_exists()


def test_registration_is_fenced_against_concurrent_teardown_token(started_cluster):
    # The teardown-token check in `ensureCoordinatedNamingCompatible` alone is only advisory: between that
    # check and the registration, the last replica of a previous setup can still win the teardown fence
    # (winning requires <keeper_path>/replicas to be empty, and this replica has not registered yet), and
    # would then still be entitled to drop the shared PostgreSQL slot/publication by name around the
    # joiner's fresh setup. The registration therefore asserts the token's absence atomically, in the same
    # Keeper multi-request that creates the registration node. Hold the startup exactly inside that window
    # with a pauseable failpoint, win the fence there (inject a foreign token), and verify the registration
    # refuses to join instead of proceeding.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    fence_keeper_path = "/clickhouse/mat_pg/1/register_fence_test"
    settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        f"materialized_postgresql_keeper_path = '{fence_keeper_path}'",
        "materialized_postgresql_replica_name = '{replica}'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]

    zk = started_cluster.get_kazoo_client("zoo1")
    pause_line = "Pausing before registering the replica"
    refused_line = "has concurrently begun being torn down"
    pause_baseline = count_in_all_logs(instance, pause_line)
    refused_baseline = count_in_all_logs(instance, refused_line)
    try:
        instance.query(
            "SYSTEM ENABLE FAILPOINT materialized_postgresql_pause_before_register_replica"
        )
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )
        # The background startup has passed the advisory token check (no token existed) and is now held
        # right before the registration.
        wait_for_new_log_occurrence(instance, pause_line, pause_baseline, timeout=60)

        # The last replica of a previous setup wins the teardown fence in this window.
        zk.create(
            fence_keeper_path + "/teardown",
            b"dead-server-uuid|dead-database-uuid",
            makepath=True,
        )
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_pause_before_register_replica"
        )

        # The released startup must fail the registration on the atomic token probe (and keep being
        # refused by the advisory check on every retry), leaving no registration and no nested table.
        wait_for_new_log_occurrence(instance, refused_line, refused_baseline, timeout=60)
        assert zk.get_children(fence_keeper_path + "/replicas") == []
        assert (
            "test_table" not in instance.query("SHOW TABLES FROM test_database").split()
        )

        # Once the pending teardown has finished (the token is gone), the retrying startup joins and
        # replicates.
        zk.delete(fence_keeper_path + "/teardown")
        check_tables_are_synchronized(instance, "test_table")
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_pause_before_register_replica"
        )
        pg_manager.drop_materialized_db()
        if zk.exists(fence_keeper_path):
            zk.delete(fence_keeper_path, recursive=True)
        zk.stop()

    assert not replication_slot_exists()
    assert not publication_exists()


def test_alter_mutable_setting_on_demoted_leader(started_cluster):
    # A former leader that loses its Keeper session is demoted back to standby: `coordinationFunc`
    # destroys its consumer, but the handler stays initialized. `ALTER DATABASE ... MODIFY SETTING` of a
    # mutable setting must be accepted in that state too (stored for the consumer built on the next
    # takeover) - gating the live update on `replication_handler_initialized` alone would throw
    # "Consumer not initialized" on every former-leader standby.
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
    standby_name = (
        "coord_instance2" if leader_name == "coord_instance1" else "coord_instance1"
    )

    # Demote the leader by cutting it off from Keeper until its session expires; the standby takes over.
    demotion_line = "Keeper session expired, releasing replication leadership"
    demotion_baseline = count_in_all_logs(leader_node, demotion_line)
    pm = PartitionManager()
    try:
        pm.drop_instance_zk_connections(leader_node)
        wait_for_new_log_occurrence(
            leader_node, demotion_line, demotion_baseline, timeout=120
        )
        wait_for_leader(standby_node, expected=standby_name)
    finally:
        pm.heal_all()

    # The demoted node is now a standby whose handler is initialized but has no consumer. The ALTER must
    # be accepted and stored.
    leader_node.query(
        "ALTER DATABASE test_database MODIFY SETTING materialized_postgresql_max_block_size = 12345"
    )
    assert "materialized_postgresql_max_block_size = 12345" in leader_node.query(
        "SHOW CREATE DATABASE test_database"
    )

    # The demoted node takes over again and consumes with the altered setting.
    standby_node.stop_clickhouse()
    wait_for_leader(leader_node, expected=leader_name)
    leader_node.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(leader_node, "test_table")
    assert int(leader_node.query("SELECT count() FROM test_database.test_table")) == 200

    standby_node.start_clickhouse()
    check_tables_are_synchronized(standby_node, "test_table")


def test_coordination_identity_must_stay_stable_across_restart(started_cluster):
    # `materialized_postgresql_keeper_path` and `materialized_postgresql_replica_name` are expanded from
    # the *current* server configuration on every startup, while the nested replicated tables keep the
    # expansion they were created with in their engine arguments and the <keeper_path>/replicas/<name>
    # registration is persistent. A configuration-only change of a macro they expand through must
    # therefore be refused: otherwise this replica would elect, register and tear down under a different
    # Keeper identity than the shared data it already owns, the old /replicas subtree could never drain
    # (leaking the shared slot, publication and snapshot marker forever) and leader election could be
    # split from the shared nested-table path.
    keeper_path_resolved = "/clickhouse/mat_pg/1/identity_test"
    identity_settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{shard}/identity_test'",
        "materialized_postgresql_replica_name = '{coord_replica}'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]
    # The macros of an integration-test instance are generated into conf.d, not config.d.
    macros_config = "/etc/clickhouse-server/conf.d/macros.xml"
    rejection_line = "coordination identity of this MaterializedPostgreSQL replica changed"

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    try:
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=identity_settings,
        )
        check_tables_are_synchronized(instance, "test_table")

        rejection_baseline = count_in_all_logs(instance, rejection_line)

        # Rename this replica in the server configuration only, and restart. The database metadata is
        # unchanged, so `{coord_replica}` now expands to a name this setup has never been registered
        # under. Only the dedicated coordination macro is changed: `{replica}` itself is part of the
        # database-disk endpoint in some CI configurations, so renaming it would relocate the metadata of
        # every database on the instance instead of exercising this check.
        instance.replace_in_config(
            macros_config,
            "<coord_replica>coord_instance1</coord_replica>",
            "<coord_replica>coord_instance1_renamed</coord_replica>",
        )
        instance.restart_clickhouse()

        wait_for_new_log_occurrence(
            instance, rejection_line, rejection_baseline, timeout=90
        )

        # The startup is refused before anything is registered or created under the new identity: the
        # only registration is still the original one.
        assert (
            instance.query(
                f"SELECT name FROM system.zookeeper "
                f"WHERE path = '{keeper_path_resolved}/replicas' ORDER BY name"
            ).split()
            == ["coord_instance1"]
        )

        # Restoring the configuration makes the retrying startup succeed and replication resume - the
        # refusal keeps the setup intact rather than breaking it.
        instance.replace_in_config(
            macros_config,
            "<coord_replica>coord_instance1_renamed</coord_replica>",
            "<coord_replica>coord_instance1</coord_replica>",
        )
        instance.restart_clickhouse()

        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        check_tables_are_synchronized(instance, "test_table")
        assert int(instance.query("SELECT count() FROM test_database.test_table")) == 200
    finally:
        # Never leave the renamed macro behind: every other test on this node depends on it.
        instance.replace_in_config(
            macros_config,
            "<coord_replica>coord_instance1_renamed</coord_replica>",
            "<coord_replica>coord_instance1</coord_replica>",
        )
        if not instance.get_process_pid("clickhouse"):
            instance.start_clickhouse()
        pg_manager.drop_materialized_db()

    assert not replication_slot_exists()
    assert not publication_exists()


def coordination_path_exists(node, path):
    parent, leaf = path.rsplit("/", 1)
    return (
        int(
            node.query(
                f"SELECT count() FROM system.zookeeper "
                f"WHERE path = '{parent}' AND name = '{leaf}'"
            )
        )
        > 0
    )


def assert_coordination_state_removed(node, path):
    # Everything the coordination protocol keeps under the keeper path must be gone after the last replica
    # was torn down. The keeper path node itself may survive as an empty leftover: the nested replicated
    # tables remove their own subtrees under <keeper_path>/tables asynchronously, so the teardown can only
    # remove the (then still not empty) parents on a best-effort basis - correctness over tidiness. What must
    # never survive is the state a recreate would act on.
    if not coordination_path_exists(node, path):
        return
    children = node.query(
        f"SELECT name FROM system.zookeeper WHERE path = '{path}' ORDER BY name"
    ).split()
    assert set(children) <= {"tables"}, children


@pytest.mark.parametrize("macro_change", ["rename", "remove"])
def test_drop_after_coordination_identity_change_tears_down_original_identity(
    started_cluster, macro_change
):
    # A configuration-only change of the macros the coordination settings expand through is refused at
    # startup, but the database stays mounted and droppable. The drop must then tear down the coordination
    # state that actually exists - the one persisted in the nested tables - and not the identity the current
    # configuration expands to: otherwise it would unregister and do the last-replica accounting under the
    # new identity, orphaning the original /replicas subtree together with the shared replication slot,
    # publication and snapshot_completed marker forever.
    #
    # "rename" changes the value of the {coord_replica} macro; "remove" renames the macro itself, so the
    # settings cannot be expanded at all - a case in which the handler must still be constructible, or the
    # database could never be dropped. The tests use a coordination-only macro because {replica} is part of
    # the database-disk endpoint in some CI configurations, where changing it would relocate the metadata of
    # every database on the instance.
    keeper_path_resolved = "/clickhouse/mat_pg/1/identity_drop_test"
    identity_settings = [
        "materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree'",
        "materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{shard}/identity_drop_test'",
        "materialized_postgresql_replica_name = '{coord_replica}'",
        "materialized_postgresql_tables_list = 'test_table'",
    ]
    macros_config = "/etc/clickhouse-server/conf.d/macros.xml"
    if macro_change == "rename":
        # Change the value of the {coord_replica} macro.
        original = "<coord_replica>coord_instance1</coord_replica>"
        changed = "<coord_replica>coord_instance1_renamed</coord_replica>"
    else:
        # Rename the macro itself, so {coord_replica} cannot be expanded at all any more.
        original = "<coord_replica>coord_instance1</coord_replica>"
        changed = "<coord_replica_gone>coord_instance1</coord_replica_gone>"

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    try:
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip,
            port=cluster.postgres_port,
            settings=identity_settings,
        )
        check_tables_are_synchronized(instance, "test_table")
        assert (
            instance.query(
                f"SELECT name FROM system.zookeeper "
                f"WHERE path = '{keeper_path_resolved}/replicas' ORDER BY name"
            ).split()
            == ["coord_instance1"]
        )

        instance.replace_in_config(macros_config, original, changed)
        instance.restart_clickhouse()

        # The drop succeeds and removes the whole original coordination path together with the shared
        # PostgreSQL objects - this replica was the only one, so it is the last replica of the setup that
        # the nested tables belong to.
        instance.query("DROP DATABASE test_database SYNC")
        assert_coordination_state_removed(instance, keeper_path_resolved)
        assert not replication_slot_exists()
        assert not publication_exists()
    finally:
        instance.replace_in_config(macros_config, changed, original)
        if not instance.get_process_pid("clickhouse"):
            instance.start_clickhouse()
        else:
            instance.restart_clickhouse()
        try:
            pg_manager.drop_materialized_db()
        except Exception:
            pass


def test_single_table_drop_after_coordination_identity_change_tears_down_original_identity(
    started_cluster,
):
    # Same for the coordinated single-table engine: after a configuration-only change of the macro its
    # coordination settings expand through, its DROP TABLE must unregister and make its last-replica decision
    # under the identity persisted in its nested table, not under the one the changed configuration expands to.
    keeper_path_resolved = "/clickhouse/mat_pg/1/single_table_identity"
    macros_config = "/etc/clickhouse-server/conf.d/macros.xml"

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )
    instance.query(
        f"CREATE TABLE test_single_table (key Int64, value Int64) "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
        f"PRIMARY KEY key "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '/clickhouse/mat_pg/{{shard}}/single_table_identity', "
        f"materialized_postgresql_replica_name = '{{coord_replica}}'",
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    try:
        for _ in range(90):
            try:
                if int(instance.query("SELECT count() FROM test_single_table")) == 100:
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise AssertionError("test_single_table did not reach 100 rows")

        instance.replace_in_config(
            macros_config,
            "<coord_replica>coord_instance1</coord_replica>",
            "<coord_replica>coord_instance1_renamed</coord_replica>",
        )
        instance.restart_clickhouse()

        instance.query("DROP TABLE test_single_table SYNC")
        assert_coordination_state_removed(instance, keeper_path_resolved)
        assert len(pg_query("SELECT slot_name FROM pg_replication_slots")) == 0
        assert not publication_exists()
    finally:
        instance.replace_in_config(
            macros_config,
            "<coord_replica>coord_instance1_renamed</coord_replica>",
            "<coord_replica>coord_instance1</coord_replica>",
        )
        if not instance.get_process_pid("clickhouse"):
            instance.start_clickhouse()
        else:
            instance.restart_clickhouse()
        instance.query("DROP TABLE IF EXISTS test_single_table SYNC")


def test_lost_leadership_during_snapshot_does_not_publish_stale_marker(started_cluster):
    # If the active worker's Keeper session expires while it is loading the initial snapshot, another
    # replica wins /leader, sees no snapshot_completed marker, truncates the shared nested tables and
    # starts a replacement snapshot. The deposed worker must then abort instead of publishing the marker:
    # a marker written by a worker that already lost its leadership describes a snapshot the replacement
    # has truncated away, so if the new leader dies before finishing its reload, the next leader would
    # trust the stale marker, skip initial_sync and permanently lose the rows that were never copied.
    # Both instances are parked right before the marker write by a pauseable failpoint; the deposed one
    # is released first, while the new leader is still mid-snapshot - exactly the window in which the
    # stale marker used to be written.
    pause_line = "Pausing before marking the initial snapshot as completed"
    # The deposed worker is parked inside its startup attempt, so the leadership-session fence aborts that
    # attempt and the leadership is released through the failed-startup path (not through the session-expiry
    # check of the coordination task, which only sees an already-released claim afterwards).
    abort_line = "Released replication leadership after a failed startup attempt"
    failpoint = "materialized_postgresql_pause_before_marking_snapshot_completed"

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pause_baseline1 = count_in_all_logs(instance, pause_line)
    pause_baseline2 = count_in_all_logs(instance2, pause_line)
    pm = PartitionManager()
    try:
        instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        instance2.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

        # Create on the first replica only, so it deterministically becomes the active worker, loads
        # the snapshot and parks right before publishing the marker.
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )
        wait_for_new_log_occurrence(instance, pause_line, pause_baseline1, timeout=90)
        assert wait_for_leader(instance) == "coord_instance1"

        # The second replica joins as a standby.
        pg_manager2.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )

        # Expire the parked leader's Keeper session. The standby takes over, sees no marker, truncates
        # the shared nested tables, starts the replacement snapshot - and parks before its own marker
        # write, keeping the takeover mid-snapshot.
        pm.drop_instance_zk_connections(instance)
        wait_for_leader(instance2, expected="coord_instance2")
        wait_for_new_log_occurrence(instance2, pause_line, pause_baseline2, timeout=120)

        # Release the deposed worker while the takeover is still mid-snapshot. Its leadership session is
        # gone, so it must abort without publishing the marker or starting a consumer.
        abort_baseline = count_in_all_logs(instance, abort_line)
        pm.heal_all()
        instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        wait_for_new_log_occurrence(instance, abort_line, abort_baseline, timeout=120)
        assert not marker_znode_exists(instance2)

        # Release the live leader: it publishes the marker and starts consuming.
        instance2.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        wait_for_marker(instance2)
    finally:
        pm.heal_all()
        for node in (instance, instance2):
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            except Exception:
                pass

    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 200
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200


def test_deposed_worker_aborts_redo_snapshot_before_touching_shared_state(started_cluster):
    # A worker that entered the redo-the-snapshot recovery branch (the slot exists but there is no
    # snapshot_completed marker) mutates shared state: it truncates the replicated nested tables and
    # drops the shared slot. If its Keeper session expires right after it entered the branch, a
    # successor may already have won /leader and be redoing the snapshot itself, so the deposed worker
    # must abort at the leadership fence instead of wiping the tables the successor has reloaded and
    # dropping the slot the successor just created.
    marker_pause_line = "Pausing before marking the initial snapshot as completed"
    redo_pause_line = "Pausing before redoing the initial snapshot"
    # Both workers are deposed while parked inside a startup attempt, so the leadership-session fence aborts
    # that attempt and the leadership is released through the failed-startup path (not through the
    # session-expiry check of the coordination task, which only sees an already-released claim afterwards).
    demotion_line = "Released replication leadership after a failed startup attempt"
    truncate_line = "Truncated nested table"
    marker_failpoint = "materialized_postgresql_pause_before_marking_snapshot_completed"
    redo_failpoint = "materialized_postgresql_pause_before_redo_snapshot_truncate"

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    pm = PartitionManager()
    try:
        # Park the first leader right before it publishes the marker, leaving the shared state as
        # "slot exists, no marker" - the state that routes the next leader into the redo branch.
        marker_pause_baseline = count_in_all_logs(instance, marker_pause_line)
        instance.query(f"SYSTEM ENABLE FAILPOINT {marker_failpoint}")
        # And park the second replica at the entry of the redo branch, before it touches anything.
        instance2.query(f"SYSTEM ENABLE FAILPOINT {redo_failpoint}")

        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )
        wait_for_new_log_occurrence(
            instance, marker_pause_line, marker_pause_baseline, timeout=90
        )
        assert wait_for_leader(instance) == "coord_instance1"

        pg_manager2.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )

        # Depose the parked first leader: the standby takes over, sees no marker, enters the redo
        # branch and parks there - before truncating anything.
        redo_pause_baseline = count_in_all_logs(instance2, redo_pause_line)
        pm.drop_instance_zk_connections(instance)
        wait_for_leader(instance2, expected="coord_instance2")
        wait_for_new_log_occurrence(
            instance2, redo_pause_line, redo_pause_baseline, timeout=120
        )

        # Release the first replica: its leadership session is gone, so it aborts at the marker fence
        # and rejoins as a standby.
        demotion_baseline1 = count_in_all_logs(instance, demotion_line)
        pm.heal_all()
        instance.query(f"SYSTEM DISABLE FAILPOINT {marker_failpoint}")
        wait_for_new_log_occurrence(instance, demotion_line, demotion_baseline1, timeout=120)

        # Depose the second replica while it is parked inside the redo branch. The first one takes
        # over, runs the redo itself (truncate, new slot, full reload) and publishes the marker.
        demotion_baseline2 = count_in_all_logs(instance2, demotion_line)
        pm.drop_instance_zk_connections(instance2)
        wait_for_leader(instance, expected="coord_instance1")
        wait_for_marker(instance)

        # Release the deposed second replica: its leadership session is gone, so it must abort at the
        # fence without truncating the tables the first replica reloaded and without dropping the slot
        # the first replica created.
        truncate_baseline = count_in_all_logs(instance2, truncate_line)
        pm.heal_all()
        instance2.query(f"SYSTEM DISABLE FAILPOINT {redo_failpoint}")
        wait_for_new_log_occurrence(
            instance2, demotion_line, demotion_baseline2, timeout=120
        )
        assert count_in_all_logs(instance2, truncate_line) == truncate_baseline
        assert marker_znode_exists(instance)
    finally:
        pm.heal_all()
        for node, failpoint in (
            (instance, marker_failpoint),
            (instance2, redo_failpoint),
        ):
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            except Exception:
                pass

    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance2, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 200
    assert int(instance2.query("SELECT count() FROM test_database.test_table")) == 200


def test_single_table_publication_recreated_after_external_drop(started_cluster):
    # The publication of a coordinated single-table engine is recreated by the same handler whenever it
    # disappears from PostgreSQL (an external drop, or a retry of a failure before it existed). The
    # recreation must be idempotent with respect to the table-name quoting: the first CREATE PUBLICATION
    # used to write the SQL-quoted form back into the handler's tables_list, so the recreation quoted it
    # a second time and asked PostgreSQL for a relation whose name literally contains double quotes,
    # wedging the self-healing path in a permanent retry loop.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    keeper_path = "/clickhouse/mat_pg/{shard}/single_table_pub_recreate"
    demotion_line = "Keeper session expired, releasing replication leadership"
    instance.query(
        f"CREATE TABLE test_single_pub (key Int64, value Int64) "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
        f"PRIMARY KEY key "
        f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
        f"materialized_postgresql_keeper_path = '{keeper_path}', "
        f"materialized_postgresql_replica_name = '{{replica}}'",
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )

    def wait_for_count(expected, timeout=90):
        for _ in range(timeout):
            try:
                if int(instance.query("SELECT count() FROM test_single_pub")) == expected:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise AssertionError(f"test_single_pub did not reach {expected} rows")

    pm = PartitionManager()
    try:
        wait_for_count(100)

        publications = pg_query(
            "SELECT pubname FROM pg_publication WHERE pubname LIKE '%test_table%'"
        )
        assert len(publications) == 1, publications
        publication_name = publications[0][0]
        pg_query(f'DROP PUBLICATION "{publication_name}"')

        # Force the same handler through its recreation path: expire the worker's Keeper session, so
        # the re-election re-runs startSynchronization -> createPublicationIfNeeded with the publication
        # absent, on a handler that has already quoted the table name once.
        demotion_baseline = count_in_all_logs(instance, demotion_line)
        pm.drop_instance_zk_connections(instance)
        wait_for_new_log_occurrence(
            instance, demotion_line, demotion_baseline, timeout=120
        )
        pm.heal_all()

        deadline = time.time() + 90
        while time.time() < deadline:
            if pg_query(
                f"SELECT pubname FROM pg_publication WHERE pubname = '{publication_name}'"
            ):
                break
            time.sleep(1)
        else:
            raise AssertionError(f"publication {publication_name} was not recreated")

        # The recreated publication publishes the correctly (singly) quoted table.
        assert pg_query(
            f"SELECT tablename FROM pg_publication_tables WHERE pubname = '{publication_name}'"
        ) == [("test_table",)]

        # Replication resumed through the recreated publication.
        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        wait_for_count(200)

        instance.query("DROP TABLE test_single_pub SYNC")
    finally:
        pm.heal_all()
        instance.query("DROP TABLE IF EXISTS test_single_pub SYNC")


def test_failed_single_table_snapshot_releases_leadership(started_cluster):
    # A coordinated single-table worker whose initial snapshot load fails must abort the startup
    # instead of constructing a consumer with no nested storage: such a consumer would keep advancing
    # the shared slot's confirmed_flush_lsn while applying nothing, silently discarding WAL for the
    # only table. It must also release the leadership, so a healthy peer can take over and redo the
    # snapshot instead of staying on standby behind a wedged leader.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    keeper_path = "/clickhouse/mat_pg/{shard}/single_table_failed_snapshot"
    keeper_path_resolved = "/clickhouse/mat_pg/1/single_table_failed_snapshot"
    release_line = "Released replication leadership after a failed startup attempt"

    def create_single_table(node):
        node.query(
            f"CREATE TABLE test_single_failed (key Int64, value Int64) "
            f"ENGINE = MaterializedPostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
            f"PRIMARY KEY key "
            f"SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree', "
            f"materialized_postgresql_keeper_path = '{keeper_path}', "
            f"materialized_postgresql_replica_name = '{{replica}}'",
            settings={"allow_experimental_materialized_postgresql_table": 1},
        )

    def wait_for_count(node, expected, timeout=120):
        for _ in range(timeout):
            try:
                if int(node.query("SELECT count() FROM test_single_failed")) == expected:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise AssertionError(
            f"test_single_failed did not reach {expected} rows on {node.name}"
        )

    release_baseline = count_in_all_logs(instance, release_line)
    instance.query(
        "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_load_from_snapshot"
    )
    try:
        create_single_table(instance)

        # Every snapshot attempt on the first replica fails; each one must abort before a consumer
        # exists and give up the leadership instead of camping on it.
        wait_for_new_log_occurrence(
            instance, release_line, release_baseline, timeout=120
        )

        # No failed attempt may have published the snapshot-completion marker.
        assert (
            int(
                instance.query(
                    f"SELECT count() FROM system.zookeeper "
                    f"WHERE path = '{keeper_path_resolved}' AND name = 'snapshot_completed'"
                )
            )
            == 0
        )

        # A healthy peer wins the released leadership and completes the snapshot. Without the fix the
        # first replica keeps the leadership with a consumer that applies nothing, this peer stays on
        # standby forever, and no rows ever arrive.
        create_single_table(instance2)
        wait_for_count(instance2, 100)

        # Rows written to PostgreSQL from now on prove the slot was not advanced past unapplied WAL;
        # the first replica receives the same data through the shared replicated tree.
        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        wait_for_count(instance2, 200)
        wait_for_count(instance, 200)

        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_load_from_snapshot"
        )
        instance.query("DROP TABLE test_single_failed SYNC")
        instance2.query("DROP TABLE test_single_failed SYNC")
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_load_from_snapshot"
        )
        instance.query("DROP TABLE IF EXISTS test_single_failed SYNC")
        instance2.query("DROP TABLE IF EXISTS test_single_failed SYNC")


def test_failed_database_snapshot_releases_leadership(started_cluster):
    # Same as test_failed_single_table_snapshot_releases_leadership, but for the coordinated
    # database engine: a snapshot-load failure of any table must abort the whole startup attempt
    # instead of building a consumer for the successfully loaded subset. Such a consumer would mark
    # the missing tables as skipped and still advance the shared slot's confirmed_flush_lsn on every
    # commit, silently discarding their WAL, and ATTACH TABLE (the usual repair path) is rejected in
    # coordinated mode. The worker must also release the leadership, so a healthy peer can redo the
    # full snapshot.
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    settings = COORDINATION_SETTINGS + [
        "materialized_postgresql_tables_list = 'test_table'"
    ]
    release_line = "Released replication leadership after a failed startup attempt"

    def wait_for_count(node, expected, timeout=120):
        for _ in range(timeout):
            try:
                if (
                    int(node.query("SELECT count() FROM test_database.test_table"))
                    == expected
                ):
                    return
            except Exception:
                pass
            time.sleep(1)
        raise AssertionError(
            f"test_database.test_table did not reach {expected} rows on {node.name}"
        )

    release_baseline = count_in_all_logs(instance, release_line)
    instance.query(
        "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_load_from_snapshot"
    )
    try:
        pg_manager.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )

        # Every snapshot attempt on the first replica fails; each one must abort before a consumer
        # exists and give up the leadership instead of camping on it.
        wait_for_new_log_occurrence(
            instance, release_line, release_baseline, timeout=120
        )

        # No failed attempt may have published the snapshot-completion marker.
        assert not marker_znode_exists(instance)

        # A healthy peer wins the released leadership and completes the snapshot. Without the fix the
        # first replica keeps the leadership with a consumer that applies nothing, this peer stays on
        # standby forever, and no rows ever arrive.
        pg_manager2.create_materialized_db(
            ip=cluster.postgres_ip, port=cluster.postgres_port, settings=settings
        )
        wait_for_count(instance2, 100)

        # Rows written to PostgreSQL from now on prove the slot was not advanced past unapplied WAL;
        # the first replica receives the same data through the shared replicated tree.
        instance.query(
            "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
        )
        wait_for_count(instance2, 200)
        wait_for_count(instance, 200)
    finally:
        instance.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_load_from_snapshot"
        )


def test_stale_registration_of_a_renamed_replica_is_purged(started_cluster):
    # <keeper_path>/replicas/<name> (as well as /naming and /table_set) is published BEFORE the first nested
    # table exists, so a configuration-only change of a macro that
    # `materialized_postgresql_replica_name` expands through, made in that window, leaves behind a
    # registration node that no nested table's metadata points at. A leftover child keeps /replicas non-empty
    # forever, and the last-replica fence - which wins only by removing the empty parent - could then never
    # fire again: the shared replication slot and publication would leak on every future drop. Such a node is
    # recognized by the owner identity stored in it (which no macro feeds into) and removed, both on the
    # startup path and on the drop path.
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
    wait_for_marker(instance)

    # The registration node stores "<server UUID>|<database UUID>" - both parts are stable across a change of
    # the macros the coordination settings expand through, which is exactly what makes the stale node
    # attributable to this replica.
    owner = "{}|{}".format(
        instance.query("SELECT serverUUID()").strip(),
        instance.query(
            "SELECT uuid FROM system.databases WHERE name = 'test_database'"
        ).strip(),
    )
    zk = started_cluster.get_kazoo_client("zoo1")
    stale_path = KEEPER_PATH_RESOLVED + "/replicas/coord_instance1_renamed"
    zk.create(stale_path, owner.encode())

    # The startup path removes it: restart the replica so the background startup task rebuilds the handler and
    # re-runs the registration.
    instance.restart_clickhouse()
    for _ in range(60):
        if not replica_registered(instance, "coord_instance1_renamed"):
            break
        time.sleep(1)
    assert not replica_registered(instance, "coord_instance1_renamed")
    assert replica_registered(instance, "coord_instance1")

    # This replica's own registration and its replication are untouched.
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 50)"
    )
    check_tables_are_synchronized(instance, "test_table")

    # The drop path removes it too, before it makes the last-replica decision - so the only replica of the
    # setup still recognizes itself as the last one and drops the shared slot and publication. Without the
    # purge the leftover child makes the fence fail, the drop decides it is not the last replica, and both
    # leak in PostgreSQL forever.
    zk.create(stale_path, owner.encode())
    pg_manager.drop_materialized_db()
    assert not replication_slot_exists()
    assert not publication_exists()


def test_hard_stop_during_non_last_teardown_keeps_replica_registered(started_cluster):
    # A server killed in the middle of a non-last DROP DATABASE must stay visible to every later
    # last-replica check: its /replicas/<name> registration is the crash-persistent record that it still
    # holds a copy of the shared data, and removing the registration and winning the last-replica fence is
    # a single atomic Keeper operation, so a non-last decision removes nothing. Without that atomicity a
    # kill between "unregister" and "re-register" would let the peer's drop win the fence and delete the
    # shared /tables subtree, slot and publication around the surviving local nested data, and the
    # restarted replica would wedge on a read-only nested table instead of recovering.
    pause_line = "Pausing the non-last coordinated teardown"
    failpoint = "materialized_postgresql_pause_in_non_last_teardown"

    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")
    wait_for_marker(instance)

    # Park the first replica's DROP right after its non-last decision and hard-kill the server there.
    pause_baseline = count_in_all_logs(instance, pause_line)
    instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

    def drop_first():
        try:
            instance.query("DROP DATABASE test_database SYNC")
        except Exception:  # noqa: BLE001
            pass  # the server is killed while the query is parked at the failpoint

    drop_thread = threading.Thread(target=drop_first)
    drop_thread.start()
    try:
        wait_for_new_log_occurrence(instance, pause_line, pause_baseline, timeout=90)
        # The non-last teardown performed no Keeper write: the registration is still in place.
        assert replica_registered(instance2, "coord_instance1")
        instance.stop_clickhouse(kill=True)
    finally:
        drop_thread.join()

    # The killed replica still holds its local nested data, and its registration keeps recording that. The
    # peer's own drop must therefore decide it is NOT the last replica and keep the shared state.
    pg_manager2.drop_materialized_db()
    assert replica_registered(instance2, "coord_instance1")
    assert replication_slot_exists()
    assert publication_exists()
    assert marker_znode_exists(instance2)

    # The killed replica restarts into the interrupted-drop state: nothing of the database had been dropped
    # yet, so it comes back registered, re-elects itself and simply resumes replicating - alone now. The
    # ephemeral leader node of the killed process only disappears when its Keeper session times out, so give
    # the re-election time.
    instance.start_clickhouse()
    wait_for_leader(instance, expected="coord_instance1", timeout=120)
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(instance, "test_table")
    assert int(instance.query("SELECT count() FROM test_database.test_table")) == 200

    # Retrying the drop - now on the last replica - tears the shared state down completely.
    instance.query("DROP DATABASE test_database SYNC")
    assert not replication_slot_exists()
    assert not publication_exists()
    assert not marker_znode_exists(instance2)


def test_graceful_stop_releases_leader_even_when_removal_fails(started_cluster):
    # `shutdown` of the active worker releases /leader with a *confirmed* removal: the node lives under
    # the server's shared Keeper session, which outlives the database, and after `shutdown` this replica
    # never re-enters the election, so an unconfirmed (lost-response) removal would keep every peer on
    # standby for as long as that shared session lives - with nobody left to remove the stale node.
    # Inject a failure of the first, session-fenced removal and check that the re-check path still frees
    # /leader and the peer takes over promptly (the dropped replica's server - and with it the Keeper
    # session the node lives under - keeps running, so a session expiry cannot be what freed it).
    pg_manager.create_postgres_table("test_table")
    instance.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100)"
    )

    create_coordinated_db("test_table")
    check_tables_are_synchronized(instance, "test_table")
    check_tables_are_synchronized(instance2, "test_table")

    leader_name = wait_for_leader(instance)
    if leader_name == "coord_instance1":
        leader_node, leader_manager, standby_node = instance, pg_manager, instance2
    else:
        leader_node, leader_manager, standby_node = instance2, pg_manager2, instance

    leader_node.query(
        "SYSTEM ENABLE FAILPOINT materialized_postgresql_fail_leader_release_at_shutdown"
    )
    try:
        # A non-last DROP DATABASE stops this replica's worker gracefully while its server keeps running.
        leader_manager.drop_materialized_db()
    finally:
        leader_node.query(
            "SYSTEM DISABLE FAILPOINT materialized_postgresql_fail_leader_release_at_shutdown"
        )

    new_leader = wait_for_leader(standby_node, expected=None, not_equal=leader_name)
    assert new_leader != leader_name

    # New changes flow through the new active worker.
    standby_node.query(
        "INSERT INTO postgres_database.test_table SELECT number, number FROM numbers(100, 100)"
    )
    check_tables_are_synchronized(standby_node, "test_table")
    assert (
        int(standby_node.query("SELECT count() FROM test_database.test_table")) == 200
    )


def test_full_attach_database_definition_is_validated(started_cluster):
    # A user ATTACH DATABASE that spells out the full engine definition is fresh user input, exactly
    # like a CREATE, so the coordination validator applies to it; only replaying an already-persisted
    # definition (server startup, and the short `ATTACH DATABASE name` syntax) is exempt. Without this,
    # a combination that CREATE rejects - here a coordinated keeper path with the default plain
    # ReplacingMergeTree nested engine, with which the standbys would hold no data - could be brought in
    # through ATTACH and would never be re-validated later.
    error = instance.query_and_get_error(
        f"ATTACH DATABASE test_attach_full_def "
        f"UUID '11111111-2222-3333-4444-555555555555' "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}') "
        f"SETTINGS materialized_postgresql_keeper_path = '{KEEPER_PATH}'"
    )
    assert "ReplicatedReplacingMergeTree" in error
    assert "test_attach_full_def" not in instance.query("SHOW DATABASES").split()


def test_full_attach_table_definition_is_validated(started_cluster):
    # Same as test_full_attach_database_definition_is_validated, for the single-table engine: a user
    # ATTACH TABLE with a full table definition must go through the coordination validator like a
    # CREATE TABLE (only a replay of persisted metadata - server startup, short-syntax ATTACH - is
    # exempt).
    error = instance.query_and_get_error(
        f"ATTACH TABLE test_attach_table_full_def "
        f"UUID '11111111-2222-3333-4444-666666666666' (key Int64, value Int64) "
        f"ENGINE = MaterializedPostgreSQL("
        f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'test_table', 'postgres', '{pg_pass}') "
        f"PRIMARY KEY key "
        f"SETTINGS materialized_postgresql_keeper_path = '{KEEPER_PATH}'",
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert "ReplicatedReplacingMergeTree" in error
    assert "test_attach_table_full_def" not in instance.query("SHOW TABLES").split()

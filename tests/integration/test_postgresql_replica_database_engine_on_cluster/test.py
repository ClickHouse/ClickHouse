import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import (
    PostgresManager,
    check_tables_are_synchronized,
    get_postgres_conn,
)
from helpers.test_tools import assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
    stay_alive=True,
)

pg_manager = PostgresManager()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        pg_manager.init(
            node1,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
        # A plain PostgreSQL engine database on node2 as well, so that
        # check_tables_are_synchronized can read the source rows from either node.
        node2.query(
            f"CREATE DATABASE postgres_database ENGINE = PostgreSQL("
            f"'{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', 'postgres', '{pg_pass}')"
        )
        yield cluster
    finally:
        cluster.shutdown()


def count_replication_slots():
    # Replication slots are cluster-wide, so any database connection sees all of them.
    conn = get_postgres_conn(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        database=True,
        database_name="postgres_database",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM pg_replication_slots")
    return int(cursor.fetchall()[0][0])


def count_publications():
    # Publications are database-scoped, so we must query the database the tables live in.
    conn = get_postgres_conn(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        database=True,
        database_name="postgres_database",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM pg_publication WHERE pubname LIKE '%_ch_publication'")
    return int(cursor.fetchall()[0][0])


def test_on_cluster_unique_replication_consumer(started_cluster):
    # Reproduces https://github.com/ClickHouse/ClickHouse/issues/58726:
    # a `MaterializedPostgreSQL` database created via `ON CLUSTER` assigns the same UUID to every
    # replica. Before the fix, all replicas derived the same replication slot and publication names
    # from that shared UUID and fought over a single PostgreSQL slot/publication, so only one replica
    # replicated correctly. With `materialized_postgresql_use_unique_replication_consumer_identifier`,
    # the per-server `ServerUUID` is mixed in, so each replica gets its own slot and publication.
    table = "test_table"
    pg_manager.create_postgres_table(table)
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(0, 50)"
    )

    node1.query(
        f"""
        CREATE DATABASE test_database ON CLUSTER test_cluster
        ENGINE = MaterializedPostgreSQL(
            '{started_cluster.postgres_ip}:{started_cluster.postgres_port}',
            'postgres_database', 'postgres', '{pg_pass}')
        SETTINGS materialized_postgresql_tables_list = '{table}',
                 materialized_postgresql_backoff_min_ms = 100,
                 materialized_postgresql_backoff_max_ms = 100,
                 materialized_postgresql_use_unique_replication_consumer_identifier = 1
        """
    )

    # Both replicas must catch up independently.
    check_tables_are_synchronized(node1, table)
    check_tables_are_synchronized(node2, table)
    assert 50 == int(node1.query(f"SELECT count() FROM test_database.{table}"))
    assert 50 == int(node2.query(f"SELECT count() FROM test_database.{table}"))

    # Each replica owns its own replication slot and publication.
    assert 2 == count_replication_slots()
    assert 2 == count_publications()

    # New changes reach both replicas.
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(1000, 1000)"
    )
    check_tables_are_synchronized(node1, table)
    check_tables_are_synchronized(node2, table)
    assert 1050 == int(node1.query(f"SELECT count() FROM test_database.{table}"))
    assert 1050 == int(node2.query(f"SELECT count() FROM test_database.{table}"))

    # Dropping the database on the cluster removes both per-replica slots and publications.
    node1.query("DROP DATABASE test_database ON CLUSTER test_cluster SYNC")
    for _ in range(30):
        if count_replication_slots() == 0 and count_publications() == 0:
            break
        time.sleep(1)
    assert 0 == count_replication_slots()
    assert 0 == count_publications()


def test_on_cluster_user_managed_slot_rejected(started_cluster):
    # A user-managed replication slot (`materialized_postgresql_replication_slot`) has a single fixed name that
    # every `ON CLUSTER` replica shares, so it cannot be made unique per server. Combining it with
    # `materialized_postgresql_use_unique_replication_consumer_identifier` (whose whole purpose is per-server
    # uniqueness for `ON CLUSTER`) is contradictory: all but one replica would fight over the single
    # user-managed slot and fail to replicate, exactly the failure that setting exists to prevent (see
    # https://github.com/ClickHouse/ClickHouse/issues/58726). The engine must reject the combination instead
    # of silently leaving the deployment half-broken, and must not create any slot or publication.
    table = "test_user_managed_slot_table"
    pg_manager.create_postgres_table(table)

    slots_before = count_replication_slots()
    publications_before = count_publications()

    node1.query(
        f"""
        CREATE DATABASE test_rejected_database ON CLUSTER test_cluster
        ENGINE = MaterializedPostgreSQL(
            '{started_cluster.postgres_ip}:{started_cluster.postgres_port}',
            'postgres_database', 'postgres', '{pg_pass}')
        SETTINGS materialized_postgresql_tables_list = '{table}',
                 materialized_postgresql_backoff_min_ms = 100,
                 materialized_postgresql_backoff_max_ms = 100,
                 materialized_postgresql_replication_slot = 'user_managed_slot',
                 materialized_postgresql_use_unique_replication_consumer_identifier = 1
        """
    )

    # Replication startup fails closed on every replica with a clear error explaining the contradiction. The
    # exception is raised while constructing the replication handler, before any slot or publication is created.
    for node in (node1, node2):
        assert_logs_contain_with_retry(
            node, "Cannot use a user-managed replication slot"
        )

    # No slot or publication was created on either replica: the contradiction is rejected, not half-applied.
    assert slots_before == count_replication_slots()
    assert publications_before == count_publications()

    node1.query("DROP DATABASE test_rejected_database ON CLUSTER test_cluster SYNC")


def test_upgrade_adopts_presalt_identity(started_cluster):
    # Before the `ON CLUSTER` fix, `materialized_postgresql_use_unique_replication_consumer_identifier`
    # derived the replication slot name from the bare ClickHouse object UUID, and the publication name
    # ignored the setting entirely (`<db>_ch_publication` for the database engine). After the fix, both
    # names are salted with the per-server `ServerUUID`. On attach (e.g. a server restart after an
    # upgrade), a deployment created with the pre-salt names must adopt them instead of looking for the
    # salted ones only: otherwise the attach would miss the existing slot, run an initial sync, and
    # reload the snapshot into the already-populated nested tables, duplicating data and leaving the old
    # slot and publication orphaned.
    table = "test_migration_table"
    pg_manager.create_postgres_table(table)
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(0, 50)"
    )

    node1.query(
        f"""
        CREATE DATABASE migration_database
        ENGINE = MaterializedPostgreSQL(
            '{started_cluster.postgres_ip}:{started_cluster.postgres_port}',
            'postgres_database', 'postgres', '{pg_pass}')
        SETTINGS materialized_postgresql_tables_list = '{table}',
                 materialized_postgresql_backoff_min_ms = 100,
                 materialized_postgresql_backoff_max_ms = 100,
                 materialized_postgresql_use_unique_replication_consumer_identifier = 1
        """
    )
    check_tables_are_synchronized(
        node1, table, materialized_database="migration_database"
    )

    # The pre-salt replication slot name is the bare ClickHouse database UUID (lower-case, `-` -> `_`),
    # and the pre-salt publication name ignores the unique-identifier setting.
    uuid = node1.query(
        "SELECT uuid FROM system.databases WHERE name = 'migration_database'"
    ).strip()
    presalt_slot = uuid.lower().replace("-", "_")
    presalt_publication = "postgres_database_ch_publication"

    conn = get_postgres_conn(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        database=True,
        database_name="postgres_database",
        auto_commit=True,
    )
    cursor = conn.cursor()
    cursor.execute("SELECT slot_name FROM pg_replication_slots")
    salted_slots = [row[0] for row in cursor.fetchall()]
    assert salted_slots != [presalt_slot]
    cursor.execute("SELECT pubname FROM pg_publication")
    salted_publications = [row[0] for row in cursor.fetchall()]
    assert salted_publications != [presalt_publication]

    # Reconstruct the PostgreSQL-side state of a deployment created before the salting: while the server
    # is down, replace the salted slot and publication with pre-salt-named ones, and add more rows so
    # that the adopted slot has something to stream after the restart.
    node1.stop_clickhouse()
    for slot in salted_slots:
        cursor.execute(f"SELECT pg_drop_replication_slot('{slot}')")
    for publication in salted_publications:
        cursor.execute(f'DROP PUBLICATION "{publication}"')
    cursor.execute(
        f"SELECT pg_create_logical_replication_slot('{presalt_slot}', 'pgoutput')"
    )
    cursor.execute(
        f'CREATE PUBLICATION "{presalt_publication}" FOR TABLE ONLY "{table}"'
    )
    cursor.execute(
        f"INSERT INTO {table} SELECT i, i FROM generate_series(50, 99) AS i"
    )
    node1.start_clickhouse()

    # The attach must adopt the pre-salt identity: no re-snapshot (the exact row set matches the source,
    # so nothing was duplicated), the streamed rows arrive, and no salted-name objects reappear.
    check_tables_are_synchronized(
        node1, table, materialized_database="migration_database"
    )
    assert 100 == int(node1.query(f"SELECT count() FROM migration_database.{table}"))

    cursor.execute("SELECT slot_name FROM pg_replication_slots")
    assert [presalt_slot] == [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT pubname FROM pg_publication")
    assert [presalt_publication] == [row[0] for row in cursor.fetchall()]

    # Dropping the database removes the adopted objects.
    node1.query("DROP DATABASE migration_database SYNC")
    for _ in range(30):
        if count_replication_slots() == 0 and count_publications() == 0:
            break
        time.sleep(1)
    assert 0 == count_replication_slots()
    assert 0 == count_publications()

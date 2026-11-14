import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import (
    PostgresManager,
    check_tables_are_synchronized,
    get_postgres_conn,
)

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


def get_replication_slots():
    conn = get_postgres_conn(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        database=True,
        database_name="postgres_database",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT slot_name FROM pg_replication_slots ORDER BY slot_name")
    return [row[0] for row in cursor.fetchall()]


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
    slots = get_replication_slots()

    # The setting and the server UUID are written into the definition of a new database,
    # so the identity survives a restart, even if the server UUID is regenerated.
    server_uuid = node1.query("SELECT serverUUID()").strip()
    create_query = node1.query("SHOW CREATE DATABASE test_database")
    assert (
        "materialized_postgresql_use_unique_replication_consumer_identifier_mixed_with_server_uuid = true"
        in create_query
    )
    assert (
        f"materialized_postgresql_unique_replication_consumer_server_uuid = \\'{server_uuid}\\'"
        in create_query
    )

    node1.stop_clickhouse()
    node1.exec_in_container(["bash", "-c", "rm /var/lib/clickhouse/uuid"])
    node1.start_clickhouse()
    assert server_uuid != node1.query("SELECT serverUUID()").strip()

    # New changes reach both replicas through the same replication slots: an update or a delete
    # would be lost if a replica created a new replication slot and reloaded the snapshot.
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(1000, 1000)"
    )
    cursor = get_postgres_conn(
        ip=cluster.postgres_ip,
        port=cluster.postgres_port,
        database=True,
        database_name="postgres_database",
    ).cursor()
    cursor.execute(f"UPDATE {table} SET value = value + 1 WHERE key < 10")
    cursor.execute(f"DELETE FROM {table} WHERE key >= 40 AND key < 50")
    check_tables_are_synchronized(node1, table)
    check_tables_are_synchronized(node2, table)
    assert 1040 == int(
        node1.query(f"SELECT count() FROM (SELECT * FROM test_database.{table})")
    )
    assert 1040 == int(
        node2.query(f"SELECT count() FROM (SELECT * FROM test_database.{table})")
    )
    assert slots == get_replication_slots()

    # Dropping the database on the cluster removes both per-replica slots and publications.
    node1.query("DROP DATABASE test_database ON CLUSTER test_cluster SYNC")
    for _ in range(30):
        if count_replication_slots() == 0 and count_publications() == 0:
            break
        time.sleep(1)
    assert 0 == count_replication_slots()
    assert 0 == count_publications()


def test_on_cluster_table_engine_unique_replication_consumer(started_cluster):
    # The same collision as test_on_cluster_unique_replication_consumer, but for the standalone
    # `MaterializedPostgreSQL` TABLE engine instead of the database engine. The table engine has its own
    # DDL/UUID plumbing, but derives its replication slot and publication names from the ClickHouse table
    # UUID through the same shared `PostgreSQLReplicationHandler`. A `CREATE TABLE ... ON CLUSTER` assigns
    # the same UUID to the table on every replica, so before the fix all replicas derived the same slot and
    # publication names and fought over a single PostgreSQL slot/publication (the table-engine mirror of
    # https://github.com/ClickHouse/ClickHouse/issues/58726) - and because the table engine runs the initial
    # sync synchronously during `CREATE`, all but one replica's `CREATE` would fail outright. With
    # `materialized_postgresql_use_unique_replication_consumer_identifier`, the per-server `ServerUUID` is
    # mixed in, so each replica gets its own slot and publication and creates its table independently.
    table = "test_table_engine_on_cluster"
    pg_manager.create_postgres_table(table)
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(0, 50)"
    )

    slots_before = count_replication_slots()
    publications_before = count_publications()

    node1.query(
        f"""
        CREATE TABLE default.{table} ON CLUSTER test_cluster (key Int32, value Int32)
        ENGINE = MaterializedPostgreSQL(
            '{started_cluster.postgres_ip}:{started_cluster.postgres_port}',
            'postgres_database', '{table}', 'postgres', '{pg_pass}')
        ORDER BY key
        SETTINGS materialized_postgresql_backoff_min_ms = 100,
                 materialized_postgresql_backoff_max_ms = 100,
                 materialized_postgresql_use_unique_replication_consumer_identifier = 1
        """
    )

    # `ON CLUSTER` really did assign one shared table UUID on both replicas - the collision precondition
    # that the fix has to make survivable.
    uuid1 = node1.query(
        f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()
    uuid2 = node2.query(
        f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()
    assert uuid1 == uuid2 and uuid1 != "00000000-0000-0000-0000-000000000000"

    # Both replicas must catch up independently.
    check_tables_are_synchronized(node1, table, materialized_database="default")
    check_tables_are_synchronized(node2, table, materialized_database="default")
    assert 50 == int(node1.query(f"SELECT count() FROM default.{table}"))
    assert 50 == int(node2.query(f"SELECT count() FROM default.{table}"))

    # Each replica owns its own replication slot and publication.
    assert slots_before + 2 == count_replication_slots()
    assert publications_before + 2 == count_publications()

    # New changes reach both replicas.
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(1000, 1000)"
    )
    check_tables_are_synchronized(node1, table, materialized_database="default")
    check_tables_are_synchronized(node2, table, materialized_database="default")
    assert 1050 == int(node1.query(f"SELECT count() FROM default.{table}"))
    assert 1050 == int(node2.query(f"SELECT count() FROM default.{table}"))

    # Dropping the table on the cluster removes both per-replica slots and publications.
    node1.query(f"DROP TABLE default.{table} ON CLUSTER test_cluster SYNC")
    for _ in range(30):
        if (
            count_replication_slots() == slots_before
            and count_publications() == publications_before
        ):
            break
        time.sleep(1)
    assert slots_before == count_replication_slots()
    assert publications_before == count_publications()


def test_on_cluster_create_as_gets_server_uuid_of_each_replica(started_cluster):
    # `CREATE TABLE ... ON CLUSTER ... AS src` copies the engine settings of `src`, including the stored
    # `materialized_postgresql_unique_replication_consumer_server_uuid`, on the initiator. Each replica must
    # still use its own server UUID, otherwise all replicas would share one replication slot and publication.
    table = "test_table_create_as"
    pg_manager.create_postgres_table(table)
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(0, 50)"
    )

    slots_before = count_replication_slots()
    publications_before = count_publications()

    node1.query(
        f"""
        CREATE TABLE default.{table}_src (key Int32, value Int32)
        ENGINE = MaterializedPostgreSQL(
            '{started_cluster.postgres_ip}:{started_cluster.postgres_port}',
            'postgres_database', '{table}', 'postgres', '{pg_pass}')
        ORDER BY key
        SETTINGS materialized_postgresql_backoff_min_ms = 100,
                 materialized_postgresql_backoff_max_ms = 100,
                 materialized_postgresql_use_unique_replication_consumer_identifier = 1
        """
    )
    node1.query(
        f"CREATE TABLE default.{table} ON CLUSTER test_cluster AS default.{table}_src"
    )

    for node in [node1, node2]:
        server_uuid = node.query("SELECT serverUUID()").strip()
        assert (
            f"materialized_postgresql_unique_replication_consumer_server_uuid = \\'{server_uuid}\\'"
            in node.query(f"SHOW CREATE TABLE default.{table}")
        )

    # The source table and each replica of the copy own their own replication slot and publication.
    assert slots_before + 3 == count_replication_slots()
    assert publications_before + 3 == count_publications()

    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(1000, 1000)"
    )
    check_tables_are_synchronized(node1, table, materialized_database="default")
    check_tables_are_synchronized(node2, table, materialized_database="default")
    assert 1050 == int(node1.query(f"SELECT count() FROM default.{table}"))
    assert 1050 == int(node2.query(f"SELECT count() FROM default.{table}"))

    node1.query(f"DROP TABLE default.{table} ON CLUSTER test_cluster SYNC")
    node1.query(f"DROP TABLE default.{table}_src SYNC")
    for _ in range(30):
        if (
            count_replication_slots() == slots_before
            and count_publications() == publications_before
        ):
            break
        time.sleep(1)
    assert slots_before == count_replication_slots()
    assert publications_before == count_publications()


def test_definition_without_setting_keeps_legacy_identity(started_cluster):
    # A definition without `materialized_postgresql_use_unique_replication_consumer_identifier_mixed_with_server_uuid`,
    # as stored by older versions, keeps using the bare ClickHouse UUID as the replication slot name,
    # so an existing database continues to replicate through its existing replication slot.
    table = "test_table_legacy_identity"
    uuid = "8a2e5b1c-6f3d-4e7a-9b0c-1d2e3f4a5b6c"
    pg_manager.create_postgres_table(table)
    node1.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number FROM numbers(0, 50)"
    )

    node1.query(
        f"""
        ATTACH DATABASE test_database_legacy UUID '{uuid}'
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
        node1, table, materialized_database="test_database_legacy"
    )
    assert uuid.replace("-", "_") in get_replication_slots()
    assert "mixed_with_server_uuid" not in node1.query(
        "SHOW CREATE DATABASE test_database_legacy"
    )

    node1.query("DROP DATABASE test_database_legacy SYNC")
    for _ in range(30):
        if uuid.replace("-", "_") not in get_replication_slots():
            break
        time.sleep(1)
    assert uuid.replace("-", "_") not in get_replication_slots()

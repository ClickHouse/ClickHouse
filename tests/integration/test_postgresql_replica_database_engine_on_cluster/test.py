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

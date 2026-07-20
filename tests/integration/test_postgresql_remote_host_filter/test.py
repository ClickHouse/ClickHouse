import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import get_postgres_conn

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_host_filter.xml"],
    with_postgres=True,
)

# A host that `remote_url_allow_hosts` does not permit (only `postgres1` is allowed,
# see `remote_host_filter.xml`).
BLOCKED_HOST = "blocked-postgres-host"

# The docker service/alias name the ClickHouse node uses to reach PostgreSQL.
PG_HOST = "postgres1"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn(cluster.postgres_ip, cluster.postgres_port)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("CREATE TABLE test_table (id Integer NOT NULL, value Integer, PRIMARY KEY (id))")
        cursor.execute("INSERT INTO test_table SELECT i, i * 2 FROM generate_series(1, 10) AS i")
        yield cluster
    finally:
        cluster.shutdown()


def test_postgresql_database_engine_respects_remote_host_filter(started_cluster):
    # The `PostgreSQL` and `MaterializedPostgreSQL` database engines used to reach the
    # remote host directly, skipping the server's `remote_url_allow_hosts` policy that
    # the table engine, table function and DDL dictionaries enforce. A user forbidden
    # from a host through those entrypoints could still open a connection (and, for
    # MaterializedPostgreSQL, a long-lived replication connection) to it through a
    # database engine. Only `postgres1` is whitelisted (see `remote_host_filter.xml`),
    # so pointing a database engine at any other host must now be rejected -- and,
    # crucially, before any connection is attempted, so the assertion is deterministic
    # even though the host does not exist.
    node.query("DROP DATABASE IF EXISTS pg_db_blocked")
    error = node.query_and_get_error(f"CREATE DATABASE pg_db_blocked ENGINE = PostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')")
    assert "UNACCEPTABLE_URL" in error

    # The MaterializedPostgreSQL database engine (its own registration path).
    node.query("DROP DATABASE IF EXISTS mpg_blocked")
    error = node.query_and_get_error(
        f"""
        CREATE DATABASE mpg_blocked
        ENGINE = MaterializedPostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')
        SETTINGS materialized_postgresql_tables_list = 'mat_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert "UNACCEPTABLE_URL" in error

    # The standalone MaterializedPostgreSQL table engine (a separate registration path,
    # already covered via `StoragePostgreSQL::getConfiguration` -- kept as a regression check).
    node.query("DROP TABLE IF EXISTS mpg_tbl_blocked SYNC")
    error = node.query_and_get_error(
        f"""
        CREATE TABLE mpg_tbl_blocked (key Int32, value Int32)
        ENGINE = MaterializedPostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'test_table', 'postgres', '{pg_pass}')
        ORDER BY key
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert "UNACCEPTABLE_URL" in error

    # The allowed host still works: the database engine over `postgres1` creates
    # successfully and reads data, proving the filter accepts the whitelisted host
    # rather than rejecting every host.
    node.query("DROP DATABASE IF EXISTS pg_db_allowed")
    node.query(f"CREATE DATABASE pg_db_allowed ENGINE = PostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')")
    assert node.query("SELECT count() FROM pg_db_allowed.test_table").strip() == "10"
    node.query("DROP DATABASE pg_db_allowed")

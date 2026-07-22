import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import get_postgres_conn
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_host_filter.xml", "configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
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

    # The standalone MaterializedPostgreSQL table engine (a separate registration path;
    # the host filter is enforced by `StoragePostgreSQL::getConfiguration`).
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


def test_postgresql_database_engine_named_collection_addresses_expr(started_cluster):
    # The `PostgreSQL` database engine over a named collection goes through
    # `StoragePostgreSQL::processNamedCollectionResult(..., require_table = false)` and then the
    # dedicated `checkHostAndPort` loop in `registerDatabasePostgreSQL`, a distinct path from the
    # positional `host:port` form above. An `addresses_expr` named collection is the less shared
    # shape here, so exercise it directly. See the named collections in `named_collections.xml`.

    # A blocked host inside `addresses_expr` is rejected by the host filter, before any connection.
    node.query("DROP DATABASE IF EXISTS pg_nc_blocked_db")
    error = node.query_and_get_error("CREATE DATABASE pg_nc_blocked_db ENGINE = PostgreSQL(mpg_nc_blocked)")
    assert "UNACCEPTABLE_URL" in error

    # The whitelisted host works: the database engine over the `addresses_expr` collection creates
    # successfully and reads data, proving the filter accepts the whitelisted host.
    node.query("DROP DATABASE IF EXISTS pg_nc_allowed_db")
    node.query("CREATE DATABASE pg_nc_allowed_db ENGINE = PostgreSQL(mpg_nc_allowed)")
    assert node.query("SELECT count() FROM pg_nc_allowed_db.test_table").strip() == "10"
    node.query("DROP DATABASE pg_nc_allowed_db")


def test_materialized_postgresql_named_collection_addresses_expr(started_cluster):
    # A named collection can specify the endpoint as `addresses_expr`, which fills only
    # `configuration.addresses` and leaves `host` / `port` empty. The MaterializedPostgreSQL
    # database engine builds its replication connection string from `host` / `port`, so it must
    # canonicalize the single parsed address back into them -- and still run it through
    # `remote_url_allow_hosts`. See the named collections in `named_collections.xml`.

    # A blocked host inside `addresses_expr` is rejected by the host filter.
    node.query("DROP DATABASE IF EXISTS mpg_nc_blocked_db")
    error = node.query_and_get_error(
        """
        CREATE DATABASE mpg_nc_blocked_db
        ENGINE = MaterializedPostgreSQL(mpg_nc_blocked)
        SETTINGS materialized_postgresql_tables_list = 'test_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert "UNACCEPTABLE_URL" in error

    # `addresses_expr` expanding to several addresses cannot feed the single replication
    # connection this engine keeps, so it is rejected up front instead of silently
    # connecting to `:0`.
    node.query("DROP DATABASE IF EXISTS mpg_nc_multiple_db")
    error = node.query_and_get_error(
        """
        CREATE DATABASE mpg_nc_multiple_db
        ENGINE = MaterializedPostgreSQL(mpg_nc_multiple)
        SETTINGS materialized_postgresql_tables_list = 'test_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert "BAD_ARGUMENTS" in error

    # The whitelisted host works end-to-end: the single `addresses_expr` address is
    # canonicalized into `host` / `port`, the replication connection is established,
    # and the table is materialized.
    node.query("DROP DATABASE IF EXISTS mpg_nc_allowed_db SYNC")
    node.query(
        """
        CREATE DATABASE mpg_nc_allowed_db
        ENGINE = MaterializedPostgreSQL(mpg_nc_allowed)
        SETTINGS materialized_postgresql_tables_list = 'test_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert_eq_with_retry(node, "SELECT count() FROM mpg_nc_allowed_db.test_table", "10", retry_count=120)
    node.query("DROP DATABASE mpg_nc_allowed_db SYNC")


def test_materialized_postgresql_table_engine_named_collection_addresses_expr(started_cluster):
    # The standalone MaterializedPostgreSQL table engine builds its replication connection
    # string from `host` / `port` the same way the database engine does, so it needs the same
    # `addresses_expr` canonicalization and multi-address rejection (otherwise a named collection
    # with `addresses_expr` reaches `formatConnectionString` with `:0`). See `named_collections.xml`.

    # A blocked host inside `addresses_expr` is rejected by the host filter.
    node.query("DROP TABLE IF EXISTS mpg_nc_blocked_tbl SYNC")
    error = node.query_and_get_error(
        """
        CREATE TABLE mpg_nc_blocked_tbl (id Int32, value Int32)
        ENGINE = MaterializedPostgreSQL(mpg_nc_blocked, table='test_table')
        ORDER BY id
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert "UNACCEPTABLE_URL" in error

    # `addresses_expr` expanding to several addresses cannot feed the single replication
    # connection this engine keeps, so it is rejected up front instead of connecting to `:0`.
    node.query("DROP TABLE IF EXISTS mpg_nc_multiple_tbl SYNC")
    error = node.query_and_get_error(
        """
        CREATE TABLE mpg_nc_multiple_tbl (id Int32, value Int32)
        ENGINE = MaterializedPostgreSQL(mpg_nc_multiple, table='test_table')
        ORDER BY id
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert "BAD_ARGUMENTS" in error

    # The whitelisted host works end-to-end: the single `addresses_expr` address is
    # canonicalized into `host` / `port` and the table is materialized.
    node.query("DROP TABLE IF EXISTS mpg_nc_allowed_tbl SYNC")
    node.query(
        """
        CREATE TABLE mpg_nc_allowed_tbl (id Int32, value Int32)
        ENGINE = MaterializedPostgreSQL(mpg_nc_allowed, table='test_table')
        ORDER BY id
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert_eq_with_retry(node, "SELECT count() FROM mpg_nc_allowed_tbl", "10", retry_count=120)
    node.query("DROP TABLE mpg_nc_allowed_tbl SYNC")

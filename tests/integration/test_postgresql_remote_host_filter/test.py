import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.database_disk import read_metadata, replace_text_in_metadata
from helpers.postgres_utility import get_postgres_conn
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_host_filter.xml", "configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    stay_alive=True,
)

# A host that `remote_url_allow_hosts` does not permit (only `postgres1` is allowed,
# see `remote_host_filter.xml`).
BLOCKED_HOST = "blocked-postgres-host"

# The docker service/alias name the ClickHouse node uses to reach PostgreSQL.
PG_HOST = "postgres1"


def _rewrite_database_metadata(old_value, new_value, database_name):
    # Rewrite the persisted definition of a database while the server is down. The definition
    # lives at `metadata/<name>.sql` on the database disk, which is a remote object storage in
    # the "db disk" CI configuration, so it must be edited through the `clickhouse disks` CLI
    # (`replace_text_in_metadata`) rather than by touching `/var/lib/clickhouse` directly.
    # Fail closed on both sides of the rewrite: the replay tests only prove anything if the
    # stored definition actually changed, and `str.replace` is a silent no-op when the metadata
    # serialization drifts and `old_value` no longer appears in it.
    metadata_path = f"metadata/{database_name}.sql"
    assert old_value in read_metadata(node, metadata_path), (
        f"persisted metadata of `{database_name}` does not contain '{old_value}'; the replay test would silently stop exercising the startup exemption"
    )
    replace_text_in_metadata(node, metadata_path, old_value, new_value)
    assert new_value in read_metadata(node, metadata_path)


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

    # A multi-address failover list is filtered as a whole: the first address is whitelisted, the
    # second is blocked, and the engine must still be rejected. This is the branch that proves
    # every failover target in `configuration.addresses` is checked before the pool is created,
    # not just the first replica.
    node.query("DROP DATABASE IF EXISTS pg_nc_mixed_db")
    error = node.query_and_get_error("CREATE DATABASE pg_nc_mixed_db ENGINE = PostgreSQL(pg_nc_mixed)")
    assert "UNACCEPTABLE_URL" in error

    # The same contract for the positional form, where the failover list comes from the
    # `host:port` engine argument instead of a named collection.
    node.query("DROP DATABASE IF EXISTS pg_mixed_db")
    error = node.query_and_get_error(f"CREATE DATABASE pg_mixed_db ENGINE = PostgreSQL('{PG_HOST}:5432|{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')")
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


def test_user_attach_respects_remote_host_filter(started_cluster):
    # A user-issued `ATTACH DATABASE` is not a startup replay: exempting it would be a direct
    # bypass of `remote_url_allow_hosts` (attach a blocked but reachable host, and the engine is
    # free to connect to it later). Only replaying a stored definition skips the filter.
    node.query("DROP DATABASE IF EXISTS pg_db_user_attach")
    error = node.query_and_get_error(f"ATTACH DATABASE pg_db_user_attach ENGINE = PostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')")
    assert "UNACCEPTABLE_URL" in error

    # MaterializedPostgreSQL is Atomic-based, so an explicit ATTACH with an engine definition
    # requires a UUID clause -- without it the query is rejected as INCORRECT_QUERY before the
    # engine registration (and thus the host filter) is ever reached.
    node.query("DROP DATABASE IF EXISTS mpg_user_attach")
    error = node.query_and_get_error(
        f"ATTACH DATABASE mpg_user_attach UUID '00001111-2222-3333-4444-555566667777' ENGINE = MaterializedPostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')",
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert "UNACCEPTABLE_URL" in error


def test_parallel_with_user_attach_respects_remote_host_filter(started_cluster):
    # A `PARALLEL WITH` child executes with `internal = true`, so the replay exemption cannot be keyed
    # on that flag: the child is still the user's own `ATTACH DATABASE` at mode `ATTACH`. Keying on it
    # let the wrapped form through while the bare form above was rejected, sending the credentials of
    # the attached definition to a host `remote_url_allow_hosts` forbids.
    node.query("DROP DATABASE IF EXISTS pg_db_parallel_attach")
    node.query("DROP DATABASE IF EXISTS pg_parallel_sink")
    error = node.query_and_get_error(
        f"ATTACH DATABASE pg_db_parallel_attach ENGINE = PostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}') "
        "PARALLEL WITH CREATE DATABASE pg_parallel_sink ENGINE = Memory"
    )
    assert "UNACCEPTABLE_URL" in error
    assert node.query("SELECT count() FROM system.databases WHERE name = 'pg_db_parallel_attach'").strip() == "0"
    node.query("DROP DATABASE IF EXISTS pg_parallel_sink")

    node.query("DROP DATABASE IF EXISTS mpg_parallel_attach")
    node.query("DROP DATABASE IF EXISTS mpg_parallel_sink")
    error = node.query_and_get_error(
        f"ATTACH DATABASE mpg_parallel_attach UUID '00001111-2222-3333-4444-555566667779' ENGINE = MaterializedPostgreSQL('{BLOCKED_HOST}:5432', 'postgres', 'postgres', '{pg_pass}') "
        "PARALLEL WITH CREATE DATABASE mpg_parallel_sink ENGINE = Memory",
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert "UNACCEPTABLE_URL" in error
    assert node.query("SELECT count() FROM system.databases WHERE name = 'mpg_parallel_attach'").strip() == "0"
    node.query("DROP DATABASE IF EXISTS mpg_parallel_sink")


def test_user_attach_table_respects_multi_address_validation(started_cluster):
    # A user `ATTACH TABLE` with a full table definition is a fresh, user-supplied definition,
    # not a metadata replay -- exempting it from the multi-address `addresses_expr` rejection
    # would let a broken definition (whose replication connection string degenerates to `:0`)
    # be attached and persisted, because replication only starts asynchronously and never
    # aborts the ATTACH. Only replaying previously persisted metadata (server startup,
    # short-syntax re-attach) keeps the legacy leniency.
    node.query("DROP TABLE IF EXISTS mpg_attach_multi_tbl SYNC")
    error = node.query_and_get_error(
        """
        ATTACH TABLE mpg_attach_multi_tbl UUID '00001111-2222-3333-4444-555566667778' (id Int32, value Int32)
        ENGINE = MaterializedPostgreSQL(mpg_nc_multiple, table='test_table')
        ORDER BY id
        """,
    )
    assert "BAD_ARGUMENTS" in error


def test_startup_metadata_replay_skips_remote_host_filter(started_cluster):
    # Server startup rebuilds every database from persisted metadata with an internal ATTACH
    # query, and `loadMetadata` aborts on the first exception, so a database created before the
    # whitelist was tightened must keep loading -- otherwise one such database would prevent the
    # whole server from booting after an upgrade. Simulate that history by creating the database
    # against the allowed host and rewriting the persisted metadata to the blocked host while the
    # server is down.
    node.query("DROP DATABASE IF EXISTS pg_db_persisted")
    node.query(f"CREATE DATABASE pg_db_persisted ENGINE = PostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')")

    node.stop_clickhouse()
    try:
        _rewrite_database_metadata(f"{PG_HOST}:5432", f"{BLOCKED_HOST}:5432", "pg_db_persisted")
    finally:
        # Bring the server back even if the rewrite failed, so a failure here does not
        # cascade into every later test in the module.
        node.start_clickhouse()

    assert node.query("SELECT count() FROM system.databases WHERE name = 'pg_db_persisted'").strip() == "1"
    node.query("DROP DATABASE pg_db_persisted")


def test_startup_metadata_replay_skips_remote_host_filter_materialized(started_cluster):
    # The same startup-replay contract as above, but for the `MaterializedPostgreSQL` database
    # engine and the host filter itself: an existing database pointing outside the whitelist
    # must keep loading after an upgrade. This is the case the multi-address replay test below
    # cannot cover (its collection only holds whitelisted addresses), so rewrite the persisted
    # definition from the allowed collection to the blocked-host one and verify the server
    # comes back up with the database in place, replication failing and retrying in the
    # background against the unreachable host.
    node.query("DROP DATABASE IF EXISTS mpg_db_host_persisted SYNC")
    node.query(
        """
        CREATE DATABASE mpg_db_host_persisted
        ENGINE = MaterializedPostgreSQL(mpg_nc_allowed)
        SETTINGS materialized_postgresql_tables_list = 'test_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert_eq_with_retry(node, "SELECT count() FROM mpg_db_host_persisted.test_table", "10", retry_count=120)

    node.stop_clickhouse()
    try:
        _rewrite_database_metadata("mpg_nc_allowed", "mpg_nc_blocked", "mpg_db_host_persisted")
    finally:
        # Bring the server back even if the rewrite failed, so a failure here does not
        # cascade into every later test in the module.
        node.start_clickhouse()

    assert node.query("SELECT count() FROM system.databases WHERE name = 'mpg_db_host_persisted'").strip() == "1"
    node.query("DROP DATABASE mpg_db_host_persisted SYNC")


def test_startup_metadata_replay_skips_multi_address_validation(started_cluster):
    # The multi-address `addresses_expr` rejection needs the same startup exemption as the host
    # filter: before it existed, `CREATE DATABASE ... ENGINE = MaterializedPostgreSQL(<nc>)` with
    # a multi-address `addresses_expr` could persist in metadata (replication starts
    # asynchronously, so the broken connection string never aborted the CREATE), and replaying
    # that stored definition must not abort server startup. Simulate such legacy metadata by
    # creating the database over the single-address collection and rewriting the persisted
    # definition to the multi-address one while the server is down.
    node.query("DROP DATABASE IF EXISTS mpg_db_persisted SYNC")
    node.query(
        """
        CREATE DATABASE mpg_db_persisted
        ENGINE = MaterializedPostgreSQL(mpg_nc_allowed)
        SETTINGS materialized_postgresql_tables_list = 'test_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert_eq_with_retry(node, "SELECT count() FROM mpg_db_persisted.test_table", "10", retry_count=120)

    node.stop_clickhouse()
    try:
        _rewrite_database_metadata("mpg_nc_allowed", "mpg_nc_multiple", "mpg_db_persisted")
    finally:
        # Bring the server back even if the rewrite failed, so a failure here does not
        # cascade into every later test in the module.
        node.start_clickhouse()

    # The database loads; background replication keeps failing and retrying against the broken
    # connection string, exactly as it did before the validation existed.
    assert node.query("SELECT count() FROM system.databases WHERE name = 'mpg_db_persisted'").strip() == "1"
    node.query("DROP DATABASE mpg_db_persisted SYNC")

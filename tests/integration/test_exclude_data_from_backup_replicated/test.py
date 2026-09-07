import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import PostgresManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    with_postgres=True,
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)

pg_manager = PostgresManager()

# The outer table's UUID is fixed so its nested table can be created under the name the outer table
# will derive from it (`<uuid of the outer table>_nested`).
OUTER_UUID = "01234567-89ab-cdef-0123-456789abcdef"
NESTED_TABLE = f"{OUTER_UUID}_nested"

# A second pair, so the test below builds its own lagging state without disturbing the first one.
EXCEPT_OUTER_UUID = "89abcdef-0123-4567-89ab-cdef01234567"
EXCEPT_NESTED_TABLE = f"{EXCEPT_OUTER_UUID}_nested"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        pg_manager.init(
            node1,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
        yield cluster
    finally:
        cluster.shutdown()


def backup_files(node, backup_id):
    listing = node.exec_in_container(
        [
            "bash",
            "-c",
            f"find /backups/{backup_id} -type f | sed 's|/backups/{backup_id}/||' | sort",
        ]
    )
    return [line for line in listing.splitlines() if line]


def test_nested_table_is_internal_on_a_lagging_replica():
    """The nested table of a standalone `MaterializedPostgreSQL` table must stay internal on a replica
    which has not created the outer table yet.

    `DatabaseReplicated::getTablesForBackup` enumerates the database from its Keeper metadata snapshot
    and deliberately tolerates `storage == nullptr` for a table which exists in Keeper but has not been
    created on this replica yet. Classifying `<uuid>_nested` by looking the outer table up in the live
    `DatabaseCatalog` therefore gave a different answer per replica: on a replica which had applied the
    nested table's DDL entry but not the outer table's, the lookup found nothing, the nested table was
    taken for an ordinary table, and the backup got it as an entry of its own - the hidden table leaks
    into the backup and comes back as a user-visible table on restore.

    The classification now comes from the same enumeration the names come from, so it does not depend on
    how far this replica has caught up.

    The nested table is created before the outer one because `StorageMaterializedPostgreSQL` cannot
    create it itself inside a `Replicated` database: `createNestedIfNeeded` runs while the outer table's
    own DDL entry is being executed and would enqueue a second entry, which cannot be reached until the
    first one finishes. With the nested table already present that call returns early.
    """
    pg_table = "lagging_pg_tbl"

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    node1.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers(50)"
    )

    for node in (node1, node2):
        node.query("DROP DATABASE IF EXISTS rdb SYNC")
    for node in (node1, node2):
        node.query(
            "CREATE DATABASE rdb ENGINE = Replicated('/test/exclude_data_rdb', '{shard}', '{replica}')"
        )

    # 1. The nested table first, and let both replicas apply it.
    node1.query(
        f"""
        CREATE TABLE rdb.`{NESTED_TABLE}`
        (
            key Int32,
            value Int32,
            _sign Int8 MATERIALIZED 1,
            _version UInt64 MATERIALIZED 1
        )
        ENGINE = ReplacingMergeTree(_version) ORDER BY key
        """
    )
    node2.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "1" == node2.query(
        f"EXISTS TABLE rdb.`{NESTED_TABLE}`"
    ).strip(), "node2 must have the nested table before it is frozen"

    # 2. Freeze node2's DDL worker so it never applies the outer table's entry.
    node2.query("SYSTEM ENABLE FAILPOINT database_replicated_stop_entry_execution")
    try:
        # `distributed_ddl_task_timeout = 0` so the initiator does not wait for the frozen replica.
        node1.query(
            f"""
            CREATE TABLE rdb.{pg_table} UUID '{OUTER_UUID}' (key Int32, value Int32)
            ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
            ORDER BY key
            """,
            settings={
                "allow_experimental_materialized_postgresql_table": 1,
                "database_replicated_allow_explicit_uuid": 1,
                "distributed_ddl_task_timeout": 0,
            },
        )

        # The rows reach the nested table through replication from PostgreSQL on node1.
        assert_eq_with_retry(node1, f"SELECT count() FROM rdb.{pg_table}", "50\n")

        # 3. This is the lagging state under test: node2 has the nested table but not the outer one,
        #    while the Keeper snapshot its backup enumerates has both.
        assert "0" == node2.query(f"EXISTS TABLE rdb.{pg_table}").strip(), (
            "node2 must NOT have created the outer table yet - the failpoint is supposed to keep its "
            "DDL worker frozen, otherwise this test is not exercising the lagging replica at all"
        )
        assert "1" == node2.query(f"EXISTS TABLE rdb.`{NESTED_TABLE}`").strip()

        # 4. Back up from the lagging replica.
        node2.query("BACKUP DATABASE rdb TO Disk('backups', 'lagging/')")
        files = backup_files(node2, "lagging")

        # The outer table is in the backup (from the snapshot, without data - it has no local storage).
        assert any(
            path == f"metadata/rdb/{pg_table}.sql" for path in files
        ), f"the outer table is missing from the backup: {files}"

        # The nested table must not be there in any form. Backup paths escape the table name
        # (`-` becomes `%2D`), so match the `_nested` marker rather than the raw UUID name.
        assert not any(
            path.startswith("metadata/rdb/") and "_nested" in path for path in files
        ), f"nested table metadata leaked into the backup: {files}"
        assert not any(
            path.startswith("data/rdb/") and "_nested" in path for path in files
        ), f"nested table data leaked into the backup: {files}"
    finally:
        node2.query(
            "SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution"
        )

    for node in (node1, node2):
        node.query("DROP DATABASE IF EXISTS rdb SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")


def test_except_data_on_a_nested_table_is_rejected_on_a_lagging_replica():
    """The clause is refused on a nested table even where this replica cannot see the outer table.

    The trace from the review, run on the lagging replica:

        BACKUP TABLE rdb.`<uuid>_nested` EXCEPT DATA FROM TABLE rdb.`<uuid>_nested` TO ...

    This is the single-table form, so nothing but the nested table is named. Validating it against the
    live `DatabaseCatalog` gave the wrong answer here for the same reason the classification did before
    it moved to the snapshot: the replica has applied the nested table's DDL entry but not its outer
    table's, so the catalog has no outer table to find and the clause was accepted, while the Keeper
    snapshot the backup enumerates holds both tables.

    The second half is the negative control: a table of the same shape whose UUID owns nothing is an
    ordinary table on this replica too, and takes the clause.
    """
    pg_table = "lag_except_tbl"

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    node1.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers(20)"
    )

    for node in (node1, node2):
        node.query("DROP DATABASE IF EXISTS rdb_except SYNC")
    for node in (node1, node2):
        node.query(
            "CREATE DATABASE rdb_except ENGINE = Replicated('/test/exclude_data_rdb_except', '{shard}', '{replica}')"
        )

    # The nested table first, so the outer table's own DDL entry does not have to enqueue a second one
    # (see `test_nested_table_is_internal_on_a_lagging_replica` for why that cannot work).
    node1.query(
        f"""
        CREATE TABLE rdb_except.`{EXCEPT_NESTED_TABLE}`
        (
            key Int32,
            value Int32,
            _sign Int8 MATERIALIZED 1,
            _version UInt64 MATERIALIZED 1
        )
        ENGINE = ReplacingMergeTree(_version) ORDER BY key
        """
    )
    # An ordinary table shaped like a nested table, whose UUID owns nothing. The negative control.
    ordinary_nested_like = "fedcba98-7654-3210-fedc-ba9876543210_nested"
    node1.query(
        f"CREATE TABLE rdb_except.`{ordinary_nested_like}` (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    node1.query(f"INSERT INTO rdb_except.`{ordinary_nested_like}` VALUES (1), (2), (3)")

    node2.query("SYSTEM SYNC DATABASE REPLICA rdb_except")
    assert "1" == node2.query(
        f"EXISTS TABLE rdb_except.`{EXCEPT_NESTED_TABLE}`"
    ).strip(), "node2 must have the nested table before it is frozen"

    node2.query("SYSTEM ENABLE FAILPOINT database_replicated_stop_entry_execution")
    try:
        node1.query(
            f"""
            CREATE TABLE rdb_except.{pg_table} UUID '{EXCEPT_OUTER_UUID}' (key Int32, value Int32)
            ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
            ORDER BY key
            """,
            settings={
                "allow_experimental_materialized_postgresql_table": 1,
                "database_replicated_allow_explicit_uuid": 1,
                "distributed_ddl_task_timeout": 0,
            },
        )

        assert_eq_with_retry(node1, f"SELECT count() FROM rdb_except.{pg_table}", "20\n")

        # The lagging state under test: node2 has the nested table but not the outer one.
        assert "0" == node2.query(f"EXISTS TABLE rdb_except.{pg_table}").strip(), (
            "node2 must NOT have created the outer table yet - the failpoint is supposed to keep its "
            "DDL worker frozen, otherwise this test is not exercising the lagging replica at all"
        )
        assert "1" == node2.query(
            f"EXISTS TABLE rdb_except.`{EXCEPT_NESTED_TABLE}`"
        ).strip()

        # 1. The reported trace. Only the nested table is named, and the replica's own catalog cannot
        #    tell it is one, so the answer has to come from the snapshot.
        with pytest.raises(Exception) as exc_info:
            node2.query(
                f"BACKUP TABLE rdb_except.`{EXCEPT_NESTED_TABLE}` "
                f"EXCEPT DATA FROM TABLE rdb_except.`{EXCEPT_NESTED_TABLE}` "
                "TO Disk('backups', 'lag_except_rejected/')"
            )
        assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in str(
            exc_info.value
        ), str(exc_info.value)

        # 2. Negative control: same name shape, same replica, but no outer table owns that UUID
        #    anywhere in the snapshot - so it is an ordinary table and the clause applies to it.
        node2.query(
            f"BACKUP TABLE rdb_except.`{ordinary_nested_like}` "
            f"EXCEPT DATA FROM TABLE rdb_except.`{ordinary_nested_like}` "
            "TO Disk('backups', 'lag_except_ordinary/')"
        )
        files = backup_files(node2, "lag_except_ordinary")
        assert any(
            path.startswith("metadata/rdb_except/") and "_nested" in path
            for path in files
        ), f"the ordinary table was not backed up: {files}"
        assert not any(
            path.startswith("data/rdb_except/") for path in files
        ), f"the clause did not exclude the data: {files}"
    finally:
        node2.query(
            "SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution"
        )

    for node in (node1, node2):
        node.query("DROP DATABASE IF EXISTS rdb_except SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")

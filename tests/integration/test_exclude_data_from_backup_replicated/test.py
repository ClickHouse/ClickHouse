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

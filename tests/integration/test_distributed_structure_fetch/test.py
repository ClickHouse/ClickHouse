"""A structure-less `CREATE TABLE ... ENGINE = Distributed` has to fetch the structure
of the remote table with a `DESC TABLE` service query. When the `CREATE` runs under a
context without a client version (e.g. the storage's global context), that service query
used to be sent with a zero initiator version and was rejected by `RemoteQueryExecutor`
with a logical error. See https://github.com/ClickHouse/ClickHouse/issues/113671.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Disable `with_remote_database_disk`: `test_structure_less_distributed_loaded_from_legacy_metadata`
# edits the table's metadata file on the local disk.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance("node2")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node2.query("CREATE TABLE default.remote_data (key UInt64, value String) ENGINE = MergeTree ORDER BY key")
        node2.query("INSERT INTO default.remote_data VALUES (1, 'one'), (2, 'two')")
        yield cluster
    finally:
        cluster.shutdown()


def test_structure_less_distributed_over_remote_shard(started_cluster):
    node1.query("CREATE TABLE default.dist ENGINE = Distributed('remote_only_cluster', default, remote_data)")
    assert node1.query("DESC TABLE default.dist") == ("key\tUInt64\t\t\t\t\t\nvalue\tString\t\t\t\t\t\n")
    assert node1.query("SELECT sum(key) FROM default.dist") == "3\n"
    node1.query("DROP TABLE default.dist SYNC")


def test_structure_inference_respects_show_columns_access(started_cluster):
    """The structure of a structure-less `Distributed` table must be inferred under the
    creator's context, not the storage's global context: on a cluster with a local shard
    the inference performs a `SHOW_COLUMNS` access check on the target table, and under
    the global context that check would pass for everyone, letting a user learn the
    schema of a local table they are not allowed to describe."""
    node1.query("CREATE TABLE default.local_data (key UInt64, value String) ENGINE = MergeTree ORDER BY key")
    node1.query("CREATE USER restricted IDENTIFIED WITH no_password")
    node1.query("GRANT CREATE TABLE, DROP TABLE ON default.* TO restricted")
    node1.query("GRANT REMOTE ON *.* TO restricted")
    try:
        assert "ACCESS_DENIED" in node1.query_and_get_error(
            "CREATE TABLE default.dist_local ENGINE = Distributed('local_cluster', default, local_data)",
            user="restricted",
        )
        node1.query("GRANT SHOW COLUMNS ON default.local_data TO restricted")
        node1.query(
            "CREATE TABLE default.dist_local ENGINE = Distributed('local_cluster', default, local_data)",
            user="restricted",
        )
        assert node1.query("DESC TABLE default.dist_local") == ("key\tUInt64\t\t\t\t\t\nvalue\tString\t\t\t\t\t\n")
    finally:
        node1.query("DROP TABLE IF EXISTS default.dist_local SYNC")
        node1.query("DROP USER restricted")
        node1.query("DROP TABLE default.local_data SYNC")


def recover_detached_legacy_table():
    """Recover `default.dist_legacy` if a failed `ATTACH` left it in `system.detached_tables`.

    On unfixed code the `ATTACH` in `test_structure_less_distributed_loaded_from_legacy_metadata`
    throws, and the table then stays detached: `DROP TABLE IF EXISTS` only sees attached tables,
    while the detached metadata still blocks `CREATE TABLE` (`TABLE_ALREADY_EXISTS ... (detached)`),
    so repeated runs on the module-scoped cluster (flaky-check) would cascade into setup failures
    instead of independent reproductions. Restoring the backed-up metadata brings the column list
    back, so the recovery `ATTACH` needs no structure inference and succeeds even on unfixed code.
    `cp` over the existing stripped file truncates it in place and keeps its ownership."""
    detached = node1.query(
        "SELECT count() FROM system.detached_tables WHERE database = 'default' AND table = 'dist_legacy'"
    ).strip()
    if detached == "0":
        return
    node1.exec_in_container(
        ["bash", "-c", "cp /tmp/dist_legacy.sql.bak /var/lib/clickhouse/metadata/default/dist_legacy.sql"],
        user="root",
    )
    node1.query("ATTACH TABLE default.dist_legacy")
    node1.query("DROP TABLE default.dist_legacy SYNC")


def test_structure_less_distributed_loaded_from_legacy_metadata(started_cluster):
    """A `CREATE` persists the inferred structure into the stored definition nowadays, but
    servers upgraded from older versions still carry column-less `Distributed` metadata,
    which `DatabaseOnDisk::createTableFromAST` deliberately loads with empty columns. On
    such a load `StorageDistributed`'s constructor re-infers the structure under the
    storage's global context, which carries no client version, so the `DESC TABLE` service
    query used to be sent with a zero initiator version and the table failed to attach.

    The `DETACH` must be `SYNC`: an asynchronous detach can leave the storage instance
    still tracked while the immediate `ATTACH` runs, making it throw `TABLE_ALREADY_EXISTS`
    instead of reproducing the bug."""
    recover_detached_legacy_table()
    node1.query("CREATE TABLE default.dist_legacy ENGINE = Distributed('remote_only_cluster', default, remote_data)")
    try:
        node1.query("DETACH TABLE default.dist_legacy SYNC")
        # Strip the persisted column list to obtain the metadata an older server would have
        # written, keeping a backup for the detached-table recovery above.
        node1.exec_in_container(
            [
                "bash",
                "-c",
                r"cp /var/lib/clickhouse/metadata/default/dist_legacy.sql /tmp/dist_legacy.sql.bak"
                r" && sed -i '/^($/,/^)$/d' /var/lib/clickhouse/metadata/default/dist_legacy.sql"
                r" && ! grep -q UInt64 /var/lib/clickhouse/metadata/default/dist_legacy.sql",
            ],
            user="root",
        )
        node1.query("ATTACH TABLE default.dist_legacy")
        assert node1.query("DESC TABLE default.dist_legacy") == ("key\tUInt64\t\t\t\t\t\nvalue\tString\t\t\t\t\t\n")
        assert node1.query("SELECT sum(key) FROM default.dist_legacy") == "3\n"
    finally:
        recover_detached_legacy_table()
        node1.query("DROP TABLE IF EXISTS default.dist_legacy SYNC")


def test_structure_less_distributed_in_replicated_database(started_cluster):
    """The variant BuzzHouse found: the `CREATE` is replayed by the `DDLWorker` of a
    `Replicated` database, whose query context also carried no client version."""
    node1.query("CREATE DATABASE replicated_db ENGINE = Replicated('/clickhouse/databases/replicated_db', 'shard1', 'replica1')")
    try:
        node1.query("CREATE TABLE replicated_db.dist ENGINE = Distributed('remote_only_cluster', default, remote_data)")
        assert node1.query("SELECT sum(key) FROM replicated_db.dist") == "3\n"
    finally:
        node1.query("DROP DATABASE replicated_db SYNC")

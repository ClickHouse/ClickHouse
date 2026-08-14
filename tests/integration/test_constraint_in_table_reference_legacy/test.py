import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import (
    get_database_disk_name,
    read_metadata,
    replace_text_in_metadata,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


COLUMN_REFERENCE = "a IN (default.b)"
ACCEPTED_SHAPE = "a != b"


def make_legacy_table(name, constraint_text=COLUMN_REFERENCE):
    """Produce a table whose stored definition holds `constraint_text`.

    A fresh declaration of that shape is refused, so the corrupt definition is written by
    patching the metadata of an accepted one -- the shape servers created before the check.
    """
    node.query(f"DROP TABLE IF EXISTS default.{name} SYNC")
    node.query(
        f"CREATE TABLE default.{name} (a UInt16, b UInt16, CONSTRAINT c CHECK {ACCEPTED_SHAPE}) "
        f"ENGINE = MergeTree ORDER BY a"
    )
    metadata_path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database='default' AND table='{name}'"
    ).strip()
    # Fail closed on both sides: `str.replace` rewrites the same bytes when the needle is
    # absent, so a formatter drift would leave an ordinary table the tests still accept.
    assert ACCEPTED_SHAPE in read_metadata(node, metadata_path), (
        f"persisted definition of `{name}` does not contain '{ACCEPTED_SHAPE}'; "
        f"the fixture would silently stop being a legacy table"
    )
    replace_text_in_metadata(node, metadata_path, ACCEPTED_SHAPE, constraint_text)
    assert constraint_text in read_metadata(node, metadata_path)

    db_disk_name = get_database_disk_name(node)
    if db_disk_name != "default":
        node.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")

    return metadata_path


def assert_definition_preserved(name, constraint_text=COLUMN_REFERENCE):
    definition = node.query(
        f"SELECT create_table_query FROM system.tables WHERE database='default' AND table='{name}'"
    )
    assert constraint_text in definition


def test_legacy_definition_loads_after_restart(started_cluster):
    make_legacy_table("legacy_restart")
    node.restart_clickhouse(kill=True)
    assert_definition_preserved("legacy_restart")


def test_legacy_definition_reattaches(started_cluster):
    make_legacy_table("legacy_attach")
    node.restart_clickhouse(kill=True)

    node.query("DETACH TABLE default.legacy_attach PERMANENTLY")
    node.query("ATTACH TABLE default.legacy_attach")
    assert_definition_preserved("legacy_attach")


def test_legacy_definition_can_be_repaired(started_cluster):
    make_legacy_table("legacy_repair")
    node.restart_clickhouse(kill=True)

    # DROP CONSTRAINT is the documented repair path, so it must stay reachable.
    node.query("ALTER TABLE default.legacy_repair DROP CONSTRAINT c")
    node.restart_clickhouse(kill=True)

    definition = node.query(
        "SELECT create_table_query FROM system.tables WHERE database='default' AND table='legacy_repair'"
    )
    assert "CONSTRAINT" not in definition

    node.query("INSERT INTO default.legacy_repair VALUES (1, 1)")
    assert node.query("SELECT count() FROM default.legacy_repair").strip() == "1"


def test_legacy_definition_is_not_copied_into_a_new_table(started_cluster):
    # `CREATE TABLE ... AS src` copies the source constraints instead of parsing a declaration, so a
    # legacy source must not be able to seed the rejected shape into a table created now.
    make_legacy_table("legacy_source")
    node.restart_clickhouse(kill=True)
    assert_definition_preserved("legacy_source")

    for name, statement in (
        ("copy_as", "CREATE TABLE default.copy_as AS default.legacy_source"),
        ("copy_clone", "CREATE TABLE default.copy_clone CLONE AS default.legacy_source"),
    ):
        error = node.query_and_get_error(statement)
        assert "BAD_ARGUMENTS" in error and "in the 'IN' operator" in error, error
        assert (
            node.query(
                f"SELECT count() FROM system.tables WHERE database='default' AND name='{name}'"
            ).strip()
            == "0"
        )

    # The source itself stays untouched and loadable, so this is not a new way to lose it.
    assert_definition_preserved("legacy_source")


def test_table_reference_form_is_write_dead_after_restart(started_cluster):
    # A table on the right-hand side resolves while the process that parsed the DDL lives, so the
    # form looks usable. It is not durable: only `AddDefaultDatabaseVisitor` produces the
    # `ASTTableIdentifier` that `makeSet` needs, and it never runs on a stored definition.
    node.query("DROP TABLE IF EXISTS default.srcset SYNC")
    node.query("CREATE TABLE default.srcset (a UInt16) ENGINE = Set")
    node.query("INSERT INTO default.srcset VALUES (1), (2)")

    table_reference = "a IN (default.srcset)"
    make_legacy_table("legacy_set", table_reference)
    node.restart_clickhouse(kill=True)

    assert_definition_preserved("legacy_set", table_reference)

    # 1 is in the set, so this INSERT satisfies the constraint and would land if the form worked.
    error = node.query_and_get_error("INSERT INTO default.legacy_set VALUES (1, 5)")
    assert "UNKNOWN_IDENTIFIER" in error and "default.srcset" in error, error
    assert node.query("SELECT count() FROM default.legacy_set").strip() == "0"


def test_replicated_database_recovers_legacy_definition(started_cluster):
    # A stored materialized view with an inner table is replayed with `attach` set, so that recovery
    # does not create the inner table twice. That must not make it look like a fresh declaration:
    # `recoverLostReplica` would then reject the legacy constraint and drop the view.
    zk_path = "/test/constraint_legacy_recovery"
    # The rewrite qualifies with the table's own database, so `default.b` would not appear here.
    column_reference = "a IN (rdb_legacy.b)"
    node.query("DROP DATABASE IF EXISTS rdb_legacy SYNC")
    node.query(
        f"CREATE DATABASE rdb_legacy ENGINE = Replicated('{zk_path}', 's1', 'r1')"
    )
    node.query(
        "CREATE TABLE rdb_legacy.src (a UInt16, b UInt16) ENGINE = MergeTree ORDER BY a"
    )
    node.query(
        f"CREATE MATERIALIZED VIEW rdb_legacy.mv (a UInt16, b UInt16, CONSTRAINT c CHECK {ACCEPTED_SHAPE}) "
        "ENGINE = MergeTree ORDER BY a AS SELECT a, b FROM rdb_legacy.src"
    )

    # Rewrite the Keeper metadata into the shape servers stored before the check, the way
    # make_legacy_table does it on disk. Fail closed if the needle is absent.
    zk = cluster.get_kazoo_client("zoo1")
    patched = 0
    for name in zk.get_children(f"{zk_path}/metadata"):
        value = zk.get(f"{zk_path}/metadata/{name}")[0].decode()
        if ACCEPTED_SHAPE in value:
            zk.set(
                f"{zk_path}/metadata/{name}",
                value.replace(ACCEPTED_SHAPE, column_reference).encode(),
            )
            patched += 1
    assert patched == 2, f"expected the view and its inner table, patched {patched}"

    # our_log_ptr below max_log_ptr on load is what sends the replica through recoverLostReplica.
    node.query("DETACH DATABASE rdb_legacy")
    zk.set(f"{zk_path}/replicas/s1|r1/log_ptr", b"0")
    node.query("ATTACH DATABASE rdb_legacy")

    # ATTACH only launches the DDL worker; recovery runs on that thread afterwards, so reading
    # `system.tables` right away can catch the database mid-recovery.
    node.query("SYSTEM SYNC DATABASE REPLICA rdb_legacy")
    tables = node.query(
        "SELECT name FROM system.tables WHERE database = 'rdb_legacy' ORDER BY name"
    )
    assert "mv" in tables and "src" in tables, tables
    assert column_reference in node.query(
        "SELECT create_table_query FROM system.tables WHERE database = 'rdb_legacy' AND name = 'mv'"
    )

    # The exemption is for stored metadata only: a declaration supplied now is still refused.
    error = node.query_and_get_error(
        "CREATE TABLE rdb_legacy.fresh (a UInt16, b UInt16, CONSTRAINT c CHECK a IN (b)) "
        "ENGINE = MergeTree ORDER BY a"
    )
    assert "BAD_ARGUMENTS" in error and "in the 'IN' operator" in error, error

    node.query("DROP DATABASE rdb_legacy SYNC")

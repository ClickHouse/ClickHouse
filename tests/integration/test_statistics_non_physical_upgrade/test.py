import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# Accepts an explicit STATISTICS clause on a column that is not physically stored, so it can create
# the state the current version refuses. Tables carrying it must stay usable after an upgrade.
_OLD_VERSION = "26.4"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/macros.xml"],
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=_OLD_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

_ILLEGAL_STATISTICS = "ILLEGAL_STATISTICS"
# A mutation that was queued and then failed reports this, unlike a statement refused before any
# mutation existed. The message carries the inner error's name, not this one's, so match on the text.
_MUTATION_FAILED = "Exception happened during execution of mutation"
_ZK_PATH = "/clickhouse/databases/rdb_stats"

# One table per ALTER case: a case can consume the grandfathered state, and the current version
# cannot recreate it, so the cases cannot share a table.
_ALTER_TABLES = [
    "t_clear",
    "t_rename_reuse",
    "t_rename_plain",
    "t_drop_readd",
    "t_drop_then_rename",
    "t_materialize",
]


def _assert_grandfathered(table):
    """The stored definition really carries the clause the new check refuses.

    This is the arming assertion: it proves the fixture puts the classifier in the state under
    test, which is separate from whether the assertions below can detect a regression.
    """
    create = node.query(f"SHOW CREATE TABLE {table}")
    assert "STATISTICS(tdigest)" in create, create
    assert "ALIAS" in create, create


@pytest.fixture(scope="module")
def upgraded():
    """Build every fixture on the old version, then upgrade once.

    The old binary is only reachable before the upgrade, so all state that the current version
    refuses to express is created here.
    """
    try:
        cluster.start()

        node.query(
            """
            CREATE TABLE t_alias
            (
                a UInt64,
                b UInt64 ALIAS a + 1 STATISTICS(tdigest),
                c UInt64 STATISTICS(tdigest)
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS allow_experimental_statistics = 1
            """,
        )
        node.query("INSERT INTO t_alias (a, c) VALUES (1, 10)")
        assert node.query("SELECT a, b, c FROM t_alias").strip() == "1\t2\t10"

        for table in _ALTER_TABLES:
            node.query(
                f"CREATE TABLE {table} "
                "(a UInt64, b UInt64 ALIAS a + 1 STATISTICS(tdigest), d UInt64) "
                "ENGINE = MergeTree ORDER BY tuple() "
                "SETTINGS allow_experimental_statistics = 1"
            )
            node.query(f"INSERT INTO {table} (a, d) VALUES (1, 100)")

        node.query(
            f"CREATE DATABASE rdb_stats ENGINE = Replicated('{_ZK_PATH}', 's1', 'r1')"
        )
        node.query(
            "CREATE TABLE rdb_stats.gf (a UInt64, b UInt64 ALIAS a + 1 STATISTICS(tdigest)) "
            "ENGINE = ReplicatedMergeTree ORDER BY a "
            "SETTINGS allow_experimental_statistics = 1",
        )
        # The definition the database stored in Keeper carries the refused clause, so the recovery
        # test really does feed the check that input.
        stored = node.query(
            "SELECT value LIKE '%STATISTICS%' AND value LIKE '%ALIAS%' "
            f"FROM system.zookeeper WHERE path = '{_ZK_PATH}/metadata' AND name = 'gf'"
        ).strip()
        assert stored == "1", stored

        for table in ["t_alias"] + _ALTER_TABLES:
            _assert_grandfathered(table)

        gf_uuid = node.query(
            "SELECT uuid FROM system.tables WHERE database = 'rdb_stats' AND name = 'gf'"
        ).strip()
        assert gf_uuid and gf_uuid != "00000000-0000-0000-0000-000000000000", gf_uuid

        def drop_local_copy(instance):
            """`SYSTEM DROP DATABASE REPLICA ... FROM ZKPATH` refuses to run while a local
            database claims the same Keeper path, so the local copy has to be gone rather than
            merely detached: only then is the definition read back out of Keeper. The restored
            table keeps the UUID recorded in Keeper, so its data directory has to go as well.
            """
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    "rm -rf /var/lib/clickhouse/metadata/rdb_stats "
                    "/var/lib/clickhouse/metadata/rdb_stats.sql "
                    f"/var/lib/clickhouse/store/{gf_uuid[:3]}/{gf_uuid}",
                ],
                user="root",
            )

        node.restart_with_latest_version(callback_onstop=drop_local_copy)

        yield cluster
    finally:
        cluster.shutdown()


def test_grandfathered_table_survives_upgrade(upgraded):
    # A definition read back from local metadata is not revalidated as fresh user input.
    _assert_grandfathered("t_alias")
    assert node.query("SELECT a, b, c FROM t_alias").strip() == "1\t2\t10"

    # The write path tolerates the inherited state, so the table keeps accepting data.
    node.query("INSERT INTO t_alias (a, c) VALUES (5, 50)")
    assert node.query("SELECT count() FROM t_alias").strip() == "2"

    # Fresh input naming the same shape is refused, so the check really is on.
    assert _ILLEGAL_STATISTICS in node.query_and_get_error(
        "CREATE TABLE t_fresh (a UInt64, b UInt64 ALIAS a + 1 STATISTICS(tdigest)) "
        "ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_statistics = 1"
    )

    # DETACH / ATTACH goes through the short ATTACH path, which loads stored metadata.
    node.query("DETACH TABLE t_alias")
    node.query("ATTACH TABLE t_alias")
    _assert_grandfathered("t_alias")

    # A restart reloads every definition from metadata, the same reparse a replica restart does.
    node.restart_clickhouse()
    _assert_grandfathered("t_alias")
    assert node.query("SELECT count() FROM t_alias").strip() == "2"


def test_alter_keeps_inherited_state_alterable(upgraded):
    """The ALTER-side classifier decides whether this statement produced the invalid state.

    These are the cases the bookkeeping exists for, so they are the ones that regress silently
    if it is simplified away.
    """
    # CLEAR COLUMN only erases data and leaves the column in place, so the state stays inherited.
    node.query("ALTER TABLE t_clear CLEAR COLUMN b")
    _assert_grandfathered("t_clear")

    # A rename reusing the freed name: the legacy column moves aside and an unrelated stored
    # column takes its name. The column now under the old name is a different, physical one.
    node.query(
        "ALTER TABLE t_rename_reuse RENAME COLUMN b TO b_legacy, RENAME COLUMN d TO b"
    )
    create = node.query("SHOW CREATE TABLE t_rename_reuse")
    assert "b_legacy" in create, create
    assert "STATISTICS(tdigest)" in create, create

    # A plain rename carries the whole description across, so the state is still inherited.
    node.query("ALTER TABLE t_rename_plain RENAME COLUMN b TO bb")
    assert "STATISTICS(tdigest)" in node.query("SHOW CREATE TABLE t_rename_plain")

    # Dropping a column and then renaming that same freed name away moves nothing, so the legacy
    # column keeps its own name and its inherited state. `IF EXISTS` is decided against the
    # definition before any command ran, so such a rename is not reported as ignored.
    node.query(
        "ALTER TABLE t_drop_then_rename DROP COLUMN d, RENAME COLUMN IF EXISTS d TO b"
    )
    _assert_grandfathered("t_drop_then_rename")


def test_alter_refuses_state_it_produces_itself(upgraded):
    # Dropping the column ends its identity, so the alias that then takes the name is a new
    # column and the statistics it declares belong to this statement.
    assert _ILLEGAL_STATISTICS in node.query_and_get_error(
        "ALTER TABLE t_drop_readd DROP COLUMN b, "
        "ADD COLUMN b UInt64 ALIAS a + 2 STATISTICS(tdigest)"
    )
    # The refusal left the table as it was.
    _assert_grandfathered("t_drop_readd")


def test_materialize_statistics_leaves_no_unfinished_mutation(upgraded):
    """`MATERIALIZE STATISTICS` over a legacy definition must not queue work it can never do.

    Statistics of a column that is not stored cannot be built from any part, so a mutation queued
    for one has nothing to retry against and stays in `system.mutations` for good.
    """
    pending = (
        "SELECT count() FROM system.mutations "
        "WHERE database = currentDatabase() AND table = 't_materialize' AND NOT is_done"
    )

    # `ALL` passes over the column that is not stored instead of refusing, so it is queued and
    # finishes. Assert an entry exists as well as that none is pending, otherwise a command that was
    # never queued at all would read the same.
    node.query(
        "ALTER TABLE t_materialize MATERIALIZE STATISTICS ALL",
        settings={"mutations_sync": 2},
    )
    assert (
        node.query(
            "SELECT count() > 0, countIf(NOT is_done) FROM system.mutations "
            "WHERE database = currentDatabase() AND table = 't_materialize'"
        ).strip()
        == "1\t0"
    )

    # Naming the column explicitly is refused before any mutation is queued today, and becomes a
    # logged no-op once that synchronous check is removed (#115769), so both are accepted. Only that
    # refusal is: a queued mutation that then failed is the state this exists to catch.
    _, error = node.query_and_get_answer_with_error(
        "ALTER TABLE t_materialize MATERIALIZE STATISTICS b SETTINGS mutations_sync = 2"
    )
    if error:
        assert _ILLEGAL_STATISTICS in error, error
        assert _MUTATION_FAILED not in error, error
    assert_eq_with_retry(node, pending, "0", retry_count=60, sleep_time=1)

    # The table is still readable through the alias, and its definition is unchanged.
    assert (
        node.query("SELECT a, b, d FROM t_materialize ORDER BY a").strip()
        == "1\t2\t100"
    )
    _assert_grandfathered("t_materialize")


def test_replicated_database_recovery_accepts_stored_definition(upgraded):
    """A `Replicated` database recovers a table from the definition it stored in Keeper.

    That definition arrives as a plain `CREATE` with no metadata transaction, so only the
    recovery flag tells it apart from fresh user input. Without that the recovery refuses the
    database's own stored definition and the replica cannot be cleaned up.
    """
    assert (
        node.query(
            "SELECT count() FROM system.databases WHERE name = 'rdb_stats'"
        ).strip()
        == "0"
    )

    # `WITH TABLES` restores every table from the metadata stored in Keeper before dropping the
    # replica, so it re-executes that grandfathered `CREATE` on the current version.
    #
    # Only the statistics check is under test. Dropping the local copy leaves the table's own
    # replica registered in Keeper, so the restore reaches a later step of its own and reports
    # REPLICA_ALREADY_EXISTS whether or not that check is present. What must not happen is the
    # restore refusing the stored definition before reaching it, so assert on which error comes
    # back: that keeps the arm measuring the check rather than the fixture.
    error = node.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM ZKPATH '{_ZK_PATH}' WITH TABLES",
        settings={"receive_timeout": 120},
    )
    assert _ILLEGAL_STATISTICS not in error, error
    assert "REPLICA_ALREADY_EXISTS" in error, error

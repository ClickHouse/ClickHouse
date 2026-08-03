import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    stay_alive=True,
)
# A two-replica Replicated database. A replay there shares SECONDARY_CREATE with a RESTORE and is
# separated from it only by the restore flag, so it is the arm that proves the flag did not
# over-reach: the replay of an over-long definition must still load.
replicated_1 = cluster.add_instance(
    "replicated_1",
    macros={"shard": "shard1", "replica": "1"},
    stay_alive=True,
    with_zookeeper=True,
)
replicated_2 = cluster.add_instance(
    "replicated_2",
    macros={"shard": "shard1", "replica": "2"},
    stay_alive=True,
    with_zookeeper=True,
)

# 252 is over the limit and is the length from the original report. The DDL check rejects it, so the
# table can only be produced by planting the definition into the stored metadata, the way a table
# created before the check existed looks on disk.
LEGACY_NAME = "c" * 252

# The largest accepted stream name, so `<name>.bin` is exactly one path component.
ACCEPTED_NAME = "c" * 251


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def plant_over_long_column(table, engine):
    """Seed a table with a short column, then rewrite its stored definition to carry an over-long
    one. Only the metadata .sql is edited; data files are never touched."""
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = {engine}")

    # system.tables reports the path relative to the data directory.
    metadata_path = (
        "/var/lib/clickhouse/"
        + node.query(
            f"SELECT metadata_path FROM system.tables WHERE database = 'default' AND name = '{table}'"
        )
        .strip()
        .lstrip("/")
    )

    node.query(f"DETACH TABLE {table}")
    definition = node.exec_in_container(["cat", metadata_path])
    anchor = "`a` UInt64"
    assert anchor in definition, definition
    planted = definition.replace(anchor, f"`{LEGACY_NAME}` UInt64")
    node.exec_in_container(
        ["bash", "-c", f"cat > {metadata_path} <<'PLANTED_EOF'\n{planted}\nPLANTED_EOF"]
    )
    # The definition is only read at load time, so restart rather than ATTACH in place.
    node.restart_clickhouse()


@pytest.mark.parametrize("engine", ["Log", "TinyLog"])
def test_legacy_over_long_column_name_keeps_the_table_loadable(engine):
    """A table whose stored metadata carries a column name that no longer fits a path component must
    still load, and must report the limit on write instead of leaking a filesystem error."""
    table = f"legacy_{engine.lower()}"
    plant_over_long_column(table, engine)

    # 1. the table loads and is readable. It is necessarily empty: sizes.json is only written by
    # saveFileSizes, which is the call that refuses, so such a table can never have been written.
    assert node.query(f"SELECT count() FROM {table}").strip() == "0"

    # 2. the planted name really is what the table carries
    assert LEGACY_NAME in node.query(f"SHOW CREATE TABLE {table}")

    # 3. writing reports the limit, naming the file and the limit, instead of STD_EXCEPTION
    error = node.query_and_get_error(f"INSERT INTO {table} VALUES (1)")
    assert "ARGUMENT_OUT_OF_BOUND" in error, error
    assert "STD_EXCEPTION" not in error, error
    assert LEGACY_NAME in error, error
    assert "251" in error, error

    # 4. the table can still be dropped
    node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("engine", ["Log", "TinyLog"])
def test_restart_is_stable(engine):
    """Loading the planted definition must be repeatable, not just survivable once."""
    table = f"restart_{engine.lower()}"
    plant_over_long_column(table, engine)

    for _ in range(2):
        node.restart_clickhouse()
        assert node.query(f"SELECT count() FROM {table}").strip() == "0"

    node.query(f"DROP TABLE {table} SYNC")


def test_restore_of_a_legacy_table_is_rejected():
    """A RESTORE introduces a definition, so it is checked like a CREATE: it arrives as
    SECONDARY_CREATE, which the loading mode alone cannot tell from a Replicated DDL replay, and
    args.is_restore_from_backup is the term that separates them. Restoring a table that could never
    accept a row is not a successful restore."""
    plant_over_long_column("legacy_for_backup", "Log")

    backup = "Disk('backups', 'legacy_log')"
    node.query(f"BACKUP TABLE legacy_for_backup TO {backup}")
    node.query("DROP TABLE legacy_for_backup SYNC")

    error = node.query_and_get_error(f"RESTORE TABLE legacy_for_backup FROM {backup}")
    assert "ARGUMENT_OUT_OF_BOUND" in error, error
    assert LEGACY_NAME in error, error


def test_create_as_a_legacy_source_is_rejected():
    """`CREATE TABLE ... AS <source>` re-materializes the column list into a fresh definition, so it
    is checked: a new table born unwritable is worse than an explicit refusal."""
    plant_over_long_column("legacy_source", "Log")

    error = node.query_and_get_error(
        "CREATE TABLE clone_of_legacy AS legacy_source ENGINE = Log"
    )
    assert "ARGUMENT_OUT_OF_BOUND" in error, error
    assert LEGACY_NAME in error, error

    node.query("DROP TABLE legacy_source SYNC")


def test_replicated_database_replay_still_loads():
    """The arm that bounds the restore term. A RESTORE and a Replicated DDL replay share
    SECONDARY_CREATE and are separated only by `is_restore_from_backup`, so a check keyed on the
    loading mode alone would refuse this replay and make an existing table unrecoverable. A replica
    joining an existing database replays every definition from Keeper, so planting there and then
    joining exercises the replay path without corrupting any state."""
    for replica in (replicated_1, replicated_2):
        replica.query("DROP DATABASE IF EXISTS replicated_db SYNC")

    replicated_1.query(
        "CREATE DATABASE replicated_db "
        "ENGINE = Replicated('/test/replicated_db', 'shard1', '1')"
    )
    replicated_1.query("CREATE TABLE replicated_db.legacy (a UInt64) ENGINE = Log")

    # A fresh CREATE on a Replicated database is still checked.
    error = replicated_1.query_and_get_error(
        f"CREATE TABLE replicated_db.fresh (`{LEGACY_NAME}` UInt8) ENGINE = Log"
    )
    assert "ARGUMENT_OUT_OF_BOUND" in error, error

    keeper = cluster.get_kazoo_client("zoo1")
    keeper_path = "/test/replicated_db/metadata/legacy"
    definition = keeper.get(keeper_path)[0].decode()
    assert "`a` UInt64" in definition, definition
    keeper.set(keeper_path, definition.replace("`a` UInt64", f"`{LEGACY_NAME}` UInt64").encode())

    # The joining replica replays the planted definition.
    replicated_2.query(
        "CREATE DATABASE replicated_db "
        "ENGINE = Replicated('/test/replicated_db', 'shard1', '2')"
    )
    replicated_2.query("SYSTEM SYNC DATABASE REPLICA replicated_db")

    assert LEGACY_NAME in replicated_2.query("SHOW CREATE TABLE replicated_db.legacy")
    assert replicated_2.query("SELECT count() FROM replicated_db.legacy").strip() == "0"

    for replica in (replicated_1, replicated_2):
        replica.query("DROP DATABASE replicated_db SYNC")


@pytest.mark.parametrize("engine", ["Log", "TinyLog"])
def test_accepted_length_still_works(engine):
    """The largest accepted name must keep working, so an off-by-one in the limit cannot pass."""
    table = f"accepted_{engine.lower()}"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"CREATE TABLE {table} (`{ACCEPTED_NAME}` UInt8) ENGINE = {engine}")
    node.query(f"INSERT INTO {table} VALUES (1)")
    assert node.query(f"SELECT count() FROM {table}").strip() == "1"

    # It survives a restart, which is the path the freshness gate exempts.
    node.restart_clickhouse()
    assert node.query(f"SELECT count() FROM {table}").strip() == "1"

    node.query(f"DROP TABLE {table} SYNC")

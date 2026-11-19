import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The strippable template carries no {database}/{table}, so a table created under it stores a
# path equal to the template verbatim and DDLAdjustingForBackupVisitor removes the engine
# arguments from the backed-up definition. That stripping is the precondition of this whole
# test: without it the restore reuses the source path and never reaches the injection branch.
CONFIG_DIR = "/etc/clickhouse-server/config.d"
NAME_BASED = "/clickhouse/tables/{shard}/{database}/{table}"

node = cluster.add_instance(
    "node",
    main_configs=["configs/backups_disk.xml", "configs/strippable.xml"],
    external_dirs=["/backups/"],
    with_zookeeper=True,
    stay_alive=True,
    # legacy_path is a configured macro whose VALUE carries {database}, which is how a stored
    # path can keep that macro past table creation: the first expansion pass unfolds {database}
    # itself but not a macro that merely contains it.
    macros={
        "shard": "s1",
        "replica": "r1",
        "legacy_path": "/clickhouse/tables/legacy/{database}/",
    },
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_default_replica_path(template):
    """Swap default_replica_path and restart, so the value is active for the next statement."""
    node.replace_config(
        f"{CONFIG_DIR}/strippable.xml",
        "<clickhouse>"
        f"<default_replica_path>{template}</default_replica_path>"
        "<default_replica_name>{replica}</default_replica_name>"
        "</clickhouse>",
    )
    node.restart_clickhouse()
    active = node.query(
        "SELECT value FROM system.server_settings WHERE name = 'default_replica_path'"
    ).strip()
    assert active == template, f"config swap did not take effect: {active}"


def backup_table_definitions(backup_dir):
    """Every table definition stored in a backup, whatever the escaped file names are.

    Empty output means no table was stored, which is the observable shape of a table silently
    dropped from a backup, so the caller can tell that apart from a definition it dislikes.
    """
    return node.exec_in_container(
        ["bash", "-c", f"cat /backups/{backup_dir}/metadata/*/*.sql 2>/dev/null || true"]
    )


def zookeeper_children(path):
    return sorted(
        node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{path}'"
        ).split()
    )


def total_replicas(database, table):
    return int(
        node.query(
            "SELECT total_replicas FROM system.replicas "
            f"WHERE database = '{database}' AND table = '{table}'"
        ).strip()
    )


def make_stripped_backup(source_db, source_table, backup_dir):
    """Create a table under the strippable template and back it up with its args stripped.

    The strippable template carries no {database}/{table}, so every table created under it
    resolves to the very same Keeper path. Each case therefore needs its own root, otherwise
    the second one hits REPLICA_ALREADY_EXISTS on the first one's leftover replica.
    """
    set_default_replica_path(f"/clickhouse/tables/fixedroot_{backup_dir}/{{shard}}")
    node.query(f"DROP DATABASE IF EXISTS {source_db} SYNC")
    node.query(f"CREATE DATABASE {source_db}")
    node.query(
        f"CREATE TABLE {source_db}.`{source_table}` (c0 Int) "
        "ENGINE = ReplicatedMergeTree ORDER BY c0"
    )
    node.query(f"INSERT INTO {source_db}.`{source_table}` VALUES (1), (2)")
    node.query(f"BACKUP TABLE {source_db}.`{source_table}` TO File('{backup_dir}')")

    # The strip precondition, asserted explicitly. If the definition still carries engine
    # arguments the restore below reuses the SOURCE path and fails with REPLICA_ALREADY_EXISTS,
    # which is a broken fixture rather than the defect under test.
    definition = backup_table_definitions(backup_dir)
    assert "ReplicatedMergeTree ORDER BY" in definition, definition
    assert "/clickhouse/tables" not in definition, (
        "backup definition kept its engine arguments, so the injection branch is "
        f"never reached and this test would be vacuous: {definition}"
    )


def make_victim(database, table="victim"):
    """A third party's table under the migrated template, with its untouched state asserted."""
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database}")
    node.query(
        f"CREATE TABLE {database}.{table} (c0 Int) ENGINE = ReplicatedMergeTree ORDER BY c0"
    )
    victim_zk = f"/clickhouse/tables/s1/{database}/{table}"
    assert total_replicas(database, table) == 1
    assert zookeeper_children(f"{victim_zk}/replicas") == ["r1"]
    return victim_zk


def assert_victim_untouched(database, victim_zk, table="victim"):
    """The oracle of every treatment row: the phantom replica is what the fix has to prevent.

    In the default variant both binaries end in an error, so the statement's outcome cannot
    discriminate them and only the Keeper state can.
    """
    assert zookeeper_children(f"{victim_zk}/replicas") == ["r1"]
    assert total_replicas(database, table) == 1


def test_restore_as_unsafe_name_rejects_injected_default(start_cluster):
    """T1: a stripped backup restored under a name that escapes the victim's subtree."""
    backup_dir = "t1"
    make_stripped_backup("t1_src", "t", backup_dir)

    set_default_replica_path(NAME_BASED)
    victim_zk = make_victim("t1_dst")

    error = node.query_and_get_error(
        "RESTORE TABLE t1_src.t AS t1_dst.`victim/replicas/ghostC` "
        f"FROM File('{backup_dir}')"
    )
    # The path must be rejected before any znode is minted. REPLICA_ALREADY_EXISTS here would
    # mean the source path was reused, i.e. the strip precondition did not hold and nothing was
    # injected; CANNOT_RESTORE_TABLE means the table was created first and the phantom replica
    # already landed in the victim's subtree.
    assert "BAD_ARGUMENTS" in error, error
    assert "REPLICA_ALREADY_EXISTS" not in error, error
    assert "CANNOT_RESTORE_TABLE" not in error, error

    assert_victim_untouched("t1_dst", victim_zk)


def test_restore_as_unsafe_name_rejects_when_definition_check_is_skipped(start_cluster):
    """T1b: the same injection with allow_different_table_def = 1.

    The definition comparison is the only thing that stops this restore from completing, and
    it is skippable with an unprivileged setting, so without the fix the statement succeeds
    outright and still plants the phantom replica. That makes this the row where the two arms
    differ in outcome as well as in Keeper state.
    """
    backup_dir = "t1b"
    make_stripped_backup("t1b_src", "t", backup_dir)

    set_default_replica_path(NAME_BASED)
    victim_zk = make_victim("t1b_dst")

    error = node.query_and_get_error(
        "RESTORE TABLE t1b_src.t AS t1b_dst.`victim/replicas/ghostC` "
        f"FROM File('{backup_dir}') SETTINGS allow_different_table_def = 1"
    )
    assert "BAD_ARGUMENTS" in error, error
    assert "REPLICA_ALREADY_EXISTS" not in error, error

    assert_victim_untouched("t1b_dst", victim_zk)


def test_restore_as_unsafe_name_rejects_for_an_unprivileged_user(start_cluster):
    """T2: T1b's injection run by a user with no administrative privilege.

    Its grants are the ones the scenario itself needs: reading the backup, and creating the
    restored table. Neither the rename nor allow_different_table_def needs anything further.
    ACCESS_DENIED is asserted absent rather than assumed: a run refused for lack of rights would
    pass this row on either binary without ever reaching the resolver.
    """
    backup_dir = "t2"
    make_stripped_backup("t2_src", "t", backup_dir)

    set_default_replica_path(NAME_BASED)
    victim_zk = make_victim("t2_dst")

    node.query("DROP USER IF EXISTS t2_user")
    node.query("CREATE USER t2_user IDENTIFIED WITH plaintext_password BY 'p'")
    node.query("GRANT SELECT, INSERT, CREATE TABLE, BACKUP ON *.* TO t2_user")
    # Reading a File() backup is a source grant of its own; without it the restore stops at
    # READ ON FILE and this row would never reach the path resolver on either binary.
    node.query("GRANT READ ON FILE TO t2_user")

    error = node.query_and_get_error(
        "RESTORE TABLE t2_src.t AS t2_dst.`victim/replicas/ghostC` "
        f"FROM File('{backup_dir}') SETTINGS allow_different_table_def = 1",
        user="t2_user",
        password="p",
    )
    assert "ACCESS_DENIED" not in error, error
    assert "BAD_ARGUMENTS" in error, error

    assert_victim_untouched("t2_dst", victim_zk)


def test_restore_without_rename_rejects_an_unsafe_created_name(start_cluster):
    """T4: no rename at all, the table was CREATED with an unsafe name under the old template.

    A rename is not what makes the path unsafe: the migrated template expands {table} against
    whatever name the definition already carries. This row is why the fix keys on the origin of
    the resolved value rather than on the restore renaming anything.
    """
    backup_dir = "t4"
    unsafe = "victim/replicas/ghostD"
    make_stripped_backup("t4_dst", unsafe, backup_dir)

    # The table goes away but its database stays, so the restore below needs no rename at all
    # and still lands inside the victim created after the migration.
    node.query(f"DROP TABLE t4_dst.`{unsafe}` SYNC")
    set_default_replica_path(NAME_BASED)
    node.query(
        "CREATE TABLE t4_dst.victim (c0 Int) ENGINE = ReplicatedMergeTree ORDER BY c0"
    )
    victim_zk = "/clickhouse/tables/s1/t4_dst/victim"
    assert total_replicas("t4_dst", "victim") == 1
    assert zookeeper_children(f"{victim_zk}/replicas") == ["r1"]

    error = node.query_and_get_error(
        f"RESTORE TABLE t4_dst.`{unsafe}` FROM File('{backup_dir}') "
        "SETTINGS allow_different_table_def = 1"
    )
    assert "BAD_ARGUMENTS" in error, error
    assert "REPLICA_ALREADY_EXISTS" not in error, error

    assert_victim_untouched("t4_dst", victim_zk)


def restore_into_replicated_database(tag, entry_version):
    """The T1b injection into a Replicated database at a given DDL entry format version.

    Returns the restore's error, the serialized log entry and the victim's path. is_backup_restore
    is only written into the entry from version 6 on, so the entry text is what tells apart an
    initiator enforcing from its own in-memory flag and one relying on the serialized copy.
    """
    make_stripped_backup(f"{tag}_src", "t", tag)

    set_default_replica_path(NAME_BASED)
    db_zk = f"/clickhouse/databases/{tag}"
    node.query(f"DROP DATABASE IF EXISTS {tag}_dst SYNC")
    node.query(f"CREATE DATABASE {tag}_dst ENGINE = Replicated('{db_zk}', 's1', 'r1')")
    node.query(
        f"CREATE TABLE {tag}_dst.victim (c0 Int) ENGINE = ReplicatedMergeTree ORDER BY c0"
    )
    victim_zk = f"/clickhouse/tables/s1/{tag}_dst/victim"
    assert total_replicas(f"{tag}_dst", "victim") == 1
    assert zookeeper_children(f"{victim_zk}/replicas") == ["r1"]

    error = node.query_and_get_error(
        f"RESTORE TABLE {tag}_src.t AS {tag}_dst.`victim/replicas/ghostC` "
        f"FROM File('{tag}') SETTINGS allow_different_table_def = 1",
        settings={"distributed_ddl_entry_format_version": entry_version},
    )
    entry = node.query(
        f"SELECT value FROM system.zookeeper WHERE path = '{db_zk}/log' "
        "ORDER BY name DESC LIMIT 1"
    )
    return error, entry, victim_zk


def test_restore_into_replicated_database_rejects_at_the_oss_entry_version(start_cluster):
    """R5a: entry version 5, the OSS default, whose entry cannot carry the restore flag."""
    error, entry, victim_zk = restore_into_replicated_database("r5a", 5)

    assert "BAD_ARGUMENTS" in error, error
    assert "REPLICA_ALREADY_EXISTS" not in error, error
    # Without the flag in the entry, a rejection can only come from the initiator's own copy of
    # it. If this string ever appears the row is no longer testing version 5.
    assert "is_backup_restore" not in entry, entry

    assert_victim_untouched("r5a_dst", victim_zk)


def test_restore_into_replicated_database_rejects_at_the_flag_carrying_version(start_cluster):
    """R5b: entry version 6, which Cloud and CI pin, whose entry does carry the flag."""
    error, entry, victim_zk = restore_into_replicated_database("r5b", 6)

    assert "BAD_ARGUMENTS" in error, error
    assert "is_backup_restore" in entry, entry

    assert_victim_untouched("r5b_dst", victim_zk)


def test_plain_create_of_an_unsafe_name_is_still_rejected(start_cluster):
    """R11: the CREATE route's own rejection is unchanged, and so is the flag's value there."""
    set_default_replica_path(NAME_BASED)
    node.query("DROP DATABASE IF EXISTS r11 SYNC")
    node.query("CREATE DATABASE r11")
    error = node.query_and_get_error(
        "CREATE TABLE r11.`v/replicas/g` (c0 Int) ENGINE = ReplicatedMergeTree ORDER BY c0"
    )
    assert "BAD_ARGUMENTS" in error, error


def test_restore_as_safe_name_still_works(start_cluster):
    """C2: a legitimate rename under the same migrated template must stay green.

    The definition check has to be skipped for a renamed restore of a stripped backup to
    complete at all: the backup definition carries no engine arguments while the created table
    carries the freshly injected ones, so the two can never compare equal on this route.
    """
    backup_dir = "c2"
    make_stripped_backup("c2_src", "t", backup_dir)

    set_default_replica_path(NAME_BASED)
    node.query("DROP DATABASE IF EXISTS c2_dst SYNC")
    node.query("CREATE DATABASE c2_dst")
    node.query(
        f"RESTORE TABLE c2_src.t AS c2_dst.safe_copy FROM File('{backup_dir}') "
        "SETTINGS allow_different_table_def = 1"
    )

    path = node.query(
        "SELECT zookeeper_path FROM system.replicas "
        "WHERE database = 'c2_dst' AND table = 'safe_copy'"
    ).strip()
    assert path == "/clickhouse/tables/s1/c2_dst/safe_copy", path
    assert node.query("SELECT count() FROM c2_dst.safe_copy").strip() == "2"


def test_restore_as_safe_name_without_the_skip_setting(start_cluster):
    """C2b: the same safe rename without allow_different_table_def.

    The definition comparison rejects it on both sides of the fix, but the path it resolved to
    must still be the destination's own subtree rather than anything derived from another table.
    """
    backup_dir = "c2b"
    make_stripped_backup("c2b_src", "t", backup_dir)

    set_default_replica_path(NAME_BASED)
    node.query("DROP DATABASE IF EXISTS c2b_dst SYNC")
    node.query("CREATE DATABASE c2b_dst")
    error = node.query_and_get_error(
        f"RESTORE TABLE c2b_src.t AS c2b_dst.safe_copy FROM File('{backup_dir}')"
    )
    assert "CANNOT_RESTORE_TABLE" in error, error
    assert "BAD_ARGUMENTS" not in error, error
    # The rejection is the definition comparison, so the path that was resolved before it must
    # be the destination's own, proving a safe rename is never what this fix refuses.
    assert "/clickhouse/tables/{shard}/c2b_dst/safe_copy" in error, error


def test_unchanged_config_is_not_affected(start_cluster):
    """C5: without the config migration the source path is reused, so the route is closed.

    This control is what proves the migration is load-bearing: the outcome here must be
    REPLICA_ALREADY_EXISTS both before and after the fix, never BAD_ARGUMENTS.
    """
    backup_dir = "c5"
    set_default_replica_path(NAME_BASED)
    node.query("DROP DATABASE IF EXISTS c5_src SYNC")
    node.query("CREATE DATABASE c5_src")
    node.query(
        "CREATE TABLE c5_src.t (c0 Int) ENGINE = ReplicatedMergeTree ORDER BY c0"
    )
    node.query(f"BACKUP TABLE c5_src.t TO File('{backup_dir}')")
    # Never migrated: the stored definition keeps its arguments. {database}/{table} are
    # expanded at CREATE, while {shard} stays a macro, so the strip cannot fire.
    definition = backup_table_definitions(backup_dir)
    assert "/clickhouse/tables/{shard}/c5_src/t" in definition, definition

    victim_zk = make_victim("c5_dst")
    error = node.query_and_get_error(
        f"RESTORE TABLE c5_src.t AS c5_dst.`victim/replicas/g` FROM File('{backup_dir}')"
    )
    assert "REPLICA_ALREADY_EXISTS" in error, error
    assert_victim_untouched("c5_dst", victim_zk)


def test_backup_still_includes_a_table_whose_path_expands_an_unsafe_database(start_cluster):
    """C7: backing up a legacy table whose stored path re-expands {database} must keep working.

    The path a stored definition carries is resolved again while the backup is collected, and
    this row is the only one where that value is unsafe: the database name became path-unsafe
    after the table was created. What it asserts is what a single replica can observe, namely
    that BACKUP succeeds and the definition is stored; the resolved path itself only reaches the
    backup coordination of a replica that lacks the table locally.
    """
    backup_dir = "c7"
    set_default_replica_path(NAME_BASED)
    node.query("DROP DATABASE IF EXISTS `c7/src` SYNC")
    node.query("DROP DATABASE IF EXISTS c7_src SYNC")
    node.query("CREATE DATABASE c7_src")
    node.query(
        "CREATE TABLE c7_src.t (c0 Int) ENGINE = ReplicatedMergeTree('{legacy_path}c7', 'r1') "
        "ORDER BY c0"
    )
    node.query("INSERT INTO c7_src.t VALUES (1), (2)")
    # The database name becomes path-unsafe only now, so the stored {database} expands to a
    # value the checks would refuse if they ran on this route.
    node.query("RENAME DATABASE c7_src TO `c7/src`")

    node.query(f"BACKUP DATABASE `c7/src` TO File('{backup_dir}')")

    definition = backup_table_definitions(backup_dir)
    assert "ReplicatedMergeTree" in definition, (
        "the table was dropped from the backup, which is what a validating metadata reader "
        f"produces: {definition!r}"
    )
    assert "{legacy_path}" in definition, definition

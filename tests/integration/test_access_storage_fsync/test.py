import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# The default node uses the `local_directory` access storage (DiskAccessStorage),
# so access DDL writes `<uuid>.sql` files to disk - unlike stateless tests which
# override the storage to `<memory/>`.
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _profile_events(query_id):
    """FileSync and DirectorySync counted for the given query."""
    instance.query("SYSTEM FLUSH LOGS query_log")
    row = instance.query(
        "SELECT ProfileEvents['FileSync'], ProfileEvents['DirectorySync'] "
        "FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip()
    file_sync, dir_sync = (int(x) for x in row.split("\t"))
    return file_sync, dir_sync


def _run(query, fsync_metadata):
    query_id = str(uuid.uuid4())
    instance.query(query, query_id=query_id, settings={"fsync_metadata": fsync_metadata})
    return _profile_events(query_id)


def test_access_entity_writes_are_fsynced():
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/68958

    DiskAccessStorage used to write access-entity `.sql` files (and remove them)
    without any fsync, so an acknowledged CREATE/ALTER/DROP USER could be silently
    lost on power loss. It must now honor `fsync_metadata` like the other on-disk
    DDL-entity stores.

    We assert the fsync path is taken (rather than simulating power loss) by reading
    the FileSync / DirectorySync ProfileEvents of each DDL query: the `.sql` write runs
    synchronously on the query thread, so its fsync is attributed to the query.
    """
    user = "u_fsync"
    instance.query(f"DROP USER IF EXISTS {user}")

    # With fsync_metadata = 1 the file content and the parent directory are both synced.
    # CREATE / GRANT / ALTER rewrite the entity `.sql` file (file + directory fsync);
    # DROP only unlinks it (directory fsync, no file content to sync).
    file_sync, dir_sync = _run(f"CREATE USER {user} IDENTIFIED WITH no_password", 1)
    assert file_sync >= 1 and dir_sync >= 1, f"CREATE USER: {file_sync}, {dir_sync}"

    file_sync, dir_sync = _run(f"GRANT SELECT ON default.* TO {user}", 1)
    assert file_sync >= 1 and dir_sync >= 1, f"GRANT: {file_sync}, {dir_sync}"

    file_sync, dir_sync = _run(
        f"ALTER USER {user} IDENTIFIED WITH plaintext_password BY 'p'", 1
    )
    assert file_sync >= 1 and dir_sync >= 1, f"ALTER USER: {file_sync}, {dir_sync}"

    file_sync, dir_sync = _run(f"DROP USER {user}", 1)
    assert dir_sync >= 1, f"DROP USER directory not synced: {file_sync}, {dir_sync}"

    # With fsync_metadata = 0 the setting must be honored: no forced sync.
    instance.query(f"DROP USER IF EXISTS {user}")
    file_sync, dir_sync = _run(f"CREATE USER {user} IDENTIFIED WITH no_password", 0)
    assert file_sync == 0 and dir_sync == 0, (
        f"fsync_metadata=0 not honored: {file_sync}, {dir_sync}"
    )
    file_sync, dir_sync = _run(f"DROP USER {user}", 0)
    assert file_sync == 0 and dir_sync == 0, (
        f"fsync_metadata=0 not honored on DROP: {file_sync}, {dir_sync}"
    )


def test_list_files_are_fsynced():
    """
    The `.list` index files are written by `writeListFile`, which must also honor
    `fsync_metadata`. `SYSTEM RELOAD USERS` rebuilds and rewrites every `.list` file
    synchronously on the query thread, so its FileSync ProfileEvent covers that path.
    """
    user = "u_fsync_list"
    instance.query(f"DROP USER IF EXISTS {user}")
    instance.query(f"CREATE USER {user} IDENTIFIED WITH no_password")

    # SYSTEM RELOAD USERS -> reloadAllAndRebuildLists() -> writeLists() writes all the
    # `.list` files in-place; with fsync_metadata = 1 each one is fsync'd (file content only,
    # no rename -> no directory sync).
    file_sync, _ = _run("SYSTEM RELOAD USERS", 1)
    assert file_sync >= 1, f"list files not fsynced on RELOAD USERS: FileSync={file_sync}"

    # fsync_metadata = 0 -> no forced sync.
    file_sync, _ = _run("SYSTEM RELOAD USERS", 0)
    assert file_sync == 0, (
        f"fsync_metadata=0 not honored on RELOAD USERS: FileSync={file_sync}"
    )

    instance.query(f"DROP USER IF EXISTS {user}")


ACCESS_DIR = "/var/lib/clickhouse/access"


def _forge_stale_entity(seed, lost):
    """
    Reproduce the power-loss window on disk: a fresh `<uuid>.sql` for `lost` that survived
    (was fsync'd) but is absent from the stale `.list` index, and remove the rebuild marker.
    Returns the forged file path.
    """
    seed_sql = instance.exec_in_container(
        ["bash", "-c", f"grep -l 'ATTACH USER {seed} ' {ACCESS_DIR}/*.sql"], user="root"
    ).strip()
    new_path = f"{ACCESS_DIR}/{uuid.uuid4()}.sql"
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"sed 's/{seed}/{lost}/g' {seed_sql} > {new_path} && "
            f"rm -f {ACCESS_DIR}/need_rebuild_lists.mark",
        ],
        user="root",
    )
    return new_path


def test_rebuild_marker_makes_stale_sql_recoverable():
    """
    Recovery test for the deferred-rebuild marker (need_rebuild_lists.mark).

    A DDL drops the `need_rebuild_lists.mark` marker (scheduleWriteLists) and writes the
    fsync'd `<uuid>.sql`, then defers the `.list` index rewrite to a background thread. On
    restart the constructor only rescans the `.sql` files when the marker is present;
    otherwise it trusts the (possibly stale) `.list` indexes. So the marker must be durable,
    or a power loss before the background rewrite makes an acknowledged CREATE/ALTER/DROP
    USER unreachable after restart even though its `.sql` survived.

    We reproduce that on-disk state - a fresh `<uuid>.sql` absent from the stale `.list`
    index - in both directions:
      - marker MISSING: the entity is NOT recovered, proving the marker is load-bearing;
      - marker PRESENT (the durable state the fix guarantees): the entity IS recovered.
    """
    seed = "u_recover_seed"
    lost = "u_recover_lost"
    instance.query(f"DROP USER IF EXISTS {seed}")
    instance.query(f"DROP USER IF EXISTS {lost}")

    # A persisted user so the `.list` files and a real `<uuid>.sql` exist on disk.
    instance.query(f"CREATE USER {seed} IDENTIFIED WITH no_password")
    # Flush the background writer so the `.list` files are in their steady (post-rebuild)
    # state and no longer mention any pending entity.
    instance.query("SYSTEM RELOAD USERS")

    # Direction 1: marker MISSING -> stale .list is trusted, .sql is not rescanned.
    _forge_stale_entity(seed, lost)
    instance.restart_clickhouse()
    assert instance.query(f"SELECT count() FROM system.users WHERE name = '{lost}'").strip() == "0", (
        "entity became reachable without the marker - the marker is not actually the "
        "signal that gates the .sql rescan, so this test would not prove its durability"
    )

    # Direction 2: same on-disk .sql, but with the marker present -> rescan recovers it.
    instance.exec_in_container(["bash", "-c", f"touch {ACCESS_DIR}/need_rebuild_lists.mark"], user="root")
    instance.restart_clickhouse()
    assert instance.query(f"SELECT count() FROM system.users WHERE name = '{lost}'").strip() == "1", (
        "entity with a durable .sql but stale .list was not recovered on restart - "
        "the need_rebuild_lists.mark marker did not trigger a rescan"
    )

    # `lost` is now reachable, so DROP removes its `.sql` cleanly (avoids an orphan file that
    # a later rebuild would resurrect).
    instance.query(f"DROP USER IF EXISTS {lost}")
    instance.query(f"DROP USER IF EXISTS {seed}")


def test_rebuild_marker_is_fsynced():
    """
    The `need_rebuild_lists.mark` marker written by scheduleWriteLists() must itself be
    durable (fsync the file and its parent directory), gated on `fsync_metadata`. It is
    (re)written on every DDL that schedules a list rewrite, so a CREATE USER syncs the marker
    (file + directory) on top of the `<uuid>.sql` write -> at least 2 file and 2 directory
    syncs. Without the marker fsync only the entity file is synced (1 each), so this asserts
    the marker sync specifically rather than just the entity write.
    """
    user = "u_marker_fsync"
    instance.query(f"DROP USER IF EXISTS {user}")

    file_sync, dir_sync = _run(f"CREATE USER {user} IDENTIFIED WITH no_password", 1)
    assert file_sync >= 2 and dir_sync >= 2, (
        f"marker not fsynced alongside entity file: FileSync={file_sync}, DirectorySync={dir_sync}"
    )

    # fsync_metadata = 0 -> neither the entity file nor the marker is synced.
    instance.query(f"DROP USER IF EXISTS {user}")
    file_sync, dir_sync = _run(f"CREATE USER {user} IDENTIFIED WITH no_password", 0)
    assert file_sync == 0 and dir_sync == 0, (
        f"fsync_metadata=0 not honored on marker: FileSync={file_sync}, DirectorySync={dir_sync}"
    )

    instance.query(f"DROP USER IF EXISTS {user}")

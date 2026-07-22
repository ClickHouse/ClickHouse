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

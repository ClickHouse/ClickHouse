import uuid

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# The node stores UDFs, workload entities and named collections on local disk
# (not the <memory/>-style overrides used by stateless tests), so their disk
# storages are exercised.
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

NAMED_COLLECTIONS_DIR = "/var/lib/clickhouse/named_collections"
USER_DEFINED_DIR = "/var/lib/clickhouse/user_defined"
# Configured two levels below an existing directory, so creating the store exercises a
# multi-component path rather than a single missing directory.
WORKLOAD_ROOT = "/var/lib/clickhouse/workload"
WORKLOAD_DIR = f"{WORKLOAD_ROOT}/entities/local"
SQL_OBJECT_DIRS = [NAMED_COLLECTIONS_DIR, USER_DEFINED_DIR, WORKLOAD_DIR]
# Stands in for the credential a named collection holds, in a value no other part of the
# suite writes, so finding it in the log can only come from the file that failed to parse.
CORRUPT_FILE_SECRET = "sentinel_secret_2b7f4c1e"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _profile_events(query_id):
    """(FileSync, DirectorySync) ProfileEvents for the given query."""
    node.query("SYSTEM FLUSH LOGS query_log")
    row = node.query(
        "SELECT ProfileEvents['FileSync'], ProfileEvents['DirectorySync'] "
        "FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip()
    file_sync, dir_sync = (int(x) for x in row.split("\t"))
    return file_sync, dir_sync


def _run(query, fsync_metadata):
    query_id = str(uuid.uuid4())
    node.query(query, query_id=query_id, settings={"fsync_metadata": fsync_metadata})
    return _profile_events(query_id)


def test_sql_object_writes_are_fsynced():
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/111381

    The disk storages for UDFs, named collections and workload entities used to
    commit a create by renaming a temp file, and a drop by unlinking, without ever
    fsyncing the parent directory. An fsync of the file content alone does not
    persist the directory entry, so an acknowledged CREATE could vanish and an
    acknowledged DROP could revert after power loss.

    We assert the durability path is taken (rather than simulating power loss) by
    reading the FileSync / DirectorySync ProfileEvents of each DDL query: the write
    runs synchronously on the query thread, so its fsync is attributed to the query.

    With fsync_metadata = 1 every create renames a `.sql` (file + directory fsync)
    and every drop unlinks it (directory fsync, nothing to content-sync). Named
    collections honor the global fsync_metadata (their storage uses the global
    context, matching the pre-existing content fsync), so they are checked with the
    default setting value of 1 only.
    """
    # UDF and workload storages honor the per-query fsync_metadata setting. A first
    # CREATE per storage may also create the storage directory (which itself syncs a
    # directory), so a second CREATE (with the directory already present) isolates the
    # commit-rename fsync from the directory-creation fsync.
    # (a second root workload is not allowed, so the second workload is created under the
    # first with an `IN` clause.)
    for create1, create2, drop2 in [
        ("CREATE FUNCTION f_fsync1 AS (x) -> x + 1",
         "CREATE FUNCTION f_fsync2 AS (x) -> x + 2", "DROP FUNCTION f_fsync2"),
        ("CREATE WORKLOAD wl_fsync1", "CREATE WORKLOAD wl_fsync2 IN wl_fsync1", "DROP WORKLOAD wl_fsync2"),
    ]:
        # First create: directory may be created here.
        _run(create1, 1)

        # Second create: the directory already exists, so a directory fsync here is the
        # commit rename being synced, not the mkdir.
        file_sync, dir_sync = _run(create2, 1)
        assert file_sync >= 1 and dir_sync >= 1, f"{create2}: {file_sync}, {dir_sync}"

        file_sync, dir_sync = _run(drop2, 1)
        assert dir_sync >= 1, f"{drop2} directory not synced: {file_sync}, {dir_sync}"

        # With fsync_metadata = 0 the setting is honored: no forced sync (directory
        # already exists, so no mkdir sync either).
        file_sync, dir_sync = _run(create2, 0)
        assert file_sync == 0 and dir_sync == 0, (
            f"fsync_metadata=0 not honored on {create2}: {file_sync}, {dir_sync}"
        )
        file_sync, dir_sync = _run(drop2, 0)
        assert file_sync == 0 and dir_sync == 0, (
            f"fsync_metadata=0 not honored on {drop2}: {file_sync}, {dir_sync}"
        )

    # Named collections: their local storage syncs based on the (default = 1)
    # fsync_metadata of the global context. Use a second create (directory already
    # present) to isolate the commit-rename fsync.
    _run("CREATE NAMED COLLECTION nc_fsync1 AS a = 1", 1)

    file_sync, dir_sync = _run("CREATE NAMED COLLECTION nc_fsync2 AS a = 1, b = 2", 1)
    assert file_sync >= 1 and dir_sync >= 1, f"CREATE NAMED COLLECTION: {file_sync}, {dir_sync}"

    file_sync, dir_sync = _run("DROP NAMED COLLECTION nc_fsync2", 1)
    assert dir_sync >= 1, f"DROP NAMED COLLECTION directory not synced: {file_sync}, {dir_sync}"

    node.query("DROP NAMED COLLECTION nc_fsync1")


def test_corrupt_named_collection_does_not_brick_startup():
    """
    A single corrupt/torn local named-collection `.sql` file (e.g. a zero-byte file
    left by a power loss with fsync_metadata=0) must not fail server startup. Before
    the fix `NamedCollectionsMetadataStorage::getAll` only caught Keeper ZNONODE, so
    a local parse error propagated out of `NamedCollectionFactory::loadIfNot` at boot
    and bricked the server. It must now skip and log the bad file, like the UDF and
    workload disk storages do.
    """
    node.query("CREATE NAMED COLLECTION nc_good AS a = 1")

    node.stop_clickhouse()

    # A torn (zero-byte) file and a syntactically invalid file.
    node.exec_in_container(
        ["bash", "-c", f"truncate -s 0 {NAMED_COLLECTIONS_DIR}/nc_torn.sql"]
    )
    # A write torn inside a credential, which is what a power loss produces here. The parse
    # error quotes the text at the position it failed on, so that position has to be inside
    # the secret for the log assertion below to mean anything: an unterminated string literal
    # puts it there.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo \"CREATE NAMED COLLECTION nc_garbage AS secret_access_key = '{CORRUPT_FILE_SECRET}\""
            f" > {NAMED_COLLECTIONS_DIR}/nc_garbage.sql",
        ]
    )

    # Must start despite the corrupt files.
    node.start_clickhouse()

    # The good collection survives; the corrupt ones are skipped, not loaded.
    collections = node.query(
        "SELECT name FROM system.named_collections ORDER BY name"
    ).split()
    assert "nc_good" in collections
    assert "nc_torn" not in collections
    assert "nc_garbage" not in collections

    # Skipping silently would leave no way to find out a collection is gone, so the skip is
    # logged and that is part of the contract.
    assert node.contains_in_log("Skipping named collection 'nc_garbage'")
    assert node.contains_in_log("Skipping named collection 'nc_torn'")

    # The log goes to a file read by anyone who can read logs, so the skip reports the error
    # code and never the text that failed to parse.
    assert not node.contains_in_log(CORRUPT_FILE_SECRET), (
        "the credential in the unparseable metadata file must not reach the log"
    )

    # Cleanup so the module teardown / reruns start clean.
    node.exec_in_container(
        ["bash", "-c", f"rm -f {NAMED_COLLECTIONS_DIR}/nc_torn.sql {NAMED_COLLECTIONS_DIR}/nc_garbage.sql"]
    )
    node.query("DROP NAMED COLLECTION nc_good")


# (create, drop, committed file) per on-disk SQL-object store. A shared directory-sync
# helper can stop syncing one store without the others noticing, so each is checked
# separately. A resource stands in for the workload store: it is kept there too, and unlike
# a workload it needs no parent, so this does not depend on what other tests leave behind.
# The file is the exact committed path, not a prefix: matching loosely would also accept the
# `.sql.tmp` file that a create leaves behind when it fails before its rename.
FAILING_SYNC_CASES = [
    (
        "CREATE FUNCTION {n} AS (x) -> x + 1",
        "DROP FUNCTION {n}",
        f"{USER_DEFINED_DIR}/function_{{n}}.sql",
    ),
    (
        "CREATE RESOURCE {n} (WRITE DISK {n}_disk)",
        "DROP RESOURCE {n}",
        f"{WORKLOAD_DIR}/resource_{{n}}.sql",
    ),
    (
        "CREATE NAMED COLLECTION {n} AS a = 1",
        "DROP NAMED COLLECTION {n}",
        f"{NAMED_COLLECTIONS_DIR}/{{n}}.sql",
    ),
]


def _exists(path):
    return node.exec_in_container(
        ["bash", "-c", f"test -e {path} && echo yes || echo no"]
    ).strip() == "yes"


def test_failed_directory_sync_fails_the_ddl():
    """
    A directory fsync that fails must fail the DDL. Reporting success for a change that
    could not be made durable is what the fsync is there to prevent, so the sync is done
    explicitly and its failure propagates instead of being logged and dropped.

    `directory_sync_fail` makes the directory sync of every on-disk SQL-object store fail.
    Each store is first exercised with the failpoint disabled: that both proves the DDL
    itself is valid (so a raise below is caused by the failed sync, not by the statement)
    and creates the store directory, so the failure under test is the commit sync rather
    than a directory-creation sync.

    The file state after each failure also pins the ordering: the sync has to happen after
    the rename or unlink it makes durable, so the `.sql` file is already in place when a
    failing create raises and already gone when a failing drop raises. A sync moved before
    the mutation would raise just the same, so the exception alone does not show this.
    """
    try:
        for i, (create, drop, committed_file) in enumerate(FAILING_SYNC_CASES):
            control = f"ds_control_{i}"
            node.query(create.format(n=control), settings={"fsync_metadata": 1})

            node.query("SYSTEM ENABLE FAILPOINT directory_sync_fail")

            failing = f"ds_failing_{i}"
            with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
                node.query(create.format(n=failing), settings={"fsync_metadata": 1})
            assert _exists(committed_file.format(n=failing)), (
                f"{failing}: the rename must already have committed when the sync failed"
            )

            with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
                node.query(drop.format(n=control), settings={"fsync_metadata": 1})
            assert not _exists(committed_file.format(n=control)), (
                f"{control}: the unlink must already have committed when the sync failed"
            )

            node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")
    finally:
        # A DDL that failed after its rename or unlink leaves the store and the server's
        # in-memory view disagreeing, which is the honest outcome of refusing to
        # acknowledge it. Drop the files and restart so the module ends consistent.
        node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")
        for directory in SQL_OBJECT_DIRS:
            node.exec_in_container(["bash", "-c", f"rm -f {directory}/*ds_control_* {directory}/*ds_failing_*"])
        node.restart_clickhouse()


def test_failed_store_directory_creation_is_rolled_back():
    """
    A store directory is created lazily on first use, and its own entry in its parent has to
    be persisted as well. If that fails the directory must not be left behind: the next
    attempt would find it present, skip creating it, so never persist its entry, and the
    CREATE it then acknowledges could still be lost together with the directory.

    The workload store is configured two levels down, so this covers a multi-component path.
    """
    node.query("DROP RESOURCE IF EXISTS dc_probe")
    node.exec_in_container(["bash", "-c", f"rm -rf {WORKLOAD_ROOT}"])

    node.query("SYSTEM ENABLE FAILPOINT directory_sync_fail")
    try:
        with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
            node.query(
                "CREATE RESOURCE dc_probe (WRITE DISK dc_probe_disk)",
                settings={"fsync_metadata": 1},
            )
        left_behind = node.exec_in_container(
            ["bash", "-c", f"test -e {WORKLOAD_ROOT} && echo yes || echo no"]
        ).strip()
        assert left_behind == "no", (
            "a directory whose entry could not be persisted was kept, so a retry would "
            "treat it as already created and never persist it"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")

    # The retry starts over, so it creates the directories again and persists every one of
    # them: three directory syncs for the three components, and a fourth for the commit
    # rename. Counting them is what shows each nested parent was synced, not just the last.
    _, dir_sync = _run("CREATE RESOURCE dc_probe (WRITE DISK dc_probe_disk)", 1)
    assert dir_sync >= 4, f"only {dir_sync} directory syncs, so a nested parent was skipped"
    assert node.query("SELECT count() FROM system.resources WHERE name = 'dc_probe'").strip() == "1"

    node.query("DROP RESOURCE dc_probe")
    node.restart_clickhouse()

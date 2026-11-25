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
# Its UDF and workload stores are the data path itself, so a create there persists no directory
# before its commit rename, and `directory_sync_fail` fails the sync after the rename.
root_node = cluster.add_instance(
    "root_node",
    main_configs=["configs/stores_at_data_root.xml"],
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


def _secret_in_error_statistics():
    return int(
        node.query(
            f"SELECT count() FROM system.errors WHERE last_error_message LIKE '%{CORRUPT_FILE_SECRET}%'"
        )
    )


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

    With fsync_metadata = 1 every create persists the store directory's own entry and
    renames a `.sql` (file fsync + two directory fsyncs), and every drop unlinks it
    (directory fsync, nothing to content-sync). Named
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

        # Second create: the store directory already exists, so the two directory syncs are
        # its own entry and the commit rename. Either one alone leaves the other unproven.
        file_sync, dir_sync = _run(create2, 1)
        assert file_sync >= 1 and dir_sync >= 2, f"{create2}: {file_sync}, {dir_sync}"

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
    # fsync_metadata of the global context. Use a second create, with the store directory
    # already present, so the count is the store directory's entry plus the commit rename.
    _run("CREATE NAMED COLLECTION nc_fsync1 AS a = 1", 1)

    file_sync, dir_sync = _run("CREATE NAMED COLLECTION nc_fsync2 AS a = 1, b = 2", 1)
    assert file_sync >= 1 and dir_sync >= 2, f"CREATE NAMED COLLECTION: {file_sync}, {dir_sync}"

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

    # A write torn inside a credential, which is what a power loss produces here. The parse
    # error quotes the text at the position it failed on, so that position has to be inside
    # the secret for the assertions below to mean anything: an unterminated string literal
    # puts it there. It is the only corrupt file of this start, so the error it raises is the
    # newest SYNTAX_ERROR in `system.errors`, which keeps the message of the last error only.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo \"CREATE NAMED COLLECTION nc_garbage AS secret_access_key = '{CORRUPT_FILE_SECRET}\""
            f" > {NAMED_COLLECTIONS_DIR}/nc_garbage.sql",
        ]
    )

    # Must start despite the corrupt file.
    node.start_clickhouse()

    # The good collection survives; the corrupt one is skipped, not loaded.
    collections = node.query(
        "SELECT name FROM system.named_collections ORDER BY name"
    ).split()
    assert "nc_good" in collections
    assert "nc_garbage" not in collections

    # Skipping silently would leave no way to find out a collection is gone, so the skip is
    # logged and that is part of the contract.
    assert node.contains_in_log("Skipping named collection 'nc_garbage'")

    # The log goes to a file read by anyone who can read logs, so the skip reports the error
    # code and never the text that failed to parse.
    assert not node.contains_in_log(CORRUPT_FILE_SECRET), (
        "the credential in the unparseable metadata file must not reach the log"
    )

    # Error statistics are readable without the right to see a collection or its secrets, so the
    # parse error is recorded there without the text or the name.
    # `system.errors` keeps the newest message only, so the count is what shows that the parser's own
    # exception, which quotes the secret, was not recorded before the one replacing it.
    assert node.query("SELECT sum(value) FROM system.errors WHERE name = 'SYNTAX_ERROR'").strip() == "1", (
        "the parse error of the corrupt file was not recorded exactly once"
    )
    assert _secret_in_error_statistics() == 0, (
        "the credential in the unparseable metadata file must not reach system.errors"
    )
    assert int(node.query("SELECT count() FROM system.errors WHERE last_error_message LIKE '%nc_garbage%'")) == 0, (
        "the name of the collection must not reach system.errors"
    )

    # A skipped collection is still in the storage, so it is not created again, but it can be
    # dropped and then created again with SQL, without access to the files.
    with pytest.raises(QueryRuntimeException, match="NAMED_COLLECTION_ALREADY_EXISTS"):
        node.query("CREATE NAMED COLLECTION nc_garbage AS a = 1")
    node.query("DROP NAMED COLLECTION nc_garbage")
    assert not _exists(f"{NAMED_COLLECTIONS_DIR}/nc_garbage.sql")
    node.query("CREATE NAMED COLLECTION nc_garbage AS a = 1")
    assert node.query("SELECT count() FROM system.named_collections WHERE name = 'nc_garbage'").strip() == "1"
    # Both statements above read the corrupt file again.
    assert _secret_in_error_statistics() == 0, (
        "the credential in the unparseable metadata file must not reach system.errors"
    )

    # A torn write can also leave a zero-byte file.
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"truncate -s 0 {NAMED_COLLECTIONS_DIR}/nc_torn.sql"]
    )
    node.start_clickhouse()

    collections = node.query(
        "SELECT name FROM system.named_collections ORDER BY name"
    ).split()
    assert "nc_good" in collections
    assert "nc_torn" not in collections
    assert node.contains_in_log("Skipping named collection 'nc_torn'")

    # A drop of the skipped entry whose sync fails has already unlinked its file, and the entry is
    # not loaded, so nothing tells a retry that it completes that drop: it persists the directory
    # anyway.
    node.query("SYSTEM ENABLE FAILPOINT directory_sync_fail")
    try:
        with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
            node.query("DROP NAMED COLLECTION nc_torn")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")
    assert not _exists(f"{NAMED_COLLECTIONS_DIR}/nc_torn.sql")
    _, dir_sync = _run("DROP NAMED COLLECTION IF EXISTS nc_torn", 1)
    assert dir_sync >= 1, "the retried drop of the skipped entry did not persist its unlink"

    node.query("DROP NAMED COLLECTION nc_garbage")
    node.query("DROP NAMED COLLECTION nc_good")

    # Without a collection directory there is nothing to persist, and such a drop still succeeds.
    node.exec_in_container(["bash", "-c", f"rm -rf {NAMED_COLLECTIONS_DIR}"])
    node.query("DROP NAMED COLLECTION IF EXISTS nc_absent")


# (create, drop, retried drop, count of loaded objects, committed file) per on-disk SQL-object
# store. A shared directory-sync helper can stop syncing one store without the others noticing,
# so each is checked separately. A resource stands in for the workload store: it is kept there
# too, and unlike a workload it needs no parent, so this does not depend on what other tests
# leave behind. The file is the exact committed path, not a prefix: matching loosely would also
# accept the `.sql.tmp` file that a create leaves behind when it fails before its rename.
FAILING_SYNC_CASES = [
    (
        "CREATE FUNCTION {n} AS (x) -> x + 1",
        "DROP FUNCTION {n}",
        "DROP FUNCTION IF EXISTS {n}",
        "SELECT count() FROM system.functions WHERE name = '{n}'",
        f"{USER_DEFINED_DIR}/function_{{n}}.sql",
    ),
    (
        "CREATE RESOURCE {n} (WRITE DISK {n}_disk)",
        "DROP RESOURCE {n}",
        "DROP RESOURCE IF EXISTS {n}",
        "SELECT count() FROM system.resources WHERE name = '{n}'",
        f"{WORKLOAD_DIR}/resource_{{n}}.sql",
    ),
    (
        "CREATE NAMED COLLECTION {n} AS a = 1",
        "DROP NAMED COLLECTION {n}",
        "DROP NAMED COLLECTION IF EXISTS {n}",
        "SELECT count() FROM system.named_collections WHERE name = '{n}'",
        f"{NAMED_COLLECTIONS_DIR}/{{n}}.sql",
    ),
]


def _exists(path, instance=node):
    return instance.exec_in_container(
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
    and creates the store directory, so no directory has to be created below.

    The file state after each failure also pins the ordering. A create persists the store
    directory's entry before it writes the object, and every directory sync fails here, so it
    stops there and leaves neither the object nor its temporary file behind. A drop's sync is
    the one that makes its unlink durable, so it comes after it and the `.sql` file is already
    gone when the drop raises. A sync moved before the unlink would raise just the same, so the
    exception alone does not show this.

    The failed drop leaves the object loaded while its file is gone, so a retried drop has to
    complete it: persist the unlink and unload the object, rather than find no file and report
    success for an object that is still there.
    """
    try:
        for i, (create, drop, retried_drop, count_loaded, committed_file) in enumerate(FAILING_SYNC_CASES):
            control = f"ds_control_{i}"
            node.query(create.format(n=control), settings={"fsync_metadata": 1})

            node.query("SYSTEM ENABLE FAILPOINT directory_sync_fail")

            failing = f"ds_failing_{i}"
            with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
                node.query(create.format(n=failing), settings={"fsync_metadata": 1})
            assert not _exists(committed_file.format(n=failing)), (
                f"{failing}: nothing may be committed once the store directory could not be persisted"
            )
            assert not _exists(committed_file.format(n=failing) + ".tmp"), (
                f"{failing}: a temporary file was left behind, so a later attempt would fail on it"
            )

            with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
                node.query(drop.format(n=control), settings={"fsync_metadata": 1})
            assert not _exists(committed_file.format(n=control)), (
                f"{control}: the unlink must already have committed when the sync failed"
            )

            node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")

            _, dir_sync = _run(retried_drop.format(n=control), 1)
            assert dir_sync >= 1, f"{control}: the retried drop did not persist the unlink"
            assert node.query(count_loaded.format(n=control)).strip() == "0", (
                f"{control}: the retried drop reported success, but the object is still loaded"
            )
    finally:
        # Restart so the module ends consistent even if an assertion above failed between a
        # failed DDL and its retry.
        node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")
        for directory in SQL_OBJECT_DIRS:
            node.exec_in_container(["bash", "-c", f"rm -f {directory}/*ds_control_* {directory}/*ds_failing_*"])
        node.restart_clickhouse()


# (create, drop if it exists, count of loaded objects, committed file) per store of `root_node`.
ROOT_STORE_CASES = [
    (
        "CREATE FUNCTION {n} AS (x) -> x + 1",
        "DROP FUNCTION IF EXISTS {n}",
        "SELECT count() FROM system.functions WHERE name = '{n}'",
        "/var/lib/clickhouse/function_{n}.sql",
    ),
    (
        "CREATE RESOURCE {n} (WRITE DISK {n}_disk)",
        "DROP RESOURCE IF EXISTS {n}",
        "SELECT count() FROM system.resources WHERE name = '{n}'",
        "/var/lib/clickhouse/resource_{n}.sql",
    ),
]


def _root_directory_syncs(query_id, query_type):
    root_node.query("SYSTEM FLUSH LOGS query_log")
    return int(
        root_node.query(
            "SELECT ProfileEvents['DirectorySync'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = '{query_type}'"
        ).strip()
    )


def test_create_whose_commit_sync_fails_leaves_nothing_behind():
    """
    A create whose directory sync fails after its rename has already committed the file. It is
    reported as failed and its object is not loaded, so the file must not stay: otherwise a retried
    create fails on it or reports success for an object that is not there, a drop finds no object to
    remove, and the next start loads an object no statement reported as created.

    The stores of `root_node` sync no directory before the commit rename, which the control create
    checks, so a failed create that synced one directory failed after its rename.
    """
    try:
        for i, (create, drop_if_exists, count_loaded, committed_file) in enumerate(ROOT_STORE_CASES):
            query_id = str(uuid.uuid4())
            root_node.query(create.format(n=f"cs_control_{i}"), query_id=query_id, settings={"fsync_metadata": 1})
            assert _root_directory_syncs(query_id, "QueryFinish") == 1, "a create syncs more than its commit here"

            root_node.query("SYSTEM ENABLE FAILPOINT directory_sync_fail")
            for name in (f"cs_retried_{i}", f"cs_dropped_{i}"):
                query_id = str(uuid.uuid4())
                with pytest.raises(QueryRuntimeException, match="Cannot fsync directory"):
                    root_node.query(create.format(n=name), query_id=query_id, settings={"fsync_metadata": 1})
                assert _root_directory_syncs(query_id, "ExceptionBeforeStart") == 1, (
                    f"{name}: the create did not fail at the sync after its rename"
                )
                file = committed_file.format(n=name)
                assert not _exists(file, root_node), f"{name}: the failed create left its file"
                assert not _exists(file + ".tmp", root_node), f"{name}: a temporary file was left"
                assert root_node.query(count_loaded.format(n=name)).strip() == "0", f"{name}: loaded, but its create failed"
            root_node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")

            root_node.query(create.format(n=f"cs_retried_{i}"), settings={"fsync_metadata": 1})
            assert root_node.query(count_loaded.format(n=f"cs_retried_{i}")).strip() == "1", (
                f"cs_retried_{i}: the retried create did not load it"
            )
            root_node.query(drop_if_exists.format(n=f"cs_dropped_{i}"), settings={"fsync_metadata": 1})

        root_node.restart_clickhouse()
        for i, (_, _, count_loaded, _) in enumerate(ROOT_STORE_CASES):
            assert root_node.query(count_loaded.format(n=f"cs_dropped_{i}")).strip() == "0", (
                f"cs_dropped_{i}: loaded at startup, although its create failed and it was then dropped"
            )
            assert root_node.query(count_loaded.format(n=f"cs_retried_{i}")).strip() == "1"
    finally:
        root_node.query("SYSTEM DISABLE FAILPOINT directory_sync_fail")


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


def test_store_directory_of_an_unsynced_write_is_persisted_later():
    """
    A store directory created while fsync_metadata was disabled has an entry that was never
    persisted, and so has every directory that write created above it. A later enabled write
    finds them all present, so it used to sync only the file it renames into the store, and the
    object it acknowledged could still be lost together with any of the directories holding it.
    An enabled write now persists each of their entries, whoever created them.

    The store is configured two levels down, so the count is what distinguishes persisting the
    whole path from persisting only the store directory itself.
    """
    node.query("DROP RESOURCE IF EXISTS pd_disabled")
    node.query("DROP RESOURCE IF EXISTS pd_enabled")
    node.exec_in_container(["bash", "-c", f"rm -rf {WORKLOAD_ROOT}"])

    # Creates the store directory and persists nothing, which is what the setting asks for.
    _, dir_sync = _run("CREATE RESOURCE pd_disabled (WRITE DISK pd_disabled_disk)", 0)
    assert dir_sync == 0, f"fsync_metadata=0 synced {dir_sync} directories"

    # Nothing left to create, so the four syncs are the entries of the three directories the
    # disabled write made and the rename committed inside the deepest one. Two of them (only the
    # store directory's entry and the rename) leave the directories above it unpersisted.
    _, dir_sync = _run("CREATE RESOURCE pd_enabled (WRITE DISK pd_enabled_disk)", 1)
    assert dir_sync >= 4, (
        f"only {dir_sync} directory syncs, so the path left unpersisted by the disabled write "
        "was not persisted here either"
    )

    node.query("DROP RESOURCE pd_enabled")
    node.query("DROP RESOURCE pd_disabled")

    # The same holds when only part of the path is there, which is what an operator's `mkdir -p`
    # of the parents leaves behind: the store directory is created here, but the entries of the
    # directories above it are no more persisted than in the case above.
    # The parents are named directly, since `mkdir -p {WORKLOAD_DIR}/..` has to create the leaf in
    # order to resolve `..` through it, which would leave the whole path behind: the case above.
    node.exec_in_container(["bash", "-c", f"rm -rf {WORKLOAD_ROOT}"])
    node.exec_in_container(["bash", "-c", f"mkdir -p {WORKLOAD_ROOT}/entities"])
    assert not _exists(WORKLOAD_DIR), f"{WORKLOAD_DIR} exists, so this is not the parents-only case"
    _, dir_sync = _run("CREATE RESOURCE pd_partial (WRITE DISK pd_partial_disk)", 1)
    assert dir_sync >= 4, (
        f"only {dir_sync} directory syncs, so an ancestor this call found rather than created "
        "was skipped"
    )

    node.query("DROP RESOURCE pd_partial")
    node.restart_clickhouse()

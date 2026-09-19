
import logging
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/remote_servers.xml",
    "configs/backups_disk.xml",
    "configs/keeper_map_path_prefix.xml",
    "configs/query_thread_log.xml",  # The common integration config removes query_thread_log; test_on_cluster needs it.
]

user_configs = [
    "configs/zookeeper_retries.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,
)


node3 = cluster.add_instance(
    "node3",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node3", "shard": "shard2"},
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def new_backup_path(prefix):
    """A destination unique to this call.

    /backups is a shared mount that survives between the cases of a module, and a BACKUP to a
    destination that already holds one fails outright, so a fixed name only works on a first run.
    The suffix is a uuid rather than a counter because the path is also matched against
    `system.backup_log.name`: a counter makes one run's name a prefix of a later one's, and the
    match has to stay unambiguous.
    """
    return f"{prefix}_{uuid.uuid4().hex}"


def get_backup_data_file_syncs(node, backup_path, initiator_query_id, diagnostics):
    """Sum the data-file FileSync of this node's internal backup operations.

    Only the data-file half is read here; the initiator's `.backup` manifest fsync is read from
    `system.backups` by the caller. The halves come from different surfaces because only one surface
    is ordered for each of them:

    - The data-file fsyncs run in `writeFile` on `BackupWorker` pool threads, which detach inside
      `writeBackupEntries`' `waitForAllToFinishAndRethrowFirstError`, so their rows are written before
      `setStatus(BACKUP_CREATED)` writes the `system.backup_log` row the query ids come from. Waiting
      for the latter therefore also orders the former.
    - The manifest fsync runs in `finalizeWriting` on the `BackupAsync` thread, which detaches only
      after the callback holding the last strong reference to the query context is released, and
      `finalizePerformanceCounters` needs that context to still be alive to write the row at all. A
      synchronous BACKUP returns before either happens, so that row is not something to wait for.

    Scoped per operation rather than read from `system.events`, whose counters are process-wide:
    `fsync_metadata` defaults to true, so an unrelated `CREATE TABLE` - in particular the lazy
    creation of a system log table on its first flush - fsyncs its metadata file and would inflate a
    process-wide delta, failing an exact assertion on an unrelated change.

    `system.backup_log` is used rather than `system.backups` because the latter hides the internal
    operations of an ON CLUSTER backup (`getAllInfos` skips them), and those are exactly the ones that
    write the data files. Every host runs one, including the host the query was issued on, which also
    runs the initiator operation - so that node has two rows, and taking whichever landed first would
    silently drop the other operation's fsyncs.
    """

    # The internal operation is the one whose id `BackupStarter` suffixed with `-internal-<host>`,
    # which is what distinguishes it from the initiator on the node that shares its `name`.
    def read_backup_log_operations():
        node.query("SYSTEM FLUSH LOGS backup_log")
        # `name` holds the destination re-formatted from the AST, so match on the unique path only,
        # with `position` rather than LIKE: the path is a plain string, not a pattern.
        rows = node.query(
            f"SELECT query_id, position(id, '-internal-') > 0 FROM system.backup_log"
            f" WHERE position(name, '{backup_path}') > 0 AND status = 'BACKUP_CREATED'"
            f" GROUP BY 1, 2 ORDER BY 1 FORMAT TSV"
        ).splitlines()
        return [
            (query_id, internal == "1")
            for query_id, internal in (row.split("\t") for row in rows)
        ]

    # Every host runs one internal operation; the host the query was issued on also runs the
    # initiator. Ask for exactly that many rather than "at least one", so an operation still in
    # flight cannot be mistaken for one that has nothing to contribute.
    is_initiator_node = (
        node.query(
            f"SELECT count() FROM system.backups WHERE query_id = '{initiator_query_id}'"
        ).strip()
        != "0"
    )
    expected_operations = 2 if is_initiator_node else 1

    try:
        operations = wait_condition(
            read_backup_log_operations,
            lambda rows: len(rows) == expected_operations
            and sum(1 for _, internal in rows if internal) == 1,
            max_attempts=60,
            delay=0.5,
        )
    except Exception as exception:
        raise AssertionError(
            f"{node.name}: expected {expected_operations} BACKUP_CREATED operation(s)"
            f" for {backup_path}, exactly one of them internal"
        ) from exception

    diagnostics[f"{node.name}.operations"] = operations

    internal_query_ids = [query_id for query_id, internal in operations if internal]

    # Wait per query id, never over the union: rows of one operation must not satisfy the wait for
    # another's. A host is assigned at least one entry, so a row always appears, which keeps "not
    # logged yet" distinguishable from a legitimate sum of zero (every entry deduplicated away).
    for query_id in internal_query_ids:

        def count_worker_rows(query_id=query_id):
            node.query("SYSTEM FLUSH LOGS query_thread_log")
            return int(
                node.query(
                    f"SELECT count() FROM system.query_thread_log"
                    f" WHERE query_id = '{query_id}' AND thread_name = 'BackupWorker'"
                ).strip()
            )

        try:
            worker_rows = wait_condition(
                count_worker_rows, lambda n: n > 0, max_attempts=60, delay=0.5
            )
        except Exception as exception:
            raise AssertionError(
                f"{node.name}: no BackupWorker rows for operation {query_id}"
            ) from exception
        diagnostics[f"{node.name}.{query_id}.worker_rows"] = worker_rows

    id_list = ", ".join(f"'{query_id}'" for query_id in internal_query_ids)
    data_file_syncs = int(
        node.query(
            f"SELECT sum(ProfileEvents['FileSync']) FROM system.query_thread_log"
            f" WHERE query_id IN ({id_list}) AND thread_name = 'BackupWorker'"
        ).strip()
    )
    diagnostics[f"{node.name}.data_file_syncs"] = data_file_syncs
    return data_file_syncs


def get_backup_manifest_file_sync(node, initiator_query_id, diagnostics):
    """Read the initiator operation's FileSync from `system.backups`.

    That row is ordered by construction: `setStatus` fills `info.profile_counters` from the process
    list element and only then notifies the waiter, both under one lock, so the value is in place
    before the client's BACKUP can return. And it is a thread-group-wide snapshot, not a per-thread
    row: every thread's counters parent to the group's and `Counters::increment` walks that chain, so
    the manifest fsync of the `BackupAsync` thread is included even though its own
    `query_thread_log` row may never be written. The snapshot stays scoped to this one operation, so
    it cannot absorb ambient fsyncs from elsewhere in the process.
    """
    attempts = 0

    def read_initiator_row():
        nonlocal attempts
        attempts += 1
        return node.query(
            f"SELECT num_entries, num_files, ProfileEvents['FileSync'] FROM system.backups"
            f" WHERE query_id = '{initiator_query_id}' AND status = 'BACKUP_CREATED'"
            f" FORMAT TSV"
        ).strip()

    row = wait_condition(
        read_initiator_row, lambda s: s != "", max_attempts=60, delay=0.5
    )
    num_entries, num_files, manifest_file_syncs = (
        int(value) for value in row.split("\t")
    )
    diagnostics["initiator.attempts"] = attempts
    diagnostics["initiator.num_entries"] = num_entries
    # num_files is not asserted on; it is reported because it is what explains a deduplicating arm
    # having fewer entries than files over the same input.
    diagnostics["initiator.num_files"] = num_files
    diagnostics["initiator.manifest_file_syncs"] = manifest_file_syncs
    return num_entries, manifest_file_syncs


@pytest.mark.parametrize("deduplicate_files", [0, 1])
def test_on_cluster(deduplicate_files):
    database_name = f"keeper_backup{deduplicate_files}"
    node1.query_with_retry(f"CREATE DATABASE {database_name} ON CLUSTER cluster")
    node1.query_with_retry(
        f"CREATE TABLE {database_name}.keeper1 ON CLUSTER cluster (key UInt64, value String) Engine=KeeperMap('/{database_name}/test_on_cluster1') PRIMARY KEY key"
    )
    node1.query_with_retry(
        f"CREATE TABLE {database_name}.keeper2 ON CLUSTER cluster (key UInt64, value String) Engine=KeeperMap('/{database_name}/test_on_cluster1') PRIMARY KEY key"
    )
    node1.query_with_retry(
        f"CREATE TABLE {database_name}.keeper3 ON CLUSTER cluster (key UInt64, value String) Engine=KeeperMap('/{database_name}/test_on_cluster2') PRIMARY KEY key"
    )
    node1.query_with_retry(
        f"INSERT INTO {database_name}.keeper2 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 5"
    )
    node1.query_with_retry(
        f"INSERT INTO {database_name}.keeper3 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 5"
    )

    expected_result = "".join(f"{i}\ttest{i}\n" for i in range(5))

    def verify_data():
        for node in [node1, node2, node3]:
            for i in range(1, 4):
                result = node.query_with_retry(
                    f"SELECT key, value FROM {database_name}.keeper{i} ORDER BY key FORMAT TSV"
                )
                assert result == expected_result

    verify_data()

    backup_path = new_backup_path(f"test_on_cluster_fsync{deduplicate_files}")
    backup_name = f"Disk('backups', '{backup_path}')"
    # keeper1 and keeper2 share one zk root, and only one table may own the data of a root, so the
    # other one is collected as a BackupEntryReference - the only producer of one in the tree. With
    # deduplicate_files = 0 a reference is written as a copy of its target's data file, and each of
    # those copies has to be made durable too, which is what fsync_backup_files = 1 asserts here.
    # deduplicate_files = 1 is the control: it stores the duplicate once instead of copying it, so
    # it has no copies at all and a smaller num_entries.
    #
    # The total is the per-host data-file fsyncs summed plus the initiator's single manifest fsync.
    # Summing is necessary because which host ends up owning a shared zk root is decided by
    # coordination, so no single host has a stable count. The sum is exact for the backup's own
    # fsyncs - one per entry actually written, targets and copies alike, a plain backup keeping every
    # file info as its own entry, plus the manifest. The two terms are read from different system
    # tables because only one surface is ordered for each; see the helpers.
    backup_query_id = f"backup_{backup_path}"
    nodes = [node1, node2, node3]
    diagnostics = {}
    node1.query(
        f"BACKUP DATABASE {database_name} ON CLUSTER cluster TO {backup_name}"
        f" SETTINGS async = false, deduplicate_files = {deduplicate_files}, fsync_backup_files = 1,"
        f" log_query_threads = 1, log_profile_events = 1;",
        query_id=backup_query_id,
    )
    data_file_syncs = sum(
        get_backup_data_file_syncs(node, backup_path, backup_query_id, diagnostics)
        for node in nodes
    )
    # num_entries is cluster-wide: BackupImpl recomputes it over the file infos of all hosts while
    # writing the manifest.
    num_entries, manifest_file_syncs = get_backup_manifest_file_sync(
        node1, backup_query_id, diagnostics
    )
    file_syncs = data_file_syncs + manifest_file_syncs
    # Logged unconditionally: a green run's per-node split and the attempt count of the manifest read
    # are what tell a later reader whether the split oracle is still measuring what it claims to.
    logging.info("backup fsync breakdown: %s", diagnostics)
    assert file_syncs == num_entries + 1, (
        f"expected {num_entries + 1} fsyncs across the cluster"
        f" ({num_entries} entries + the manifest), got {file_syncs}"
        f" ({data_file_syncs} data files + {manifest_file_syncs} manifest); {diagnostics}"
    )

    node1.query(f"DROP DATABASE {database_name} ON CLUSTER cluster SYNC;")

    def apply_for_all_nodes(f):
        for node in [node1, node2, node3]:
            f(node)

    def change_keeper_map_prefix(node):
        node.replace_config(
            "/etc/clickhouse-server/config.d/keeper_map_path_prefix.xml",
            """
<clickhouse>
    <keeper_map_path_prefix>/different_path/keeper_map</keeper_map_path_prefix>
</clickhouse>
""",
        )

    apply_for_all_nodes(lambda node: node.stop_clickhouse())
    apply_for_all_nodes(change_keeper_map_prefix)
    apply_for_all_nodes(lambda node: node.start_clickhouse())

    node1.query(
        f"RESTORE DATABASE {database_name} ON CLUSTER cluster FROM {backup_name} SETTINGS async = false;"
    )

    verify_data()

    node1.query(f"DROP TABLE {database_name}.keeper3 ON CLUSTER cluster SYNC;")
    node1.query(
        f"RESTORE TABLE {database_name}.keeper3 ON CLUSTER cluster FROM {backup_name} SETTINGS async = false;"
    )

    verify_data()

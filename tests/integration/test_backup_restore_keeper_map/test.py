
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


backup_id_counter = 0


def new_backup_id():
    """A per-run counter for backup destinations.

    /backups is a shared mount that survives between the cases of a module, and a BACKUP to a
    destination that already holds one fails outright, so a fixed name only works on a first run.
    """
    global backup_id_counter
    backup_id_counter += 1
    return backup_id_counter


def get_backup_file_syncs(node, backup_name):
    """Sum FileSync over the threads of this node's own operations for one backup.

    Scoped to the backup rather than read from `system.events`, whose counters are process-wide:
    `fsync_metadata` defaults to true, so an unrelated `CREATE TABLE` - in particular the lazy
    creation of a system log table on its first flush - fsyncs its metadata file and would inflate a
    process-wide delta, failing an exact assertion on an unrelated change. A per-thread row can only
    hold the fsyncs of its own query.

    All the fsync work is attributable: the per-file ones (the data file and its copies alike) run in
    `writeFile` on `BackupWorker` pool threads, and the initiator's `.backup` fsync runs in
    `finalizeWriting` on the `BackupAsync` thread of a foreground BACKUP, whose query context outlives
    it. The rows are waited for rather than read once, because a host reaches its coordination finish
    node in `finalizeWriting`, before `setStatus(BACKUP_CREATED)` writes the `system.backup_log` row
    the query ids come from.

    `system.backup_log` is used rather than `system.backups` because the latter hides the internal
    operations of an ON CLUSTER backup (`getAllInfos` skips them), and those are exactly the ones that
    write the data files. Every host runs one, including the host the query was issued on, which also
    runs the initiator operation - hence "operations", plural, per node.
    """

    def read_backup_log_query_ids():
        node.query("SYSTEM FLUSH LOGS backup_log")
        # `name` holds the destination re-formatted from the AST, so match on the unique path only.
        return node.query(
            f"SELECT DISTINCT query_id FROM system.backup_log"
            f" WHERE name LIKE '%{backup_name}%' AND status = 'BACKUP_CREATED'"
            f" ORDER BY query_id FORMAT TSV"
        ).split()

    try:
        query_ids = wait_condition(
            read_backup_log_query_ids,
            lambda ids: len(ids) > 0,
            max_attempts=60,
            delay=0.5,
        )
    except Exception as exception:
        raise AssertionError(
            f"no BACKUP_CREATED row on {node.name} for {backup_name}"
        ) from exception

    id_list = ", ".join(f"'{query_id}'" for query_id in query_ids)

    # Wait for the threads that do the syncing, not for "any row": the foreground query thread logs a
    # row promptly, so a zero read too early would look like "no fsyncs" instead of "not logged yet".
    def count_backup_thread_rows():
        node.query("SYSTEM FLUSH LOGS query_thread_log")
        return int(
            node.query(
                f"SELECT count() FROM system.query_thread_log"
                f" WHERE query_id IN ({id_list})"
                f" AND thread_name IN ('BackupWorker', 'BackupAsync')"
            ).strip()
        )

    wait_condition(
        count_backup_thread_rows, lambda n: n > 0, max_attempts=60, delay=0.5
    )

    return int(
        node.query(
            f"SELECT sum(ProfileEvents['FileSync']) FROM system.query_thread_log"
            f" WHERE query_id IN ({id_list})"
        ).strip()
    )


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

    backup_path = f"test_on_cluster_fsync{deduplicate_files}_{new_backup_id()}"
    backup_name = f"Disk('backups', '{backup_path}')"
    # keeper1 and keeper2 share one zk root, and only one table may own the data of a root, so the
    # other one is collected as a BackupEntryReference - the only producer of one in the tree. With
    # deduplicate_files = 0 a reference is written as a copy of its target's data file, and each of
    # those copies has to be made durable too, which is what fsync_backup_files = 1 asserts here.
    # deduplicate_files = 1 is the control: it stores the duplicate once instead of copying it, so
    # it has no copies at all and a smaller num_entries.
    #
    # Each host's count comes from the threads of its own backup operations, and those are summed:
    # every host writes and syncs the data files it produced itself, the initiator additionally syncs
    # the one .backup manifest last in finalizeWriting, and which host ends up owning a shared zk root
    # is decided by coordination, so no single host has a stable count. The sum is exact for the
    # backup's own fsyncs - one per entry actually written, targets and copies alike, a plain backup
    # keeping every file info as its own entry, plus the manifest.
    backup_query_id = f"backup_{backup_path}"
    nodes = [node1, node2, node3]
    node1.query(
        f"BACKUP DATABASE {database_name} ON CLUSTER cluster TO {backup_name}"
        f" SETTINGS async = false, deduplicate_files = {deduplicate_files}, fsync_backup_files = 1,"
        f" log_query_threads = 1, log_profile_events = 1;",
        query_id=backup_query_id,
    )
    file_syncs = sum(get_backup_file_syncs(node, backup_path) for node in nodes)

    # system.backups is in-memory and holds the cluster-wide counters, which BackupImpl recomputes
    # over the file infos of all hosts while writing the manifest. Waiting for the BACKUP_CREATED
    # row rather than reading whatever is there keeps a partially filled row out of the assertion.
    num_entries = int(
        wait_condition(
            lambda: node1.query(
                f"SELECT num_entries FROM system.backups"
                f" WHERE query_id = '{backup_query_id}' AND status = 'BACKUP_CREATED'"
            ).strip(),
            lambda s: s != "",
            max_attempts=60,
            delay=0.5,
        )
    )
    assert file_syncs == num_entries + 1, (
        f"expected {num_entries + 1} fsyncs across the cluster"
        f" ({num_entries} entries + the manifest), got {file_syncs}"
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

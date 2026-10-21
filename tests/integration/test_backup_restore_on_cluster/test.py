import os.path
import random
import re
import string

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry, wait_condition

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/cluster.xml",
    "configs/cluster3.xml",
    "configs/replicated_access_storage.xml",
    "configs/replicated_user_defined_sql_objects.xml",
    "configs/named_collections_storage.xml",
    "configs/backups_disk.xml",
    "configs/backups_allowed_path.xml",  # Needed by the File() destination of test_backup_on_cluster_internal_writer_fsyncs.
    "configs/query_thread_log.xml",  # The common integration config removes query_thread_log; that test needs it.
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
]

# node3 holds a different named-collections encryption key (and a different Keeper path), to model
# a restore target without the key, see test_system_named_collections_key_mismatch.
main_configs_other_nc_key = [
    (
        "configs/named_collections_storage_other_key.xml"
        if config == "configs/named_collections_storage.xml"
        else config
    )
    for config in main_configs
]

user_configs = [
    "configs/allow_database_types.xml",
    "configs/zookeeper_retries.xml",
    "configs/named_collections_users.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,  # Necessary for the "test_stop_other_host_while_backup" test
)

node3 = cluster.add_instance(
    "node3",
    main_configs=main_configs_other_nc_key + ["configs/shutdown_wait_unfinished_queries.xml"],
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node3", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,  # Necessary for the "test_shutdown_waits_for_restore_that_arrives_during_shutdown" test
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP TABLE IF EXISTS tbl2 ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP DATABASE IF EXISTS mydb2 ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP DATABASE IF EXISTS initdb ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP DATABASE IF EXISTS addrdb ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP USER IF EXISTS u1, u2 ON CLUSTER 'cluster3'")
        node1.query("DROP NAMED COLLECTION IF EXISTS backup_nc")
        node1.query("DROP NAMED COLLECTION IF EXISTS backup_nc_mismatch")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def new_backup_id():
    global backup_id_counter
    backup_id_counter += 1
    return backup_id_counter


def get_path_to_backup(backup_name):
    name = backup_name.split(",")[1].strip("')/ ")
    return os.path.join(node1.cluster.instances_dir, "backups", name)


def test_replicated_table():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (1, 'Don''t')")
    node2.query("INSERT INTO tbl VALUES (2, 'count')")
    node1.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (3, 'your')")
    node2.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (4, 'chickens')")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=1"
    )

    # Drop table on both nodes.
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )


def test_empty_replicated_table():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=1"
    )

    # Drop table on both nodes.
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl") == ""
    assert node2.query("SELECT * FROM tbl") == ""


def test_replicated_database():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("INSERT INTO mydb.tbl VALUES (1, 'Don''t')")
    node2.query("INSERT INTO mydb.tbl VALUES (2, 'count')")
    node1.query("INSERT INTO mydb.tbl VALUES (3, 'your')")
    node2.query("INSERT INTO mydb.tbl VALUES (4, 'chickens')")
    node1.query("OPTIMIZE TABLE mydb.tbl ON CLUSTER 'cluster' FINAL")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    # check data in sync
    expect = TSV([[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]])
    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == expect
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == expect

    # Make backup.
    backup_name = new_backup_name()
    node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=2"
    )

    # Drop table on both nodes.
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == expect
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == expect


def test_replicated_database_compare_parts():
    """
    stop merges and fetches then write data to two nodes and
    compare that parts are restored from single node (second) after backup
    replica is selected by settings replica_num=2, replica_num_in_backup=2
    """
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("SYSTEM STOP MERGES mydb.tbl")
    node2.query("SYSTEM STOP MERGES mydb.tbl")

    node1.query("SYSTEM STOP FETCHES mydb.tbl")
    node2.query("SYSTEM STOP FETCHES mydb.tbl")

    node1.query("INSERT INTO mydb.tbl VALUES (1, 'a')")
    node1.query("INSERT INTO mydb.tbl VALUES (2, 'b')")

    node2.query("INSERT INTO mydb.tbl VALUES (3, 'x')")
    node2.query("INSERT INTO mydb.tbl VALUES (4, 'y')")

    p2 = node2.query("SELECT * FROM mydb.tbl ORDER BY x")

    # Make backup.
    backup_name = new_backup_name()
    node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=2"
    )

    # Drop table on both nodes.
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=2"
    )
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    # compare parts
    p1_ = node1.query("SELECT _part, * FROM mydb.tbl ORDER BY x")
    p2_ = node2.query("SELECT _part, * FROM mydb.tbl ORDER BY x")
    assert p1_ == p2_

    # compare data
    assert p2 == node2.query("SELECT * FROM mydb.tbl ORDER BY x")


def test_different_tables_on_nodes():
    node1.query(
        "CREATE TABLE tbl (`x` UInt8, `y` String) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("CREATE TABLE tbl (`w` Int64) ENGINE = MergeTree ORDER BY w")

    node1.query(
        "INSERT INTO tbl VALUES (1, 'Don''t'), (2, 'count'), (3, 'your'), (4, 'chickens')"
    )
    node2.query("INSERT INTO tbl VALUES (-333), (-222), (-111), (0), (111)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")

    assert node1.query("SELECT * FROM tbl") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )
    assert node2.query("SELECT * FROM tbl") == TSV([-333, -222, -111, 0, 111])


def test_embedded_rocksdb_same_name_on_nodes():
    # Same local EmbeddedRocksDB table name on two hosts. Each host has its own host-local
    # rocksdb_dir (the default dir derived from the table name is the same STRING on both hosts),
    # so the backup/restore coordination election_id is identical across hosts. This exercises the
    # OnCluster RocksDB coordination path: without host-qualified znode names the second host's
    # registration would collide (ZNODEEXISTS) and lose its data entry. Each host must back up and
    # restore its own rows.
    node1.query(
        "CREATE TABLE tbl (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)"
    )
    node2.query(
        "CREATE TABLE tbl (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)"
    )

    node1.query("INSERT INTO tbl VALUES (1, 'node1_a'), (2, 'node1_b')")
    node2.query("INSERT INTO tbl VALUES (10, 'node2_a'), (20, 'node2_b'), (30, 'node2_c')")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")

    assert node1.query("SELECT * FROM tbl ORDER BY key") == TSV(
        [[1, "node1_a"], [2, "node1_b"]]
    )
    assert node2.query("SELECT * FROM tbl ORDER BY key") == TSV(
        [[10, "node2_a"], [20, "node2_b"], [30, "node2_c"]]
    )


def get_internal_backup_file_syncs(node, backup_path):
    """Sum FileSync over the threads of this node's internal backup operation.

    The internal operation's own query row cannot be used: `rewriteSettingsWithoutOnCluster`
    injects `async = true` along with `internal = true`, so the work runs on a detached thread
    that starts after the DDL query row is already closed. The per-file fsyncs still land in
    `system.query_thread_log` under that query id, because they run on `BackupWorker` pool
    threads which detach while `BackupStarter` - the only strong reference to the query context
    that `finalizePerformanceCounters` needs - is still alive.

    Waiting for "any row" would return too early: the foreground DDL thread logs a row for that
    id promptly. Wait for the `BackupWorker` rows instead, which are exactly the threads whose
    fsyncs are being counted, so a zero afterwards means "no fsyncs" and not "not logged yet".

    `system.backup_log` is used rather than `system.backups` because the latter hides internal
    operations (`getAllInfos` skips them). Only the row's query id is read: its size/entry
    counters are always 0 for an internal operation, since `doBackup` fills them only when not
    internal. That read is retried because the row is not ordered before the initiator's BACKUP
    returns - see the call site.
    """

    def read_backup_log_query_id():
        node.query("SYSTEM FLUSH LOGS backup_log")
        # `name` holds the destination re-formatted from the AST, so match on the unique path only.
        return node.query(
            f"SELECT query_id FROM system.backup_log"
            f" WHERE name LIKE '%{backup_path}%' AND status = 'BACKUP_CREATED'"
            f" ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip()

    try:
        query_id = wait_condition(
            read_backup_log_query_id, lambda s: s != "", max_attempts=60, delay=0.5
        )
    except Exception as exception:
        raise AssertionError(
            f"no BACKUP_CREATED row on {node.name} for {backup_path}"
        ) from exception

    def count_worker_rows():
        node.query("SYSTEM FLUSH LOGS query_thread_log")
        return int(
            node.query(
                f"SELECT count() FROM system.query_thread_log"
                f" WHERE query_id = '{query_id}' AND thread_name = 'BackupWorker'"
            ).strip()
        )

    wait_condition(count_worker_rows, lambda n: n > 0, max_attempts=60, delay=0.5)

    return int(
        node.query(
            f"SELECT sum(ProfileEvents['FileSync']) FROM system.query_thread_log"
            f" WHERE query_id = '{query_id}'"
        ).strip()
    )


def get_directory_syncs(node):
    """Read this node's process-wide DirectorySync counter.

    `system.events` exposes `ProfileEvents::global_counters` of the queried server process only,
    so this cannot observe another node's fsyncs - which is what makes it usable here, node2
    being the follower. `sum()` is used because the row is absent while the counter is 0.
    """
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'DirectorySync'"
        ).strip()
    )


def test_backup_on_cluster_internal_writer_fsyncs():
    # BACKUP ON CLUSTER makes every host write its own data files, and each of those hosts has to
    # fsync them and the directories it created - not just the initiator, which is the only host a
    # stateless test can have. Only the .backup manifest and the lock file are initiator-only.
    #
    # File() is used rather than an archive on purpose: archives are rejected outright for an
    # on-cluster backup ("Using archives with backups on clusters is disabled"), so there is no
    # internal-writer archive path to cover here.
    node1.query(
        "CREATE TABLE tbl (`x` UInt8, `y` String) ENGINE = MergeTree ORDER BY x"
    )
    node2.query(
        "CREATE TABLE tbl (`x` UInt8, `y` String) ENGINE = MergeTree ORDER BY x"
    )
    node1.query("INSERT INTO tbl VALUES (1, 'node1_only')")
    node2.query("INSERT INTO tbl VALUES (2, 'node2_only')")

    # Every BACKUP is issued on node1, so node2 is always an internal (non-initiator) writer, and
    # the query id read from node2's own system.backup_log scopes the file counter to its internal
    # operation. The nodes share /backups through external_dirs, so a File() destination is
    # reachable from both. log_query_threads/log_profile_events reach node2 because
    # extractCoreSettingsFromQuery forwards non-backup settings to the other hosts.
    #
    # The directory fsyncs cannot be attributed to a query id at all: they run in
    # finalizeWriting on the starter thread, whose query_thread_log row is dropped because the
    # internal backup is forced async and ThreadGroup::query_context (a weak pointer) has already
    # expired with the BackupStarter by the time that thread detaches. They are therefore counted
    # as a delta of node2's own process-wide counter, which is a lower bound rather than an exact
    # per-directory count; the exact per-directory anchors are the initiator-side stateless test's
    # job. The fsync_backup_files = 0 control below is what makes the bound meaningful.
    logging_settings = "log_query_threads = 1, log_profile_events = 1"

    # The destinations carry a per-run counter: /backups is a shared mount that survives between
    # runs within a module, and a BACKUP to a destination that already holds one fails outright.
    on_path = f"/backups/fsync_oc_{new_backup_id()}_on"
    off_path = f"/backups/fsync_oc_{new_backup_id()}_off"

    dir_syncs_before = get_directory_syncs(node2)
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO File('{on_path}')"
        f" SETTINGS fsync_backup_files = 1, {logging_settings}"
    )
    # BACKUP ON CLUSTER returns only after waitOtherHostsFinish, and node2 reaches its finish node
    # only after finalizeWriting, so node2 has done all its writing and syncing by now and the
    # delta is safe to read straight away. That wait says nothing about node2's log tables:
    # setStatus(BACKUP_CREATED), which writes the system.backup_log row, runs after finish() - so
    # the helper below retries that read.
    dir_syncs_on = get_directory_syncs(node2) - dir_syncs_before

    file_syncs_on = get_internal_backup_file_syncs(node2, on_path)
    assert (
        file_syncs_on > 0
    ), f"internal writer synced no files (FileSync={file_syncs_on})"
    assert (
        dir_syncs_on > 0
    ), f"internal writer synced no directories (DirectorySync delta={dir_syncs_on})"

    # Control: with the opt-out the same internal writer must issue exactly zero fsyncs of either
    # kind. A non-zero directory delta here would mean something other than the backup increments
    # the counter in this module, which would void the assertion above rather than weaken it.
    dir_syncs_before = get_directory_syncs(node2)
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO File('{off_path}')"
        f" SETTINGS fsync_backup_files = 0, {logging_settings}"
    )
    dir_syncs_off = get_directory_syncs(node2) - dir_syncs_before

    file_syncs_off = get_internal_backup_file_syncs(node2, off_path)
    assert (file_syncs_off, dir_syncs_off) == (0, 0), (
        f"fsync_backup_files = 0 still synced {file_syncs_off} files"
        f" / {dir_syncs_off} directories"
    )

    # The rows restored on node2 are rows only node2 ever held, so node2 did write data files into
    # the backup: the assertions above cannot pass vacuously. This also checks the durability
    # change did not corrupt an on-cluster backup.
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM File('{on_path}')")
    assert node1.query("SELECT * FROM tbl") == TSV([[1, "node1_only"]])
    assert node2.query("SELECT * FROM tbl") == TSV([[2, "node2_only"]])


def test_backup_restore_on_single_replica():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )
    node1.query(
        "CREATE TABLE mydb.test (`name` String, `value` UInt32) ENGINE = ReplicatedMergeTree ORDER BY value"
    )
    node1.query("INSERT INTO mydb.test VALUES ('abc', 1), ('def', 2)")
    node1.query("INSERT INTO mydb.test VALUES ('ghi', 3)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb TO {backup_name}")

    node1.query("DROP DATABASE mydb SYNC")

    # Cannot restore table because it already contains data on other replicas.
    expected_error = "already contains some data"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE DATABASE mydb FROM {backup_name}"
    )

    # Can restore table with structure_only=true.
    node1.query(
        f"RESTORE DATABASE mydb FROM {backup_name} SETTINGS structure_only=true"
    )

    node1.query("SYSTEM SYNC REPLICA mydb.test")
    assert node1.query("SELECT * FROM mydb.test ORDER BY name") == TSV(
        [["abc", 1], ["def", 2], ["ghi", 3]]
    )

    # Can restore table with allow_non_empty_tables=true.
    node1.query("DROP DATABASE mydb SYNC")
    node1.query(
        f"RESTORE DATABASE mydb FROM {backup_name} SETTINGS allow_non_empty_tables=true"
    )

    node1.query("SYSTEM SYNC REPLICA mydb.test")
    assert node1.query("SELECT * FROM mydb.test ORDER BY name") == TSV(
        [["abc", 1], ["abc", 1], ["def", 2], ["def", 2], ["ghi", 3], ["ghi", 3]]
    )


def test_table_with_parts_in_queue_considered_non_empty():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )
    node1.query(
        "CREATE TABLE mydb.test (`x` UInt32) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    node1.query("INSERT INTO mydb.test SELECT number AS x FROM numbers(10000000)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb TO {backup_name}")

    node1.query("DROP DATABASE mydb SYNC")

    # Cannot restore table because it already contains data on other replicas.
    expected_error = "already contains some data"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE DATABASE mydb FROM {backup_name}"
    )


def test_replicated_table_with_uuid_in_zkpath():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}','{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (1, 'AA')")
    node2.query("INSERT INTO tbl VALUES (2, 'BB')")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    # The table `tbl2` is expected to have a different UUID so it's ok to have both `tbl` and `tbl2` at the same time.
    node2.query(f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}")

    node1.query("INSERT INTO tbl2 VALUES (3, 'CC')")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    for instance in [node1, node2]:
        assert instance.query("SELECT * FROM tbl ORDER BY x") == TSV(
            [[1, "AA"], [2, "BB"]]
        )
        assert instance.query("SELECT * FROM tbl2 ORDER BY x") == TSV(
            [[1, "AA"], [2, "BB"], [3, "CC"]]
        )


def test_replicated_table_restore_keeps_duplicate_parts():
    try:
        node1.query(
            "CREATE TABLE tbl ON CLUSTER 'cluster' ("
            "key UInt64, value String, array Array(String)"
            ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}', '{replica}')"
            "ORDER BY tuple() SETTINGS replicated_deduplication_window = 0, "
            "replicated_deduplication_window_for_async_inserts = 0"
        )

        node1.query("SYSTEM STOP MERGES ON CLUSTER 'cluster' tbl")

        node1.query("INSERT INTO tbl VALUES (0, 'same', ['same'])")
        node1.query("INSERT INTO tbl VALUES (0, 'same', ['same'])")
        node1.query(
            "ALTER TABLE tbl ON CLUSTER 'cluster' MODIFY SETTING "
            "replicated_deduplication_window = 10000, "
            "replicated_deduplication_window_for_async_inserts = 10000"
        )

        assert node1.query("SELECT count() FROM tbl") == "2\n"
        assert (
            node1.query("SELECT count() FROM system.parts WHERE table = 'tbl' AND active")
            == "2\n"
        )

        backup_name = new_backup_name()
        node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

        node2.query(f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}")
        node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

        expected = node1.query("SELECT count(), sum(sipHash64(*)) FROM tbl")
        for instance in [node1, node2]:
            assert instance.query("SELECT count() FROM tbl2") == "2\n"
            assert instance.query("SELECT count(), sum(sipHash64(*)) FROM tbl2") == expected
    finally:
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")
        node1.query("DROP TABLE IF EXISTS tbl2 ON CLUSTER 'cluster' SYNC")


def test_replicated_table_with_not_synced_insert():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM STOP REPLICATED SENDS ON CLUSTER 'cluster' tbl")

    node1.query("INSERT INTO tbl VALUES (333)")
    node2.query("INSERT INTO tbl VALUES (444)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])


def test_replicated_table_with_not_synced_merge():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("SYSTEM STOP MERGES ON CLUSTER 'cluster' tbl")

    node1.query("INSERT INTO tbl VALUES (111)")
    node1.query("INSERT INTO tbl VALUES (222)")

    node2.query("SYSTEM SYNC REPLICA tbl")

    node2.query("SYSTEM START MERGES tbl")
    node2.query("OPTIMIZE TABLE tbl FINAL")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_replicated_table_restored_into_bigger_cluster():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster3' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster3' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node3.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_replicated_table_restored_into_smaller_cluster():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster1' FROM {backup_name}")
    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_replicated_database_async():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query("CREATE TABLE mydb.tbl(x UInt8) ENGINE=ReplicatedMergeTree ORDER BY x")

    node1.query(
        "CREATE TABLE mydb.tbl2(y String) ENGINE=ReplicatedMergeTree ORDER BY y"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("INSERT INTO mydb.tbl VALUES (1)")
    node1.query("INSERT INTO mydb.tbl VALUES (22)")
    node2.query("INSERT INTO mydb.tbl2 VALUES ('a')")
    node2.query("INSERT INTO mydb.tbl2 VALUES ('bb')")

    node1.query("OPTIMIZE TABLE mydb.tbl ON CLUSTER 'cluster' FINAL")
    node1.query("OPTIMIZE TABLE mydb.tbl2 ON CLUSTER 'cluster' FINAL")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl2")

    backup_name = new_backup_name()
    [id, status] = node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} ASYNC"
    ).split("\t")

    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["BACKUP_CREATED", ""]]),
    )

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    [id, status] = node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} ASYNC"
    ).split("\t")

    assert status == "RESTORING\n" or status == "RESTORED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["RESTORED", ""]]),
    )

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl2")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV([1, 22])
    assert node2.query("SELECT * FROM mydb.tbl2 ORDER BY y") == TSV(["a", "bb"])


@pytest.mark.parametrize("special_macro", ["uuid", "database"])
def test_replicated_database_with_special_macro_in_zk_path(special_macro):
    zk_path = "/clickhouse/databases/{" + special_macro + "}"
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('"
        + zk_path
        + "','{shard}','{replica}')"
    )

    # ReplicatedMergeTree without arguments means ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
    node1.query("CREATE TABLE mydb.tbl(x Int64) ENGINE=ReplicatedMergeTree ORDER BY x")

    node1.query("INSERT INTO mydb.tbl VALUES (-3)")
    node1.query("INSERT INTO mydb.tbl VALUES (1)")
    node1.query("INSERT INTO mydb.tbl VALUES (10)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name}")

    # RESTORE DATABASE with rename should work here because the new database will have another UUID and thus another zookeeper path.
    node1.query(
        f"RESTORE DATABASE mydb AS mydb2 ON CLUSTER 'cluster' FROM {backup_name}"
    )

    node1.query("INSERT INTO mydb.tbl VALUES (2)")

    node1.query("SYSTEM SYNC DATABASE REPLICA ON CLUSTER 'cluster' mydb2")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb2.tbl")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV(
        [[-3], [1], [2], [10]]
    )

    assert node1.query("SELECT * FROM mydb2.tbl ORDER BY x") == TSV([[-3], [1], [10]])
    assert node2.query("SELECT * FROM mydb2.tbl ORDER BY x") == TSV([[-3], [1], [10]])


# By default `backup_restore_keeper_value_max_size` is 1 MB, but in this test we'll set it to 50 bytes just to check it works.
def test_keeper_value_max_size():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM STOP REPLICATED SENDS ON CLUSTER 'cluster' tbl")

    node1.query("INSERT INTO tbl VALUES (333)")
    node2.query("INSERT INTO tbl VALUES (444)")

    backup_name = new_backup_name()
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}",
        settings={"backup_restore_keeper_value_max_size": 50},
    )

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])


@pytest.mark.parametrize(
    "interface, on_cluster", [("native", True), ("http", True), ("http", False)]
)
def test_async_backups_to_same_destination(interface, on_cluster):
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    nodes = [node1, node2]
    node1.query("INSERT INTO tbl VALUES (1)")

    backup_name = new_backup_name()
    on_cluster_part = "ON CLUSTER 'cluster'" if on_cluster else ""

    # Multiple backups to the same destination.
    ids = []
    for node in nodes:
        if interface == "http":
            res, err = node.http_query_and_get_answer_with_error(
                f"BACKUP TABLE tbl {on_cluster_part} TO {backup_name} ASYNC"
            )
        else:
            res, err = node.query_and_get_answer_with_error(
                f"BACKUP TABLE tbl {on_cluster_part} TO {backup_name} ASYNC"
            )

        # The second backup to the same destination is expected to fail. It can either fail immediately or after a while.
        # If it fails immediately we won't even get its ID.
        if not err:
            ids.append(res.split("\t")[0])

    ids_for_query = "[" + ", ".join(f"'{id}'" for id in ids) + "]"

    for node in nodes:
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.backups WHERE id IN {ids_for_query} AND status == 'CREATING_BACKUP'",
            "",
        )

    num_created_backups = sum(
        [
            int(
                node.query(
                    f"SELECT count() FROM system.backups WHERE id IN {ids_for_query} AND status == 'BACKUP_CREATED'"
                ).strip()
            )
            for node in nodes
        ]
    )

    num_failed_backups = sum(
        [
            int(
                node.query(
                    f"SELECT count() FROM system.backups WHERE id IN {ids_for_query} AND status == 'BACKUP_FAILED'"
                ).strip()
            )
            for node in nodes
        ]
    )

    # Only one backup should succeed.
    if (num_created_backups != 1) or (num_failed_backups != len(ids) - 1):
        for node in nodes:
            print(
                node.query(
                    f"SELECT status, error FROM system.backups WHERE id IN {ids_for_query}"
                )
            )

    assert num_created_backups == 1
    assert num_failed_backups == len(ids) - 1

    # Check that the succeeded backup is all right.
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(f"RESTORE TABLE tbl FROM {backup_name}")
    assert node1.query("SELECT * FROM tbl") == "1\n"


def test_required_privileges():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (100)")

    node1.query("CREATE USER u1")
    node1.query("GRANT CLUSTER ON *.* TO u1")
    # new_backup_name() returns a Disk(...) locator, so the DISK source grant is required.
    node1.query("GRANT READ ON DISK, WRITE ON DISK TO u1")

    backup_name = new_backup_name()
    expected_error = "necessary to have the grant BACKUP ON default.tbl"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}", user="u1"
    )

    node1.query("GRANT BACKUP ON tbl TO u1")
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}", user="u1")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON default.tbl2"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    node1.query("GRANT INSERT, CREATE TABLE ON tbl2 TO u1")
    node1.query(
        f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )
    node2.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    assert node2.query("SELECT * FROM tbl2") == "100\n"

    node1.query("DROP TABLE tbl2 ON CLUSTER 'cluster' SYNC")
    node1.query("REVOKE ALL FROM u1")
    # REVOKE ALL took the DISK grant too; the RESTORE below still needs the read direction.
    node1.query("GRANT READ ON DISK TO u1")

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON default.tbl"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE ALL ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    node1.query("GRANT INSERT, CREATE TABLE ON tbl TO u1")
    node1.query(f"RESTORE ALL ON CLUSTER 'cluster' FROM {backup_name}", user="u1")
    node2.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node2.query("SELECT * FROM tbl") == "100\n"


def test_system_users():
    node1.query("CREATE USER u1 SETTINGS custom_a=123")
    node1.query("GRANT SELECT ON tbl TO u1")

    backup_name = new_backup_name()
    node1.query("CREATE USER u2 SETTINGS allow_backup=false")
    node1.query("GRANT CLUSTER ON *.* TO u2")
    # new_backup_name() returns a Disk(...) locator, so the DISK source grant is required.
    node1.query("GRANT READ ON DISK, WRITE ON DISK TO u2")

    expected_error = "necessary to have the grant BACKUP ON system.users"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE system.users ON CLUSTER 'cluster' TO {backup_name}", user="u2"
    )

    node1.query("GRANT BACKUP ON system.users TO u2")
    node1.query(
        f"BACKUP TABLE system.users ON CLUSTER 'cluster' TO {backup_name}", user="u2"
    )

    node1.query("DROP USER u1")

    expected_error = (
        "necessary to have the grant SELECT ON default.tbl WITH GRANT OPTION"
    )
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    node1.query("GRANT CREATE USER ON *.* TO u2")

    expected_error = (
        "necessary to have the grant SELECT ON default.tbl WITH GRANT OPTION"
    )
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    node1.query("GRANT SELECT ON tbl TO u2 WITH GRANT OPTION")
    node1.query(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    assert (
        node1.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH no_password SETTINGS custom_a = 123\n"
    )
    assert node1.query("SHOW GRANTS FOR u1") == "GRANT SELECT ON default.tbl TO u1\n"


def test_system_functions():
    node1.query("CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;")
    node1.query("CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE system.functions ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP FUNCTION linear_equation")
    node1.query("DROP FUNCTION parity_str")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.functions WHERE name='parity_str'", ""
    )

    node1.query(
        f"RESTORE TABLE system.functions ON CLUSTER 'cluster' FROM {backup_name}"
    )

    assert node1.query(
        "SELECT number, linear_equation(number, 2, 1) FROM numbers(3)"
    ) == TSV([[0, 1], [1, 3], [2, 5]])

    assert node1.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )

    assert node2.query(
        "SELECT number, linear_equation(number, 2, 1) FROM numbers(3)"
    ) == TSV([[0, 1], [1, 3], [2, 5]])

    assert node2.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )

    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.functions WHERE name='parity_str'",
        "parity_str\n",
    )
    assert node2.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )

    node1.query("DROP FUNCTION linear_equation")
    node1.query("DROP FUNCTION parity_str")


def test_system_named_collections():
    node1.query(
        "CREATE NAMED COLLECTION backup_nc AS url = 'http://example.com/nc_secret_value' NOT OVERRIDABLE, format = 'CSV'"
    )
    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.named_collections WHERE name='backup_nc'",
        "1\n",
    )

    backup_name = new_backup_name()
    node1.query(
        f"BACKUP TABLE system.named_collections ON CLUSTER 'cluster' TO {backup_name}"
    )

    # The collection is copied to the backup as the ciphertext the Keeper storage holds:
    # no plaintext value appears anywhere in the backup.
    entry_files = []
    for dir_path, _, file_names in os.walk(get_path_to_backup(backup_name)):
        for file_name in file_names:
            file_path = os.path.join(dir_path, file_name)
            with open(file_path, "rb") as f:
                content = f.read()
            assert b"nc_secret_value" not in content, file_path
            if file_name.endswith(".sql.enc"):
                entry_files.append(file_path)
                assert content.startswith(b"ENC"), file_path
    assert len(entry_files) == 1

    node1.query("DROP NAMED COLLECTION backup_nc")
    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.named_collections WHERE name='backup_nc'",
        "0\n",
    )

    node1.query(
        f"RESTORE TABLE system.named_collections ON CLUSTER 'cluster' FROM {backup_name}"
    )

    assert node1.query(
        "SELECT mapKeys(collection) FROM system.named_collections WHERE name='backup_nc'"
    ) == TSV([["['format','url']"]])
    # Restored collections are visible on every host as soon as the RESTORE finishes, without
    # waiting for the Keeper watch: each host reloads them synchronously before table creation
    # (a table CREATE from the backup may resolve them), so no retry here.
    assert (
        node2.query("SELECT count() FROM system.named_collections WHERE name='backup_nc'")
        == "1\n"
    )

    node1.query("DROP NAMED COLLECTION backup_nc")


def test_system_named_collections_required_privileges():
    node1.query("CREATE NAMED COLLECTION backup_nc AS key1 = 'nc_secret_value'")

    node1.query("CREATE USER u1")
    node1.query("GRANT CLUSTER ON *.* TO u1")
    # new_backup_name() returns a Disk(...) locator, so the DISK source grant is required.
    node1.query("GRANT READ ON DISK, WRITE ON DISK TO u1")
    node1.query("GRANT BACKUP ON system.named_collections TO u1")

    # Exporting the collections reveals their values, so the initiator must hold the same rights
    # that reading the values requires. The initiator is the only place to check them: with the
    # default `distributed_ddl_use_initial_user_and_roles = false` the other hosts execute the
    # dispatched query with full access.
    backup_name = new_backup_name()
    expected_error = "necessary to have the grant SHOW NAMED COLLECTIONS, SHOW NAMED COLLECTIONS SECRETS ON *"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE system.named_collections ON CLUSTER 'cluster' TO {backup_name}",
        user="u1",
    )

    # `SHOW NAMED COLLECTIONS SECRETS` alone does not allow reading the values back:
    # `displaySecretsInShowAndSelect` is required with it, for the export too.
    node1.query("GRANT SHOW NAMED COLLECTIONS, SHOW NAMED COLLECTIONS SECRETS ON * TO u1")
    expected_error = "necessary to have the grant displaySecretsInShowAndSelect"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE system.named_collections ON CLUSTER 'cluster' TO {backup_name}",
        user="u1",
    )

    node1.query("GRANT displaySecretsInShowAndSelect ON *.* TO u1")
    node1.query(
        f"BACKUP TABLE system.named_collections ON CLUSTER 'cluster' TO {backup_name}",
        user="u1",
    )

    node1.query("DROP NAMED COLLECTION backup_nc")


def test_system_named_collections_implicit_selection_on_cluster():
    node1.query("CREATE NAMED COLLECTION backup_nc AS key1 = 'nc_secret_value'")

    node1.query("CREATE USER u1")
    node1.query("GRANT CLUSTER ON *.* TO u1")
    node1.query("GRANT READ ON DISK, WRITE ON DISK TO u1")
    node1.query("GRANT BACKUP ON *.* TO u1")

    # With `backup_named_collections` unset, an implicit (DATABASE or ALL) selection exports the
    # collections only if the initiating user may read their values. The hosts execute the
    # dispatched query with full access, so the initiator takes that decision before dispatch:
    # for u1, who may not read the values, the hosts must not export them.
    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE system ON CLUSTER 'cluster' TO {backup_name}", user="u1")
    for dir_path, _, file_names in os.walk(get_path_to_backup(backup_name)):
        assert "named_collections" not in os.path.relpath(dir_path, get_path_to_backup(backup_name)), dir_path
        for file_name in file_names:
            with open(os.path.join(dir_path, file_name), "rb") as f:
                assert b"nc_secret_value" not in f.read(), file_name

    # The hosts skip silently on the forwarded resolved value, so the initiator must have warned
    # about the privilege-based skip, like a local backup does.
    assert node1.contains_in_log(
        "Not exporting the named collections to the backup: the initiating user has no rights"
    )

    # An element explicitly naming the table keeps its hard requirement even when a wildcard
    # element selects the table too: the backup must fail rather than silently omit the
    # explicitly requested entries.
    expected_error = "necessary to have the grant SHOW NAMED COLLECTIONS, SHOW NAMED COLLECTIONS SECRETS ON *"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE system.named_collections, DATABASE system ON CLUSTER 'cluster' TO {new_backup_name()}",
        user="u1",
    )

    # The same backup by a user holding the rights exports the collections.
    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE system ON CLUSTER 'cluster' TO {backup_name}")
    entry_files = []
    for dir_path, _, file_names in os.walk(get_path_to_backup(backup_name)):
        if os.path.basename(dir_path) == "named_collections":
            entry_files += file_names
    assert len(entry_files) == 1

    node1.query("DROP NAMED COLLECTION backup_nc")


def test_backup_required_privileges_address_default_database():
    # The addresses of 'cluster_address_default_db' set `default_database`, so an unqualified
    # table name in the dispatched query resolves on every host to that database, not to the
    # initiator's current database. The initiator must authorize what the hosts will actually
    # export: with the default `distributed_ddl_use_initial_user_and_roles = false` the hosts
    # execute the dispatched query with full access.
    node1.query("CREATE DATABASE initdb ON CLUSTER 'cluster'")
    node1.query("CREATE DATABASE addrdb ON CLUSTER 'cluster'")
    node1.query(
        "CREATE TABLE initdb.tbl ON CLUSTER 'cluster' (x UInt8) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/initdb_tbl/', '{replica}') ORDER BY x"
    )
    node1.query(
        "CREATE TABLE addrdb.tbl ON CLUSTER 'cluster' (x UInt8) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/addrdb_tbl/', '{replica}') ORDER BY x"
    )
    node1.query("INSERT INTO addrdb.tbl VALUES (1)")

    node1.query("CREATE USER u1")
    node1.query("GRANT CLUSTER ON *.* TO u1")
    # new_backup_name() returns a Disk(...) locator, so the DISK source grant is required.
    node1.query("GRANT READ ON DISK, WRITE ON DISK TO u1")
    node1.query("GRANT BACKUP ON initdb.* TO u1")

    backup_name = new_backup_name()
    expected_error = "necessary to have the grant BACKUP ON addrdb.tbl"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster_address_default_db' TO {backup_name}",
        user="u1",
        database="initdb",
    )

    node1.query("GRANT BACKUP ON addrdb.tbl TO u1")
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster_address_default_db' TO {backup_name}",
        user="u1",
        database="initdb",
    )

    node1.query("DROP DATABASE initdb ON CLUSTER 'cluster' SYNC")
    node1.query("DROP DATABASE addrdb ON CLUSTER 'cluster' SYNC")


def test_system_named_collections_key_mismatch():
    node1.query("CREATE NAMED COLLECTION backup_nc_mismatch AS key1 = 'value1'")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE system.named_collections TO {backup_name}")
    node1.query("DROP NAMED COLLECTION backup_nc_mismatch")

    # node3 does not hold the key the collections were encrypted with: the restore must fail
    # before anything is restored, with both key fingerprints in the message.
    expected_error = "encrypted with a key with fingerprint"
    assert expected_error in node3.query_and_get_error(
        f"RESTORE TABLE system.named_collections FROM {backup_name}"
    )
    assert (
        node3.query(
            "SELECT count() FROM system.named_collections WHERE name='backup_nc_mismatch'"
        )
        == "0\n"
    )

    # `restore_named_collections = false` is the escape hatch: the restore proceeds without them.
    node3.query(
        f"RESTORE TABLE system.named_collections FROM {backup_name} "
        "SETTINGS restore_named_collections = false"
    )
    assert (
        node3.query(
            "SELECT count() FROM system.named_collections WHERE name='backup_nc_mismatch'"
        )
        == "0\n"
    )

    # A node holding the key restores the collection.
    node2.query(f"RESTORE TABLE system.named_collections FROM {backup_name}")
    assert (
        node2.query(
            "SELECT count() FROM system.named_collections WHERE name='backup_nc_mismatch'"
        )
        == "1\n"
    )

    assert_eq_with_retry(
        node1,
        "SELECT count() FROM system.named_collections WHERE name='backup_nc_mismatch'",
        "1\n",
    )
    node1.query("DROP NAMED COLLECTION backup_nc_mismatch")


def test_projection():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' (x UInt32, y String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}') "
        "ORDER BY y PARTITION BY x%10"
    )
    node1.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(3)")

    node1.query("ALTER TABLE tbl ADD PROJECTION prjmax (SELECT MAX(x))")
    node1.query("INSERT INTO tbl VALUES (100, 'a'), (101, 'b')")

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "2\n"
    )

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "0\n"
    )

    node1.query(f"RESTORE TABLE tbl FROM {backup_name}")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[0, "0"], [1, "1"], [2, "2"], [100, "a"], [101, "b"]]
    )

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "2\n"
    )


def test_file_deduplication():
    # Random column name helps finding it in logs.
    column_name = "".join(random.choice(string.ascii_letters) for x in range(10))

    # Make four replicas in total: 2 on each host.
    node1.query(
        f"""
        CREATE TABLE tbl ON CLUSTER 'cluster' (
        {column_name} Int32
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{{replica}}')
        ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0
        """
    )

    node1.query(
        f"""
        CREATE TABLE tbl2 ON CLUSTER 'cluster' (
        {column_name} Int32
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{{replica}}-2')
        ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0
        """
    )

    # Unique data.
    node1.query(
        "INSERT INTO tbl VALUES (3556), (1177), (4004), (4264), (3729), (1438), (2158), (2684), (415), (1917)"
    )
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl, TABLE tbl2 ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("SYSTEM FLUSH LOGS ON CLUSTER 'cluster'")

    # The bin file should be written to the backup once, and skipped three times (because there are four replicas in total).
    bin_file_writing_log_line = (
        f"Writing backup for file .*{column_name}.bin .* (disk default)"
    )
    bin_file_skip_log_line = f"Writing backup for file .*{column_name}.bin .* skipped"

    num_bin_file_writings = int(node1.count_in_log(bin_file_writing_log_line)) + int(
        node2.count_in_log(bin_file_writing_log_line)
    )
    num_bin_file_skips = int(node1.count_in_log(bin_file_skip_log_line)) + int(
        node2.count_in_log(bin_file_skip_log_line)
    )

    assert num_bin_file_writings == 1
    assert num_bin_file_skips == 3


def test_replicated_table_with_not_synced_def():
    node1.query(
        "CREATE TABLE tbl ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY tuple()"
    )

    node2.query(
        "CREATE TABLE tbl ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY tuple()"
    )

    node2.query("SYSTEM STOP REPLICATION QUEUES tbl")
    node1.query("ALTER TABLE tbl MODIFY COLUMN x String")

    # Not synced because the replication queue is stopped
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "UInt8"], ["y", "String"]])

    backup_name = new_backup_name()
    node2.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # But synced after RESTORE anyway
    node1.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=1"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node2.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=2"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])


def test_table_in_replicated_database_with_not_synced_def():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY tuple()"
    )

    node1.query("ALTER TABLE mydb.tbl MODIFY COLUMN x String")

    backup_name = new_backup_name()
    node2.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # But synced after RESTORE anyway
    node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=1"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    node2.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=2"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])

def has_mutation_in_backup(mutation_id, backup_name, database, table):
    return (
        os.path.exists(
            os.path.join(
                get_path_to_backup(backup_name),
                f"shards/1/replicas/1/data/{database}/{table}/mutations/{mutation_id}.txt",
            )
        )
        or os.path.exists(
            os.path.join(
                get_path_to_backup(backup_name),
                f"shards/1/replicas/2/data/{database}/{table}/mutations/{mutation_id}.txt",
            )
        )
        or os.path.exists(
            os.path.join(
                get_path_to_backup(backup_name),
                f"shards/1/replicas/3/data/{database}/{table}/mutations/{mutation_id}.txt",
            )
        )
    )


def test_mutation():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY tuple()"
    )

    node1.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(5)")

    node2.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(5, 5)")

    node1.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(10, 5)")

    node1.query("ALTER TABLE tbl UPDATE x=x+1 WHERE 1")
    node1.query("ALTER TABLE tbl UPDATE x=x+1+sleep(3) WHERE 1")
    node1.query("ALTER TABLE tbl UPDATE x=x+1+sleep(3) WHERE 1")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    # mutation #0000000000: "UPDATE x=x+1 WHERE 1" could already finish before starting the backup
    # mutation #0000000001: "UPDATE x=x+1+sleep(3) WHERE 1"
    assert has_mutation_in_backup("0000000001", backup_name, "default", "tbl")
    # mutation #0000000002: "UPDATE x=x+1+sleep(3) WHERE 1"
    assert has_mutation_in_backup("0000000002", backup_name, "default", "tbl")
    # mutation #0000000003: not expected
    assert not has_mutation_in_backup("0000000003", backup_name, "default", "tbl")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")


def test_tables_dependency():
    node1.query("CREATE DATABASE mydb ON CLUSTER 'cluster3'")

    node1.query(
        "CREATE TABLE mydb.src ON CLUSTER 'cluster' (x Int64, y String) ENGINE=MergeTree ORDER BY tuple()"
    )

    node1.query(
        "CREATE DICTIONARY mydb.dict ON CLUSTER 'cluster' (x Int64, y String) PRIMARY KEY x "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB 'mydb' TABLE 'src')) LAYOUT(FLAT()) LIFETIME(0)"
    )

    node1.query(
        "CREATE TABLE mydb.dist1 (x Int64) ENGINE=Distributed('cluster', 'mydb', 'src')"
    )

    node3.query(
        "CREATE TABLE mydb.dist2 (x Int64) ENGINE=Distributed(cluster, 'mydb', 'src')"
    )

    node1.query("CREATE TABLE mydb.clusterfunc1 AS cluster('cluster', 'mydb.src')")
    node1.query("CREATE TABLE mydb.clusterfunc2 AS cluster(cluster, mydb.src)")
    node1.query("CREATE TABLE mydb.clusterfunc3 AS cluster(cluster, 'mydb', 'src')")
    node1.query(
        "CREATE TABLE mydb.clusterfunc4 AS cluster(cluster, dictionary(mydb.dict))"
    )
    node1.query(
        "CREATE TABLE mydb.clusterfunc5 AS clusterAllReplicas(cluster, dictionary(mydb.dict))"
    )

    node3.query("CREATE TABLE mydb.clusterfunc6 AS cluster('cluster', 'mydb.src')")
    node3.query("CREATE TABLE mydb.clusterfunc7 AS cluster(cluster, mydb.src)")
    node3.query("CREATE TABLE mydb.clusterfunc8 AS cluster(cluster, 'mydb', 'src')")
    node3.query(
        "CREATE TABLE mydb.clusterfunc9 AS cluster(cluster, dictionary(mydb.dict))"
    )
    node3.query(
        "CREATE TABLE mydb.clusterfunc10 AS clusterAllReplicas(cluster, dictionary(mydb.dict))"
    )

    backup_name = new_backup_name()
    node3.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster3' TO {backup_name}")

    node3.query("DROP DATABASE mydb")

    node3.query(f"RESTORE DATABASE mydb ON CLUSTER 'cluster3' FROM {backup_name}")

    node3.query("SYSTEM FLUSH LOGS ON CLUSTER 'cluster3'")
    expect_in_logs_1 = [
        "Table mydb.src has no dependencies (level 0)",
        "Table mydb.dict has 1 dependencies: mydb.src (level 1)",
        "Table mydb.dist1 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc1 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc2 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc3 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc4 has 1 dependencies: mydb.dict (level 2)",
        "Table mydb.clusterfunc5 has 1 dependencies: mydb.dict (level 2)",
    ]
    expect_in_logs_2 = [
        "Table mydb.src has no dependencies (level 0)",
        "Table mydb.dict has 1 dependencies: mydb.src (level 1)",
    ]
    expect_in_logs_3 = [
        "Table mydb.dist2 has no dependencies (level 0)",
        "Table mydb.clusterfunc6 has no dependencies (level 0)",
        "Table mydb.clusterfunc7 has no dependencies (level 0)",
        "Table mydb.clusterfunc8 has no dependencies (level 0)",
        "Table mydb.clusterfunc9 has no dependencies (level 0)",
        "Table mydb.clusterfunc10 has no dependencies (level 0)",
    ]
    for expect in expect_in_logs_1:
        assert node1.contains_in_log(f"BackupMetadataFinder: {expect}")
    for expect in expect_in_logs_2:
        assert node2.contains_in_log(f"BackupMetadataFinder: {expect}")
    for expect in expect_in_logs_3:
        assert node3.contains_in_log(f"BackupMetadataFinder: {expect}")


def test_get_error_from_other_host():
    node1.query("CREATE TABLE tbl (`x` UInt8) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO tbl VALUES (3)")

    backup_name = new_backup_name()
    expected_error = "Got error from host node2.*Table default.tbl was not found"
    assert re.search(
        expected_error,
        node1.query_and_get_error(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}"
        ),
    )


def test_shutdown_waits_for_backup():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (3)")
    node2.query("INSERT INTO tbl VALUES (5)")

    backup_name = new_backup_name()

    id = node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC"
    ).split("\t")[0]

    # If kill=False the pending backup must be completed
    # If kill=True the pending backup might be completed or failed
    node2.restart_clickhouse(kill=False)

    assert_eq_with_retry(
        node1,
        f"SELECT status FROM system.backups WHERE id='{id}' AND status == 'CREATING_BACKUP'",
        "",
        retry_count=100,
    )

    status = node1.query(f"SELECT status FROM system.backups WHERE id='{id}'").strip()
    assert status == "BACKUP_CREATED"

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA tbl")
    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([3, 5])


def wait_for_unregistered_part(node, query_kind):
    # The node's part of the operation is running, but the backups worker has not registered it.
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.processes WHERE query_kind = '{query_kind}' AND NOT is_initial_query",
        "1",
    )
    query_id = node.query(
        f"SELECT query_id FROM system.processes WHERE query_kind = '{query_kind}' AND NOT is_initial_query"
    ).strip()
    assert query_id
    node.query("SYSTEM FLUSH LOGS backup_log")
    assert node.query(f"SELECT count() FROM system.backup_log WHERE query_id = '{query_id}'") == "0\n"


def test_shutdown_waits_for_backup_that_arrives_during_shutdown():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (3)")
    node2.query("INSERT INTO tbl VALUES (5)")

    # Holds node2's part of the backup after its query is running and before the backup is registered, until
    # the graceful shutdown below stops node2's DDL worker, which it does only after cancelling the queries.
    # The restart clears the failpoint.
    node2.query("SYSTEM ENABLE FAILPOINT backups_hold_registration_until_ddl_worker_stops")

    backup_name = new_backup_name()

    try:
        id = node1.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC"
        ).split("\t")[0]
        wait_for_unregistered_part(node2, "Backup")
    except Exception:
        # Releases the held part, which would block node2's DDL queue for the following tests.
        node2.query("SYSTEM DISABLE FAILPOINT backups_hold_registration_until_ddl_worker_stops")
        raise

    node2.stop_clickhouse(stop_wait_sec=60)
    try:
        # Checked while node2 is down, so its part cannot have been re-run after a restart.
        assert_eq_with_retry(
            node1,
            f"SELECT status FROM system.backups WHERE id='{id}' AND status == 'CREATING_BACKUP'",
            "",
            retry_count=100,
        )
        status = node1.query(f"SELECT status FROM system.backups WHERE id='{id}'").strip()
    finally:
        node2.start_clickhouse()

    assert status == "BACKUP_CREATED"

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA tbl")
    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([3, 5])


# node3 waits for unfinished queries on shutdown, so it drains the late parts only after that wait.
@pytest.mark.parametrize(
    "cluster_name, node_name",
    [("cluster", "node2"), ("cluster3", "node3")],
    ids=["default", "shutdown_wait_unfinished_queries"],
)
def test_shutdown_waits_for_restore_that_arrives_during_shutdown(cluster_name, node_name):
    node = cluster.instances[node_name]
    node1.query(f"CREATE DATABASE mydb ON CLUSTER '{cluster_name}'")
    node1.query(
        f"CREATE TABLE mydb.tbl ON CLUSTER '{cluster_name}' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO mydb.tbl VALUES (3)")
    node.query("INSERT INTO mydb.tbl VALUES (5)")

    # The restore has to create the database, too.
    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb ON CLUSTER '{cluster_name}' TO {backup_name}")
    node1.query(f"DROP DATABASE mydb ON CLUSTER '{cluster_name}' SYNC")

    # Holds the node's part of the restore after its query is running and before the restore is registered, until
    # the graceful shutdown below stops the node's DDL worker, which it does only after cancelling the queries.
    # The restart clears the failpoint.
    node.query("SYSTEM ENABLE FAILPOINT backups_hold_registration_until_ddl_worker_stops")

    try:
        id = node1.query(
            f"RESTORE DATABASE mydb ON CLUSTER '{cluster_name}' FROM {backup_name} ASYNC"
        ).split("\t")[0]
        wait_for_unregistered_part(node, "Restore")
    except Exception:
        # Releases the held part, which would block the node's DDL queue for the following tests.
        node.query("SYSTEM DISABLE FAILPOINT backups_hold_registration_until_ddl_worker_stops")
        raise

    node.stop_clickhouse(stop_wait_sec=60)
    try:
        # Checked while the node is down, so its part cannot have been re-run after a restart.
        assert_eq_with_retry(
            node1,
            f"SELECT status FROM system.backups WHERE id='{id}' AND status == 'RESTORING'",
            "",
            retry_count=100,
        )
        status = node1.query(f"SELECT status FROM system.backups WHERE id='{id}'").strip()
    finally:
        node.start_clickhouse()

    assert status == "RESTORED"

    node.query("SYSTEM SYNC REPLICA mydb.tbl")
    assert node.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV([3, 5])


def test_replicated_table_after_alters():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x Int32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (1)")
    node1.query("ALTER TABLE tbl ADD COLUMN y Int32")
    node1.query("INSERT INTO tbl VALUES (2, 20)")
    node1.query("ALTER TABLE tbl ADD COLUMN z Int32")
    node1.query("INSERT INTO tbl VALUES (3, 30, 300)")

    backup_name = new_backup_name()

    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")

    node1.query("OPTIMIZE TABLE tbl FINAL")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, 0, 0], [2, 20, 0], [3, 30, 300]]
    )

    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, 0, 0], [2, 20, 0], [3, 30, 300]]
    )
def test_except_data_from_table_on_cluster():
    """
    Regression test for EXCEPT DATA FROM TABLE clause distribution to worker hosts.

    Before commits ba96353/3cc6fc2, the EXCEPT DATA FROM TABLE clause was lost when
    BackupsWorker::sendQueryToOtherHosts formatted the query to send to worker nodes,
    causing worker hosts to back up table data instead of just DDL. This test verifies
    that after BACKUP ... EXCEPT DATA FROM TABLE ... ON CLUSTER, all nodes restore
    the table structure but not the data.
    """
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster3' ("
        "x UInt32, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    # Insert data on multiple nodes
    node1.query("INSERT INTO tbl VALUES (1, 'node1_a'), (2, 'node1_b')")
    node2.query("INSERT INTO tbl VALUES (3, 'node2_a'), (4, 'node2_b')")
    node3.query("INSERT INTO tbl VALUES (5, 'node3_a'), (6, 'node3_b')")

    # Sync all replicas
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster3' tbl")

    # Verify all nodes have the data before backup
    expected_before = TSV(
        [[1, "node1_a"], [2, "node1_b"], [3, "node2_a"],
         [4, "node2_b"], [5, "node3_a"], [6, "node3_b"]]
    )
    assert node1.query("SELECT * FROM tbl ORDER BY x") == expected_before
    assert node2.query("SELECT * FROM tbl ORDER BY x") == expected_before
    assert node3.query("SELECT * FROM tbl ORDER BY x") == expected_before

    backup_name = new_backup_name()

    # CRITICAL: This is the clause that was being lost on worker hosts before the fix.
    # The initiator (node1) would correctly exclude data, but worker nodes (node2, node3)
    # would back up data because the EXCEPT DATA FROM TABLE clause was stripped during
    # the format-and-send step in BackupsWorker::sendQueryToOtherHosts.
    node1.query(
        f"BACKUP TABLE tbl EXCEPT DATA FROM TABLE tbl ON CLUSTER 'cluster3' TO {backup_name}"
    )

    # Drop table on all nodes
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster3' SYNC")

    # Restore from backup
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster3' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster3' tbl")

    # Verify table structure exists on all nodes (columns match)
    expected_schema = TSV([["x", "UInt32"], ["y", "String"]])
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl' ORDER BY name"
    ) == expected_schema
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl' ORDER BY name"
    ) == expected_schema
    assert node3.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl' ORDER BY name"
    ) == expected_schema

    # VERIFY THE FIX: All nodes should have zero rows (data was excluded from backup)
    # Before the fix, worker nodes would have 6 rows because they backed up data.
    assert node1.query("SELECT count() FROM tbl") == "0\n"
    assert node2.query("SELECT count() FROM tbl") == "0\n"
    assert node3.query("SELECT count() FROM tbl") == "0\n"


def test_reset_setting_is_not_forwarded_as_a_value():
    # A `name = DEFAULT` in the SETTINGS clause resets the setting on the initiator, which leaves it unset
    # there. The other hosts must end the same way: the reset must reach them neither as an explicit value
    # (which marks the setting as changed, and then e.g. object-storage code overrides the host's own `<s3>`
    # configuration with it) nor with the override it cancels.
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )
    node1.query("INSERT INTO tbl VALUES (1, 'a')")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    backup_name = new_backup_name()
    reset_clause = "max_threads = 3, max_threads = DEFAULT"

    # `max_threads` is also set for the whole query, so the reset in the clause has something to cancel on
    # the initiator besides the override in the clause itself.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS {reset_clause}",
        settings={"max_threads": 7},
    )

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} SETTINGS {reset_clause}",
        settings={"max_threads": 7},
    )
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    assert node2.query("SELECT * FROM tbl") == TSV([[1, "a"]])

    # The per-host queries, the ones received from the initiator, are the non-initial ones.
    escaped_backup_name = backup_name.replace("'", "\\'")
    for node in [node1, node2]:
        node.query("SYSTEM FLUSH LOGS query_log")
        assert node.query(f"""
            SELECT query_kind, mapContains(Settings, 'max_threads')
            FROM system.query_log
            WHERE type = 'QueryFinish' AND NOT is_initial_query
                AND query_kind IN ('Backup', 'Restore') AND position(query, '{escaped_backup_name}') > 0
            ORDER BY query_kind
            """) == TSV([["Backup", 0], ["Restore", 0]])

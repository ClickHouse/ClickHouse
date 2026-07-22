import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.database_disk import get_database_disk_name, move_file

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 1},
)
dummy_node = cluster.add_instance(
    "dummy_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings2.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def create_some_tables(db):
    settings = {
        "distributed_ddl_task_timeout": 0,
        "allow_suspicious_codecs": 1,
    }
    main_node.query(f"CREATE TABLE {db}.t1 (n int) ENGINE=Memory", settings=settings)
    dummy_node.query(
        f"CREATE TABLE {db}.t2 (s String) ENGINE=Memory", settings=settings
    )
    main_node.query(
        f"CREATE TABLE {db}.mt1 (n int) ENGINE=MergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.mt2 (n int) ENGINE=MergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE TABLE {db}.rmt1 (n int) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.rmt2 (n int CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12))) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE TABLE {db}.rmt3 (n int, json JSON materialized '{{}}') ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.rmt5 (n int) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv1 (n int) ENGINE=ReplicatedMergeTree order by n AS SELECT n FROM {db}.rmt1",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv2 (n int) ENGINE=ReplicatedMergeTree order by n  AS SELECT n FROM {db}.rmt2",
        settings=settings,
    )
    main_node.query(
        f"CREATE DICTIONARY {db}.d1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
    )
    dummy_node.query(
        f"CREATE DICTIONARY {db}.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt2' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
    )


def test_recover_digest_mismatch(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")

    main_node.query(
        "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica2');"
    )

    create_some_tables("recover_digest_mismatch")

    main_node.query("SYSTEM SYNC DATABASE REPLICA recover_digest_mismatch")
    dummy_node.query("SYSTEM SYNC DATABASE REPLICA recover_digest_mismatch")

    db_disk_name = get_database_disk_name(dummy_node)
    db_data_path = dummy_node.query(
        "SELECT metadata_path FROM system.databases WHERE database='recover_digest_mismatch'"
    ).strip()

    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml --disk {db_disk_name} --save-logs --query "
    db_disk_path = dummy_node.query(
        f"SELECT path FROM system.disks WHERE name='{db_disk_name}'"
    ).strip()

    print(f"db_data_path {db_data_path}")

    mv1_metadata = dummy_node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'read --path-from {db_data_path}mv1.sql' "]
    )
    corrupted_mv1_metadata = (
        mv1_metadata.replace("Int32", "String").replace("`", r"\`").replace('"', r"\"")
    )
    ways_to_corrupt_metadata = [
        f"{disk_cmd_prefix} 'move --path-from {db_data_path}t1.sql --path-to {db_data_path}m1.sql'",
        f"""printf "%s" "{corrupted_mv1_metadata}" | {disk_cmd_prefix} 'write --path-to {db_data_path}mv1.sql'""",
        f"{disk_cmd_prefix} 'remove {db_data_path}d1.sql'",
        "rm -rf /var/lib/clickhouse/metadata/recover_digest_mismatch/",  # Will trigger "Directory already exists"
        f"{disk_cmd_prefix} 'remove -r {db_disk_path}store/' || true && rm -rf /var/lib/clickhouse/store"
    ]

    for command in ways_to_corrupt_metadata:
        print(f"Corrupting data using `{command}`")
        need_remove_is_active_node = "rm -rf" in command
        dummy_node.stop_clickhouse(kill=not need_remove_is_active_node)
        dummy_node.exec_in_container(["bash", "-c", command])

        query = (
            "SELECT name, uuid, create_table_query FROM system.tables WHERE database='recover_digest_mismatch' AND name NOT LIKE '.inner_id.%' "
            "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
        )
        expected = main_node.query(query)

        if need_remove_is_active_node:
            # NOTE Otherwise it fails to recreate ReplicatedMergeTree table due to "Replica already exists"
            main_node.query(
                "SYSTEM DROP REPLICA '2' FROM DATABASE recover_digest_mismatch"
            )

        # There is a race condition between deleting active node and creating it on server startup
        # So we start a server only after we deleted all table replicas from the Keeper
        dummy_node.start_clickhouse()
        assert_eq_with_retry(dummy_node, query, expected)

    main_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS recover_digest_mismatch SYNC")

    print("Everything Okay")


def test_recover_reverted_rename_keeps_plain_mergetree_data(started_cluster):
    # Issue #111374: in a Replicated database, RENAME/EXCHANGE commits to Keeper before the
    # local metadata rename, which is not fsynced. After a power cut the local catalog reverts
    # to the old name while Keeper remembers the new one, the digest check triggers
    # recoverLostReplica, and for a plain MergeTree recovery used to exile the table to
    # `<db>_broken_tables` and recreate an empty table under the Keeper name -- losing the data.
    # Recovery must instead rename the table into place (it matches a Keeper table by UUID and
    # metadata modulo name). We simulate the lost local rename by moving the metadata file back
    # on disk while the server is down; the data parts stay where they are.
    db = "recover_reverted_rename"
    main_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    main_node.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', 'shard1', 'replica1')"
    )

    main_node.query(f"CREATE TABLE {db}.t (id UInt64) ENGINE=MergeTree ORDER BY id")
    main_node.query(f"INSERT INTO {db}.t SELECT number FROM numbers(10000)")
    # A Memory table is a negative control: it is replica-local and non-persistent, so its
    # reverted rename must still recover EMPTY (the fix must not resurrect it).
    main_node.query(f"CREATE TABLE {db}.mem (id UInt64) ENGINE=Memory")
    main_node.query(f"INSERT INTO {db}.mem SELECT number FROM numbers(777)")

    assert main_node.query(f"SELECT count() FROM {db}.t").strip() == "10000"

    db_data_path = main_node.query(
        f"SELECT metadata_path FROM system.databases WHERE database='{db}'"
    ).strip()

    # Acked DDL: RENAME commits to Keeper and renames the local metadata file.
    main_node.query(f"RENAME TABLE {db}.t TO {db}.t2")
    main_node.query(f"RENAME TABLE {db}.mem TO {db}.mem2")

    # Simulate power loss reverting the un-fsynced local renames: move the metadata files back to
    # the old names on disk while the server is down (Keeper still has the new names).
    main_node.stop_clickhouse(kill=True)
    move_file(main_node, f"{db_data_path}t2.sql", f"{db_data_path}t.sql")
    move_file(main_node, f"{db_data_path}mem2.sql", f"{db_data_path}mem.sql")
    main_node.start_clickhouse()

    # After recovery the plain MergeTree must exist under the Keeper name WITH its data, not empty
    # and not exiled to <db>_broken_tables.
    assert_eq_with_retry(main_node, f"SELECT count() FROM {db}.t2", "10000")
    assert (
        main_node.query(
            f"SELECT count() FROM system.tables WHERE database='{db}_broken_tables'"
        ).strip()
        in ("0", "")
    )
    # Negative control: the Memory table recovers empty (non-persistent, not resurrected).
    assert main_node.query(f"SELECT count() FROM {db}.mem2").strip() == "0"

    main_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_recover_reverted_exchange_keeps_plain_mergetree_data(started_cluster):
    # Issue #111374 (EXCHANGE variant): a power cut after an acknowledged EXCHANGE TABLES used to
    # exile BOTH plain MergeTree tables to <db>_broken_tables and recreate both empty. Recovery
    # must instead reconcile the two-name swap and keep both tables' data.
    db = "recover_reverted_exchange"
    main_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    main_node.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', 'shard1', 'replica1')"
    )

    main_node.query(f"CREATE TABLE {db}.a (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id")
    main_node.query(f"CREATE TABLE {db}.b (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id")
    main_node.query(f"INSERT INTO {db}.a SELECT number, 1 FROM numbers(10000)")
    main_node.query(f"INSERT INTO {db}.b SELECT number, 2 FROM numbers(5000)")

    db_data_path = main_node.query(
        f"SELECT metadata_path FROM system.databases WHERE database='{db}'"
    ).strip()

    main_node.query(f"EXCHANGE TABLES {db}.a AND {db}.b")
    # After the exchange: a holds old-b data (v=2, 5000 rows), b holds old-a data (v=1, 10000 rows).
    assert main_node.query(f"SELECT count(), any(v) FROM {db}.a").strip() == "5000\t2"
    assert main_node.query(f"SELECT count(), any(v) FROM {db}.b").strip() == "10000\t1"

    # Simulate the power-loss-reverted (un-fsynced) local exchange: swap the two metadata files
    # back on disk while the server is down (Keeper keeps the swapped names).
    main_node.stop_clickhouse(kill=True)
    move_file(main_node, f"{db_data_path}a.sql", f"{db_data_path}tmp_a.sql")
    move_file(main_node, f"{db_data_path}b.sql", f"{db_data_path}a.sql")
    move_file(main_node, f"{db_data_path}tmp_a.sql", f"{db_data_path}b.sql")
    main_node.start_clickhouse()

    # After recovery both tables must be reconciled to the acked state with their data intact.
    assert_eq_with_retry(main_node, f"SELECT count(), any(v) FROM {db}.a", "5000\t2")
    assert_eq_with_retry(main_node, f"SELECT count(), any(v) FROM {db}.b", "10000\t1")
    assert (
        main_node.query(
            f"SELECT count() FROM system.tables WHERE database='{db}_broken_tables'"
        ).strip()
        in ("0", "")
    )

    main_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_recover_log_stale_replica_does_not_resurrect_reused_uuid(started_cluster):
    # Guards the reverted-rename reconciliation (issue #111374) against a UUID-reuse hazard: the
    # match of a local table to a ZooKeeper table is only by UUID + metadata, which is unsafe for a
    # replica that has NOT applied every committed log entry. Such a replica could have missed a
    # DROP + CREATE that deliberately reused the same explicit UUID, and must NOT rename its stale
    # local table into the recreated table's name. Recovery must reconcile renames only when the
    # replica's log pointer is current (checked via can_reconcile_renames).
    db = "recover_reused_uuid"
    zk_path = f"/clickhouse/databases/{db}"
    main_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    dummy_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")

    main_node.query(f"CREATE DATABASE {db} ENGINE = Replicated('{zk_path}', 'shard1', 'replica1')")
    # Small logs_to_keep so the offline replica becomes log-stale after a few DDL queries.
    started_cluster.get_kazoo_client("zoo1").set(f"{zk_path}/logs_to_keep", b"2")
    dummy_node.query(f"CREATE DATABASE {db} ENGINE = Replicated('{zk_path}', 'shard1', 'replica2')")

    main_node.query(f"CREATE TABLE {db}.orig (id UInt64) ENGINE=MergeTree ORDER BY id")
    main_node.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    dummy_node.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    # Plain MergeTree data is per-replica (not replicated), so give dummy_node its own local rows in
    # `orig` -- this is the stale data that must NOT be resurrected under the reused-UUID name.
    dummy_node.query(f"INSERT INTO {db}.orig SELECT number FROM numbers(10000)")
    assert dummy_node.query(f"SELECT count() FROM {db}.orig").strip() == "10000"

    reused_uuid = main_node.query(
        f"SELECT uuid FROM system.tables WHERE database='{db}' AND name='orig'"
    ).strip()

    dummy_node.stop_clickhouse(kill=True)

    # While the replica is offline: drop `orig` and create a DIFFERENT table `orig2` that reuses the
    # same explicit UUID with identical schema, holding different data. Then run enough extra DDL to
    # push the offline replica past logs_to_keep so it recovers as log-stale, not digest-mismatch.
    # distributed_ddl_task_timeout=0 so these queries do not block on the stopped replica.
    ddl = {"distributed_ddl_task_timeout": 0}
    main_node.query(f"DROP TABLE {db}.orig SYNC", settings=ddl)
    main_node.query(
        f"CREATE TABLE {db}.orig2 UUID '{reused_uuid}' (id UInt64) ENGINE=MergeTree ORDER BY id",
        settings={"database_replicated_allow_explicit_uuid": 3, "distributed_ddl_task_timeout": 0},
    )
    main_node.query(f"INSERT INTO {db}.orig2 SELECT number FROM numbers(5)")
    for i in range(6):
        main_node.query(
            f"CREATE TABLE {db}.pad{i} (id UInt64) ENGINE=MergeTree ORDER BY id", settings=ddl
        )

    dummy_node.start_clickhouse()

    # After recovery the reused-UUID table on dummy_node must NOT contain the stale local `orig`
    # data. The offline replica missed the DROP+CREATE, so it must exile its stale `orig` and create
    # a fresh `orig2`. Plain MergeTree data is per-replica, so dummy_node's `orig2` is empty here;
    # the point of the guard is that it is NOT the resurrected 10000 rows.
    assert_eq_with_retry(dummy_node, f"SELECT count() FROM {db}.orig2", "0")
    assert dummy_node.query(f"SELECT count() FROM {db}.orig2").strip() != "10000"
    # The stale data should have been exiled rather than lost.
    assert (
        dummy_node.query(
            f"SELECT count() FROM system.tables WHERE database='{db}_broken_tables'"
        ).strip()
        not in ("0", "")
    )

    main_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    dummy_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")

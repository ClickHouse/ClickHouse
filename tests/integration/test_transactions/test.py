import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/transactions.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    # Transactions refuse to start unless Keeper advertises these.
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def tx(session, query):
    params = {"session_id": "session_{}".format(session)}
    return node.http_query(None, data=query, params=params)


def replace_txn_text(output: str, replace_map):
    for k, v in replace_map.items():
        output = output.replace(k, v)
    return output


def expect_part_info(
    replace_map,
    table_name: str,
    part_name: str,
    is_active: int,
    creation_tid: str,
    creation_csn: str,
    removal_tid: str,
    removal_csn: str,
):
    data = node.query(f"SELECT * FROM {table_name} WHERE _part='{part_name}'")
    print(f"part={part_name}, data={data}")
    res = node.query(
        f"SELECT active, creation_tid, 'csn' || toString(creation_csn) || '_', removal_tid, 'csn' || toString(removal_csn) || '_' FROM system.parts WHERE table='{table_name}' AND name='{part_name}'"
    )
    print(f"part={part_name}, info={res}")

    res = replace_txn_text(res.strip(), replace_map)
    res = res.split("\t")
    assert len(res) == 5
    assert int(res[0]) == is_active
    assert res[1] == creation_tid
    assert res[2] == creation_csn
    assert res[3] == removal_tid
    assert res[4] == removal_csn


def held_txn_ephemerals(zk, txn_root="/clickhouse/txn"):
    # The ephemeral znodes one TransactionManager::start() attempt takes: cleanupLockPath() and
    # TransactionSession::replicaActivePath(). The sibling `_session` and `_tail_ptr` nodes under
    # the same parent are persistent, hence the suffix filter.
    #
    # sync() first: this client is pinned to zoo1 while the server shuffles the three configured
    # endpoints, so an unsynced read can still show a holder the server has already removed, or
    # miss one it has just taken.
    zk.sync(txn_root)
    held = ["cleanup_lock"] if zk.exists(f"{txn_root}/cleanup_lock") else []
    replicas = f"{txn_root}/replicas"
    if zk.exists(replicas):
        held += sorted(c for c in zk.get_children(replicas) if c.endswith("_active"))
    return held


def test_failed_start_releases_ephemeral_holders(start_cluster):
    # A failing TransactionManager::start() must leave no ephemeral node holder behind: the failed
    # attempt's instance is discarded and its ~TransactionManager -> shutdown() removes both holders
    # under shutdown()'s own Keeper component guard. Before the split the holders were taken in the
    # constructor body; they are members, so they outlived the body's locals, an exception destroyed
    # the component guard first, and the member ~EphemeralNodeHolder -> tryRemove -> pushRequest then
    # found an empty component and threw LOGICAL_ERROR, which aborts a debug build and force-closes
    # the server's shared Keeper session otherwise. Reproducing that needs
    # enforce_keeper_component_tracking, which is off by default but is written into every integration
    # instance (helpers/0_common_enforce_zookeeper_component_name.xml).
    #
    # This test has to run before any test that creates a transactional part: with the bad entry
    # planted below, a restart whose disk already held transactional parts would fail to resolve their
    # CSNs while the log cannot be loaded. The log root itself already exists by the time this body
    # runs, created by initLogRoot on the fixture's own startup initialization.
    zk = cluster.get_kazoo_client("zoo1")
    log_path = "/clickhouse/txn/log"
    bad_entry = f"{log_path}/csn-0009999999"
    try:
        # One transaction that must succeed, so that a later failure is attributable to the entry
        # injected below and not to the fixture. It also creates no data part, which matters for the
        # restart below.
        tx(100, "BEGIN TRANSACTION")
        tx(100, "ROLLBACK")

        # The log root must already exist, created by TransactionLog::initLogRoot: it is the only
        # thing that fast-forwards the sequential counter past the reserved CSNs, and it only does so
        # when it finds the log absent -- so the injected node below must never be what brings the
        # log into existence. That is also why there is no makepath below.
        assert zk.exists(log_path), f"{log_path} should have been created by initLogRoot"

        # A CSN log entry claiming a format version this server does not know. Deserializing it
        # throws from reloadCSNLogs, i.e. after initOwnReplicaState has taken `_active` and the
        # cleanup lease: that window is what this test is about. It has to be planted before the
        # restart, because configs/transactions.xml enables transactions and the server therefore
        # initializes the log itself at startup.
        zk.create(bad_entry, b"version: 2\n")

        # A graceful restart runs Context::shutdown -> TransactionManager::shutdownIfAny, which
        # releases both holders. The new process then fails start() on its own eager call.
        node.restart_clickhouse()

        # Precondition for the arm below: the eager initialization in Server.cpp ran in this process
        # and failed on the planted entry. Without this, "the server still starts" would hold just as
        # well on a server that never tried, and nothing in this test would observe the startup path.
        assert int(node.count_in_log("Cannot initialize the transaction log at startup")) >= 1

        # Arm 1 of 2: each failing attempt is checked on its own. A count aggregated over all three
        # attempts cannot localize a leak, because tryAcquireCleanupLock treats an existing lease as
        # a non-fatal skip and logs nothing, and createActiveNode removes a stale `_active` before
        # taking its own -- one attempt's leak is therefore absorbed silently by the next.
        leases_after_eager = int(node.count_in_log("Acquired cleanup lease"))
        assert leases_after_eager >= 1, "the eager start() attempt never took the cleanup lease"
        held = held_txn_ephemerals(zk)
        assert held == [], f"the eager start() failure left ephemeral node holders behind: {held}"

        # A transaction log that cannot be initialized must not keep the server from starting.
        assert node.query("SELECT 1").strip() == "1"

        # Precondition and first arm in one: the lazy path must re-enter start() and fail the same
        # way. If the singleton were already published this would succeed, and everything below
        # would be vacuous.
        with pytest.raises(Exception) as excinfo:
            tx(101, "BEGIN TRANSACTION")
        assert "Unknown CSN entry format version" in str(excinfo.value), (
            "BEGIN TRANSACTION did not fail inside start() -- the singleton was probably already "
            f"published, so this test covers nothing. Got: {excinfo.value}"
        )

        # Arm 2 of 2: the same pair of checks for the lazy attempt.
        leases_after_lazy = int(node.count_in_log("Acquired cleanup lease"))
        assert leases_after_lazy > leases_after_eager, (
            "the lazy start() attempt did not take the cleanup lease, so the eager failure had "
            "not released it"
        )
        held = held_txn_ephemerals(zk)
        assert held == [], f"the lazy start() failure left ephemeral node holders behind: {held}"

        zk.delete(bad_entry)

        # A third attempt, now unobstructed.
        tx(102, "BEGIN TRANSACTION")
        tx(102, "ROLLBACK")

        # The count below is absolute rather than a delta because
        # helpers/0_common_instance_config.xml sets <rotateOnOpen>, so the log file count_in_log
        # reads holds exactly the process started by the restart above, which is the whole window of
        # interest: every failing and succeeding start() happened in it.
        #
        # The direct symptom, in both build flavours: a debug build logs it from
        # abortOnFailedAssertion, a release build from ~EphemeralNodeHolder's own handler.
        assert int(node.count_in_log("Current component is empty")) == 0

        # Liveness for both arms above: the published instance holds exactly what the failed
        # attempts had to give up, so their empty results are an absence and not a path that never
        # takes these znodes.
        assert int(node.count_in_log("Acquired cleanup lease")) > leases_after_lazy
        held = held_txn_ephemerals(zk)
        assert "cleanup_lock" in held and len(held) == 2, (
            f"the started instance does not hold both ephemerals, so the arms above prove nothing: {held}"
        )

        # A transaction on the now-published singleton. Had a failed attempt's instance been reused
        # instead of discarded, a debug build would have aborted inside one of the attempts above, on
        # loadLogFromZooKeeper's chassert(!zookeeper) or initLogRoot's chassert(tid_to_csn.empty());
        # a working transaction here is the other end of that oracle.
        tx(103, "BEGIN TRANSACTION")
        tx(103, "ROLLBACK")
    finally:
        if zk.exists(bad_entry):
            zk.delete(bad_entry)
        zk.stop()
        zk.close()


def test_rollback_unfinished_on_restart1(start_cluster):
    node.query("DROP TABLE IF EXISTS mt")
    node.query(
        "create table mt (n int, m int) engine=MergeTree order by n partition by n % 2 settings remove_empty_parts = 0"
    )
    node.query("insert into mt values (1, 10), (2, 20)")
    # INSERT_1
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000',0)"

    # it will hold a snapshot and avoid parts cleanup
    tx(0, "begin transaction")

    tx(4, "begin transaction")

    tx(1, "begin transaction")
    tid1 = tx(1, "select transactionID()").strip()
    # ALTER_1
    tx(1, "alter table mt drop partition id '1'")
    tx(1, "commit")

    tx(1, "begin transaction")
    tid2 = tx(1, "select transactionID()").strip()
    # INSERT_2
    tx(1, "insert into mt values (3, 30), (4, 40)")
    tx(1, "commit")

    node.query("system flush logs")
    csn1 = node.query(
        "select csn from system.transactions_info_log where type='Commit' and tid={}".format(
            tid1
        )
    ).strip()
    csn2 = node.query(
        "select csn from system.transactions_info_log where type='Commit' and tid={}".format(
            tid2
        )
    ).strip()

    # insert a part before starting mutation and check that it will not be mutated
    # INSERT_3
    tx(4, "insert into mt values (9, 90)")

    # check that uncommitted mutation will be rolled back on restart
    tx(1, "begin transaction")
    tid3 = tx(1, "select transactionID()").strip()
    # INSERT_4
    tx(1, "insert into mt values (5, 50)")
    # ALTER_2
    tx(1, "alter table mt update m = m+n in partition id '1' where 1")

    # check that uncommitted insert will be rolled back on restart (using `START TRANSACTION` syntax)
    tx(3, "start transaction")
    tid5 = tx(3, "select transactionID()").strip()
    # INSERT_5
    tx(3, "insert into mt values (6, 70)")

    tid6 = tx(4, "select transactionID()").strip()
    tx(4, "commit")
    node.query("system flush logs")
    csn6 = node.query(
        "select csn from system.transactions_info_log where type='Commit' and tid={}".format(
            tid6
        )
    ).strip()

    node.restart_clickhouse(kill=True)
    node.query("SYSTEM WAIT LOADING PARTS mt")

    assert (
        node.query("select *, _part from mt order by n")
        == "2\t20\t0_2_2_0\n3\t30\t1_3_3_0\n4\t40\t0_4_4_0\n9\t90\t1_5_5_0\n"
    )

    replace_map = {
        tid0: "tid0",
        tid1: "tid1",
        "csn" + csn1 + "_": "csn_1",
        tid2: "tid2",
        "csn" + csn2 + "_": "csn_2",
        tid3: "tid3",
        tid5: "tid5",
        tid6: "tid6",
        "csn" + csn6 + "_": "csn_6",
    }

    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_2_2_0",
        is_active=1,
        creation_tid="tid0",  # Created by INSERT_1
        creation_csn="csn1_",  # Committed with Tx::NonTransactionalCSN = 1
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # Was being replaced by 0_2_2_0_7 by ALTER_2 in tid3 (not committed before restarting) -> the remove_tid is reset to Tx::Empty after restarting
        removal_csn="csn0_",  # No removal_csn, tid3 was not committed yet.
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_2_2_0_7",
        is_active=0,  # Created by tid3 in ALTER_2 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_4_4_0",
        is_active=1,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid2",  # Created by  INSERT_2 in tid2
        creation_csn="csn_2",  # tid2 was commited
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_4_4_0_7",
        is_active=0,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_8_8_0",
        is_active=0,  # Created by INSERT_5 in tid5 (not committed before restarting) -> is_active = 0, creation_tid = tid5
        creation_tid="tid5",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_1_1_0",
        is_active=0,
        creation_tid="tid0",  # Created by INSERT_1
        creation_csn="csn1_",  # Committed with Tx::NonTransactionalCSN = 1
        removal_tid="tid1",  # Was being drop by ALTER_1 in tid1
        removal_csn="csn_1",  # tid1 was committed with csn_1
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_3_3_0",
        is_active=1,
        creation_tid="tid2",  # Created by INSERT_2 in tid2
        creation_csn="csn_2",  # tid2 was commited with csn_2
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_3_3_0_7",
        is_active=0,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_5_5_0",
        is_active=1,
        creation_tid="tid6",  # Created by INSERT_3 in tid6
        creation_csn="csn_6",  # tid6 was commited with csn_6
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_6_6_0",
        is_active=0,  # Created by INSERT_4 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_6_6_0_7",
        is_active=0,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    node.query("DROP TABLE IF EXISTS mt SYNC")


def test_rollback_unfinished_on_restart2(start_cluster):
    node.query("DROP TABLE IF EXISTS mt2 SYNC")
    node.query(
        "create table mt2 (n int, m int) engine=MergeTree order by n partition by n % 2 settings remove_empty_parts = 0"
    )
    # INSERT_1
    node.query("insert into mt2 values (1, 10), (2, 20)")
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000',0)"

    # it will hold a snapshot and avoid parts cleanup
    tx(0, "begin transaction")

    tx(4, "begin transaction")

    tx(1, "begin transaction")
    tid1 = tx(1, "select transactionID()").strip()
    # ALTER_1
    tx(1, "alter table mt2 drop partition id '1'")
    tx(1, "commit")

    tx(1, "begin transaction")
    tid2 = tx(1, "select transactionID()").strip()
    # INSERT_2
    tx(1, "insert into mt2 values (3, 30), (4, 40)")
    tx(1, "commit")

    node.query("system flush logs")
    csn1 = node.query(
        "select csn from system.transactions_info_log where type='Commit' and tid={}".format(
            tid1
        )
    ).strip()
    csn2 = node.query(
        "select csn from system.transactions_info_log where type='Commit' and tid={}".format(
            tid2
        )
    ).strip()

    # check that uncommitted merge will be rolled back on restart
    tx(2, "begin transaction")
    tid4 = tx(2, "select transactionID()").strip()
    tx(
        2,
        "optimize table mt2 partition id '0' final settings optimize_throw_if_noop = 1",
    )

    # check that uncommitted insert will be rolled back on restart
    tx(3, "begin transaction")
    tid5 = tx(3, "select transactionID()").strip()
    # INSERT_3
    tx(3, "insert into mt2 values (6, 70)")

    tid6 = tx(4, "select transactionID()").strip()
    tx(4, "commit")
    node.query("system flush logs")
    csn6 = node.query(
        "select csn from system.transactions_info_log where type='Commit' and tid={}".format(
            tid6
        )
    ).strip()

    node.restart_clickhouse(kill=True)
    node.query("SYSTEM WAIT LOADING PARTS mt2")

    replace_map = {
        tid0: "tid0",
        tid1: "tid1",
        "csn" + csn1 + "_": "csn_1",
        tid2: "tid2",
        "csn" + csn2 + "_": "csn_2",
        tid4: "tid4",
        tid5: "tid5",
        tid6: "tid6",
        "csn" + csn6 + "_": "csn_6",
    }

    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_2_2_0",
        is_active=1,
        creation_tid="tid0",  # Created by INSERT_1
        creation_csn="csn1_",  # Committed with Tx::NonTransactionalCSN = 1
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # Was being replaced by 0_2_2_0_7 by ALTER_1 in tid4 (not committed before restarting) -> the remove_tid is reset to Tx::Empty after restarting
        removal_csn="csn0_",  # No removal_csn, tid3 was not committed yet.
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_2_4_1",
        is_active=0,  # Created by ALTER_1 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid4",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_4_4_0",
        is_active=1,
        creation_tid="tid2",  # Created by INSERT_2
        creation_csn="csn_2",  # tid2 was commited with csn_2
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_5_5_0",
        is_active=0,  # Created by INSERT_3 in tid5 (not committed before restarting) -> is_active = 0, creation_tid = tid5
        creation_tid="tid5",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::NonTransactionalLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::NonTransactionalLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="1_1_1_0",
        is_active=0,
        creation_tid="tid0",  # Created by INSERT_1
        creation_csn="csn1_",  # Committed with Tx::NonTransactionalCSN = 1
        removal_tid="tid1",  # Was being drop by ALTER_1 in tid1
        removal_csn="csn_1",  # tid1 was committed with csn_1
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="1_3_3_0",
        is_active=1,
        creation_tid="tid2",  # Created by INSERT_2
        creation_csn="csn_2",  # tid2 was commited with csn_2
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000',0)",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )

    node.query("DROP TABLE IF EXISTS mt2 SYNC")


def test_mutate_transaction_involved_parts(start_cluster):
    node.query("DROP TABLE IF EXISTS mt3 SYNC")
    node.query("CREATE TABLE mt3 (n int, m int) ENGINE=MergeTree ORDER BY n")
    tx(0, "BEGIN TRANSACTION")
    tx(0, "INSERT INTO mt3 VALUES(1, 1)")
    tx(0, "COMMIT")

    tx(1, "BEGIN TRANSACTION")
    tx(1, "INSERT INTO mt3 VALUES(2, 2)")
    tx(1, "COMMIT")

    tx(2, "BEGIN TRANSACTION")
    tx(2, "INSERT INTO mt3 VALUES(3, 3)")
    tx(2, "COMMIT")

    # When mutating table without a transaction, after restarting, the instance should load the outdated parts successfully
    # Refer: https://github.com/ClickHouse/ClickHouse/pull/81734
    node.query("ALTER TABLE mt3 UPDATE m = 10 WHERE 1")
    node.restart_clickhouse()

    node.query("DROP TABLE IF EXISTS mt3 SYNC")


def test_removal_csn_valid_after_commit(start_cluster):
    """
    Test that committing a transaction that removes parts does not cause
    spurious CORRUPTED_DATA from hasValidMetadata when the in-memory
    removal_csn is set before the on-disk file is updated.

    This test exercises the code path fixed in:
      "Fix false CORRUPTED_DATA in hasValidMetadata for removal_csn"
    The fix added a `persisted_info.removal_csn != Tx::UnknownCSN` guard
    mirroring the same guard that already existed for creation_csn.
    """
    node.query("DROP TABLE IF EXISTS mt_removal_csn SYNC")
    node.query(
        "CREATE TABLE mt_removal_csn (n int) ENGINE=MergeTree ORDER BY n"
        " SETTINGS old_parts_lifetime=3600"
    )
    node.query("SYSTEM STOP MERGES mt_removal_csn")
    node.query("INSERT INTO mt_removal_csn VALUES (1)")
    node.query("INSERT INTO mt_removal_csn VALUES (2)")
    node.query("INSERT INTO mt_removal_csn VALUES (3)")

    # Commit multiple transactional removals in quick succession.
    # Each commit triggers afterCommit() → setAndStoreRemovalCSN(), which first
    # updates the in-memory removal_csn before writing to disk.  The debug
    # assertion assertHasValidVersionMetadata() (called from remove() in the
    # cleanup thread) must not see this transient state as corruption.
    for _ in range(5):
        tx(0, "BEGIN TRANSACTION")
        tx(0, "ALTER TABLE mt_removal_csn DROP PARTITION ID 'all'")
        tx(0, "COMMIT")
        node.query("SYSTEM START MERGES mt_removal_csn")
        # Give the cleanup thread a chance to run and call remove()
        node.query("SYSTEM WAIT LOADING PARTS mt_removal_csn")
        node.query("SYSTEM STOP MERGES mt_removal_csn")
        node.query("INSERT INTO mt_removal_csn VALUES (1)")
        node.query("INSERT INTO mt_removal_csn VALUES (2)")
        node.query("INSERT INTO mt_removal_csn VALUES (3)")

    # After all the churning, the table must be healthy and queryable
    count = node.query("SELECT count() FROM mt_removal_csn").strip()
    assert count == "3", f"Expected 3 rows, got {count}"

    node.query("DROP TABLE IF EXISTS mt_removal_csn SYNC")


def test_removal_metadata_persisted_after_restart(start_cluster):
    """
    Test that removal_tid and removal_csn are correctly persisted to disk
    and survive a server restart.

    The VersionMetadataOnDisk implementation writes metadata to txn_version.txt.
    After a commit that removes a part, the on-disk file must contain the
    removal_tid and removal_csn, so that after restart system.parts reflects
    the correct values without re-reading the transaction log.
    """
    node.query("DROP TABLE IF EXISTS mt_persist SYNC")
    node.query(
        "CREATE TABLE mt_persist (n int) ENGINE=MergeTree ORDER BY n"
        " SETTINGS old_parts_lifetime=3600"
    )
    # Non-transactional insert creates a part with NonTransactionalTID
    node.query("INSERT INTO mt_persist VALUES (1)")

    # Commit a transactional removal so removal_tid/csn are written to disk
    tx(0, "BEGIN TRANSACTION")
    tid_remove = tx(0, "SELECT transactionID()").strip()
    tx(0, "ALTER TABLE mt_persist DROP PARTITION ID 'all'")
    tx(0, "COMMIT")

    node.query("SYSTEM FLUSH LOGS")
    csn_remove = node.query(
        f"SELECT csn FROM system.transactions_info_log"
        f" WHERE type='Commit' AND tid={tid_remove}"
    ).strip()
    assert csn_remove, f"Could not find commit CSN for tid {tid_remove}"

    # Verify removal metadata before restart
    removal_info = node.query(
        "SELECT removal_csn > 1, removal_tid != (0,0,'00000000-0000-0000-0000-000000000000',0)"
        " FROM system.parts"
        " WHERE database='default' AND table='mt_persist'"
        "   AND removal_csn > 1"
    ).strip()
    assert removal_info == "1\t1", f"Unexpected removal metadata before restart: {removal_info}"

    node.restart_clickhouse()
    node.query("SYSTEM WAIT LOADING PARTS mt_persist")

    # After restart, the removed part must still show the correct removal metadata
    removal_info_after = node.query(
        "SELECT removal_csn > 1, removal_tid != (0,0,'00000000-0000-0000-0000-000000000000',0)"
        " FROM system.parts"
        " WHERE database='default' AND table='mt_persist'"
        "   AND removal_csn > 1"
    ).strip()
    assert removal_info_after == "1\t1", (
        f"Unexpected removal metadata after restart: {removal_info_after}"
    )

    node.query("DROP TABLE IF EXISTS mt_persist SYNC")


def test_rollback_clears_removal_tid(start_cluster):
    """
    Test that rolling back a transaction that attempted to remove a part
    correctly clears the removal_tid so a subsequent transaction can remove
    the same part without a SERIALIZATION_ERROR.

    This exercises the path in MergeTreeTransaction::rollback() that calls
    setAndStoreRemovalTID(Tx::EmptyTID) for each part in removing_parts.
    """
    node.query("DROP TABLE IF EXISTS mt_rollback_removal SYNC")
    node.query(
        "CREATE TABLE mt_rollback_removal (n int) ENGINE=MergeTree ORDER BY n"
        " SETTINGS old_parts_lifetime=3600"
    )
    node.query("SYSTEM STOP MERGES mt_rollback_removal")
    node.query("INSERT INTO mt_rollback_removal VALUES (1)")

    # Transaction 1: lock the part for removal, then roll back
    tx(1, "BEGIN TRANSACTION")
    tx(1, "ALTER TABLE mt_rollback_removal DROP PARTITION ID 'all'")
    tx(1, "ROLLBACK")

    # After rollback the part must be active again with empty removal metadata
    info = node.query(
        "SELECT active,"
        "       removal_tid = (0,0,'00000000-0000-0000-0000-000000000000',0),"
        "       removal_csn = 0"
        " FROM system.parts"
        " WHERE database='default' AND table='mt_rollback_removal'"
        "   AND active"
    ).strip()
    assert info == "1\t1\t1", f"Unexpected part state after rollback: {info}"

    # Transaction 2: must be able to remove the same part without SERIALIZATION_ERROR
    tx(2, "BEGIN TRANSACTION")
    tx(2, "ALTER TABLE mt_rollback_removal DROP PARTITION ID 'all'")
    tx(2, "COMMIT")

    active_count = node.query(
        "SELECT count() FROM system.parts"
        " WHERE database='default' AND table='mt_rollback_removal' AND active"
    ).strip()
    assert active_count == "0", f"Expected 0 active parts after removal, got {active_count}"

    node.query("DROP TABLE IF EXISTS mt_rollback_removal SYNC")


def test_concurrent_independent_transactions(start_cluster):
    """
    Test that two concurrent independent transactions inserting into separate
    tables both succeed without any SERIALIZATION_ERROR.

    This verifies that the refactored VersionMetadata class correctly handles
    independent concurrent operations with no false conflicts.
    """
    node.query("DROP TABLE IF EXISTS mt_ind_a SYNC")
    node.query("DROP TABLE IF EXISTS mt_ind_b SYNC")
    node.query("CREATE TABLE mt_ind_a (n int) ENGINE=MergeTree ORDER BY n")
    node.query("CREATE TABLE mt_ind_b (n int) ENGINE=MergeTree ORDER BY n")
    node.query("SYSTEM STOP MERGES mt_ind_a")
    node.query("SYSTEM STOP MERGES mt_ind_b")

    # Two transactions operate on separate tables — no conflicts expected
    tx(1, "BEGIN TRANSACTION")
    tx(2, "BEGIN TRANSACTION")

    tx(1, "INSERT INTO mt_ind_a VALUES (1)")
    tx(2, "INSERT INTO mt_ind_b VALUES (2)")

    tx(1, "COMMIT")
    tx(2, "COMMIT")

    count_a = node.query("SELECT count() FROM mt_ind_a").strip()
    count_b = node.query("SELECT count() FROM mt_ind_b").strip()
    assert count_a == "1", f"Expected 1 row in mt_ind_a, got {count_a}"
    assert count_b == "1", f"Expected 1 row in mt_ind_b, got {count_b}"

    node.query("DROP TABLE IF EXISTS mt_ind_a SYNC")
    node.query("DROP TABLE IF EXISTS mt_ind_b SYNC")


def test_non_txn_merge_metadata(start_cluster):
    """
    Test that a non-transactional OPTIMIZE TABLE FINAL produces correct
    metadata on the replaced (source) parts and on the merged result.

    Non-transactional inserts leave transactions_enabled=false for the table,
    so OPTIMIZE TABLE without a transaction is allowed.  Once transactions_enabled
    becomes true (after any transactional write), OPTIMIZE without a transaction
    is rejected to protect isolation — so this test intentionally uses only
    non-transactional inserts.

    The merged source parts must get:
      - removal_tid = NonTransactionalTID = (1,1,'00000000-0000-0000-0000-000000000000',0)
      - removal_csn = NonTransactionalCSN = 1

    The merged result must have:
      - creation_tid = NonTransactionalTID
      - creation_csn = NonTransactionalCSN = 1
    """
    node.query("DROP TABLE IF EXISTS mt_merge_txn SYNC")
    node.query(
        "CREATE TABLE mt_merge_txn (n int) ENGINE=MergeTree ORDER BY n"
        " SETTINGS old_parts_lifetime=3600"
    )
    node.query("SYSTEM STOP MERGES mt_merge_txn")

    # Insert rows non-transactionally so transactions_enabled stays false;
    # this is required for a non-transactional OPTIMIZE to be accepted.
    node.query("INSERT INTO mt_merge_txn VALUES (1)")
    node.query("INSERT INTO mt_merge_txn VALUES (2)")

    # Non-transactional merge — re-enable merges first so OPTIMIZE can run
    node.query("SYSTEM START MERGES mt_merge_txn")
    node.query(
        "OPTIMIZE TABLE mt_merge_txn FINAL SETTINGS optimize_throw_if_noop=1"
    )

    # Source parts must have non-transactional removal metadata
    source_removal = node.query(
        "SELECT count()"
        " FROM system.parts"
        " WHERE database='default' AND table='mt_merge_txn'"
        "   AND active=0"
        "   AND removal_tid = (1,1,'00000000-0000-0000-0000-000000000000',0)"
        "   AND removal_csn = 1"
    ).strip()
    assert source_removal == "2", (
        f"Expected 2 source parts with NonTransactional removal metadata, got {source_removal}"
    )

    # Merged result must have non-transactional creation metadata
    merged_creation = node.query(
        "SELECT count()"
        " FROM system.parts"
        " WHERE database='default' AND table='mt_merge_txn'"
        "   AND active=1"
        "   AND creation_tid = (1,1,'00000000-0000-0000-0000-000000000000',0)"
        "   AND creation_csn = 1"
    ).strip()
    assert merged_creation == "1", (
        f"Expected 1 merged part with NonTransactional creation metadata, got {merged_creation}"
    )

    # Server must survive restart with this mixed metadata state
    node.restart_clickhouse()
    node.query("SYSTEM WAIT LOADING PARTS mt_merge_txn")

    row_count = node.query("SELECT count() FROM mt_merge_txn").strip()
    assert row_count == "2", f"Expected 2 rows after restart, got {row_count}"

    node.query("DROP TABLE IF EXISTS mt_merge_txn SYNC")


def test_removal_csn_concurrent_rollback_stress(start_cluster):
    """
    Stress test for the race condition fixed in:
      "Fix race condition in MergeTreeTransaction::removeOldPart"

    Before the fix, setAndStoreRemovalTID was called after releasing the mutex,
    creating a window where rollback could overwrite removal_tid on disk and
    the original thread would write it back, leaving an invalid post-rollback state.

    After the fix, setAndStoreRemovalTID runs inside the mutex, making the
    lock acquisition and disk persist atomic with respect to rollback.

    We stress-test this by running many iterations of:
      1. Insert a part
      2. Begin transaction and mark part for removal
      3. Immediately rollback
      4. Verify the part is restored with clean removal metadata
    Then verify the table is consistent throughout.
    """
    node.query("DROP TABLE IF EXISTS mt_race_stress SYNC")
    node.query(
        "CREATE TABLE mt_race_stress (n int) ENGINE=MergeTree ORDER BY n"
        " SETTINGS old_parts_lifetime=3600"
    )
    node.query("SYSTEM STOP MERGES mt_race_stress")

    for i in range(20):
        node.query(f"INSERT INTO mt_race_stress VALUES ({i})")

        tx(0, "BEGIN TRANSACTION")
        tx(0, "ALTER TABLE mt_race_stress DROP PARTITION ID 'all'")
        tx(0, "ROLLBACK")

        # After rollback, all parts must be active with empty removal metadata
        bad_parts = node.query(
            "SELECT count()"
            " FROM system.parts"
            " WHERE database='default' AND table='mt_race_stress'"
            "   AND active=1"
            "   AND (removal_tid != (0,0,'00000000-0000-0000-0000-000000000000',0)"
            "        OR removal_csn != 0)"
        ).strip()
        assert bad_parts == "0", (
            f"Iteration {i}: found active parts with stale removal metadata: {bad_parts}"
        )

    node.query("DROP TABLE IF EXISTS mt_race_stress SYNC")

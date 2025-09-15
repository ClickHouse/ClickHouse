import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/transactions.xml"],
    stay_alive=True,
    with_zookeeper=True,
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


def test_rollback_unfinished_on_restart1(start_cluster):
    node.query("DROP TABLE IF EXISTS mt")
    node.query(
        "create table mt (n int, m int) engine=MergeTree order by n partition by n % 2 settings remove_empty_parts = 0"
    )
    node.query("insert into mt values (1, 10), (2, 20)")
    # INSERT_1
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000')"

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
        creation_csn="csn1_",  # Committed with Tx::PrehistoricCSN = 1
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # Was being replaced by 0_2_2_0_7 by ALTER_2 in tid3 (not committed before restarting) -> the remove_tid is reset to Tx::Empty after restarting
        removal_csn="csn0_",  # No removal_csn, tid3 was not committed yet.
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_2_2_0_7",
        is_active=0,  # Created by tid3 in ALTER_2 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_4_4_0",
        is_active=1,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid2",  # Created by  INSERT_2 in tid2
        creation_csn="csn_2",  # tid2 was commited
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_4_4_0_7",
        is_active=0,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="0_8_8_0",
        is_active=0,  # Created by INSERT_5 in tid5 (not committed before restarting) -> is_active = 0, creation_tid = tid5
        creation_tid="tid5",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_1_1_0",
        is_active=0,
        creation_tid="tid0",  # Created by INSERT_1
        creation_csn="csn1_",  # Committed with Tx::PrehistoricCSN = 1
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
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_3_3_0_7",
        is_active=0,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_5_5_0",
        is_active=1,
        creation_tid="tid6",  # Created by INSERT_3 in tid6
        creation_csn="csn_6",  # tid6 was commited with csn_6
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_6_6_0",
        is_active=0,  # Created by INSERT_4 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt",
        part_name="1_6_6_0_7",
        is_active=0,  # Created by ALTER_2 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid3",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
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
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000')"

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
        creation_csn="csn1_",  # Committed with Tx::PrehistoricCSN = 1
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # Was being replaced by 0_2_2_0_7 by ALTER_1 in tid4 (not committed before restarting) -> the remove_tid is reset to Tx::Empty after restarting
        removal_csn="csn0_",  # No removal_csn, tid3 was not committed yet.
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_2_4_1",
        is_active=0,  # Created by ALTER_1 in tid3 (not committed before restarting) -> is_active = 0, creation_tid = tid3
        creation_tid="tid4",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_4_4_0",
        is_active=1,
        creation_tid="tid2",  # Created by INSERT_2
        creation_csn="csn_2",  # tid2 was commited with csn_2
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="0_5_5_0",
        is_active=0,  # Created by INSERT_3 in tid5 (not committed before restarting) -> is_active = 0, creation_tid = tid5
        creation_tid="tid5",
        creation_csn="csn18446744073709551615_",  # creation_csn is reset to Tx::RolledBackCSN = 18446744073709551615 after restarting
        # In the previous test version, the `removal_tid` is Tx::PrehistoricLocalTID (tid0).
        # Because in `MergeTreeData::preparePartForRemoval`, it sets the removal lock to `Tx::PrehistoricLocalTID``, and `removal_tid` is updated accordingly.
        # In this version, `removal_tid` is not updated when it locks the object for removal.
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
        removal_csn="csn0_",  # No transaction attempted to remove this part
    )
    expect_part_info(
        replace_map=replace_map,
        table_name="mt2",
        part_name="1_1_1_0",
        is_active=0,
        creation_tid="tid0",  # Created by INSERT_1
        creation_csn="csn1_",  # Committed with Tx::PrehistoricCSN = 1
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
        removal_tid="(0,0,'00000000-0000-0000-0000-000000000000')",  # No transaction attempted to remove this part
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

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


def test_rollback_unfinished_on_restart1(start_cluster):
    node.query(
        "create table mt (n int, m int) engine=MergeTree order by n partition by n % 2 settings remove_empty_parts = 0"
    )
    node.query("insert into mt values (1, 10), (2, 20)")
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000')"

    # it will hold a snapshot and avoid parts cleanup
    tx(0, "begin transaction")

    tx(4, "begin transaction")

    tx(1, "begin transaction")
    tid1 = tx(1, "select transactionID()").strip()
    tx(1, "alter table mt drop partition id '1'")
    tx(1, "commit")

    tx(1, "begin transaction")
    tid2 = tx(1, "select transactionID()").strip()
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
    tx(4, "insert into mt values (9, 90)")

    # check that uncommitted mutation will be rolled back on restart
    tx(1, "begin transaction")
    tid3 = tx(1, "select transactionID()").strip()
    tx(1, "insert into mt values (5, 50)")
    tx(1, "alter table mt update m = m+n in partition id '1' where 1")

    # check that uncommitted insert will be rolled back on restart (using `START TRANSACTION` syntax)
    tx(3, "start transaction")
    tid5 = tx(3, "select transactionID()").strip()
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
    res = node.query(
        "select name, active, creation_tid, 'csn' || toString(creation_csn) || '_', removal_tid, 'csn' || toString(removal_csn) || '_' from system.parts where table='mt' order by name"
    )
    res = res.replace(tid0, "tid0")
    res = res.replace(tid1, "tid1").replace("csn" + csn1 + "_", "csn_1")
    res = res.replace(tid2, "tid2").replace("csn" + csn2 + "_", "csn_2")
    res = res.replace(tid3, "tid3")
    res = res.replace(tid5, "tid5")
    res = res.replace(tid6, "tid6").replace("csn" + csn6 + "_", "csn_6")
    assert (
        res
        == "0_2_2_0\t1\ttid0\tcsn1_\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
        "0_2_2_0_7\t0\ttid3\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "0_4_4_0\t1\ttid2\tcsn_2\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
        "0_4_4_0_7\t0\ttid3\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "0_8_8_0\t0\ttid5\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "1_1_1_0\t0\ttid0\tcsn1_\ttid1\tcsn_1\n"
        "1_3_3_0\t1\ttid2\tcsn_2\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
        "1_3_3_0_7\t0\ttid3\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "1_5_5_0\t1\ttid6\tcsn_6\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
        "1_6_6_0\t0\ttid3\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "1_6_6_0_7\t0\ttid3\tcsn18446744073709551615_\ttid0\tcsn0_\n"
    )


def test_rollback_unfinished_on_restart2(start_cluster):
    node.query(
        "create table mt2 (n int, m int) engine=MergeTree order by n partition by n % 2 settings remove_empty_parts = 0"
    )
    node.query("insert into mt2 values (1, 10), (2, 20)")
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000')"

    # it will hold a snapshot and avoid parts cleanup
    tx(0, "begin transaction")

    tx(4, "begin transaction")

    tx(1, "begin transaction")
    tid1 = tx(1, "select transactionID()").strip()
    tx(1, "alter table mt2 drop partition id '1'")
    tx(1, "commit")

    tx(1, "begin transaction")
    tid2 = tx(1, "select transactionID()").strip()
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

    assert (
        node.query("select *, _part from mt2 order by n")
        == "2\t20\t0_2_2_0\n3\t30\t1_3_3_0\n4\t40\t0_4_4_0\n"
    )
    res = node.query(
        "select name, active, creation_tid, 'csn' || toString(creation_csn) || '_', removal_tid, 'csn' || toString(removal_csn) || '_' from system.parts where table='mt2' order by name"
    )
    res = res.replace(tid0, "tid0")
    res = res.replace(tid1, "tid1").replace("csn" + csn1 + "_", "csn_1")
    res = res.replace(tid2, "tid2").replace("csn" + csn2 + "_", "csn_2")
    res = res.replace(tid4, "tid4")
    res = res.replace(tid5, "tid5")
    res = res.replace(tid6, "tid6").replace("csn" + csn6 + "_", "csn_6")
    assert (
        res
        == "0_2_2_0\t1\ttid0\tcsn1_\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
        "0_2_4_1\t0\ttid4\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "0_4_4_0\t1\ttid2\tcsn_2\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
        "0_5_5_0\t0\ttid5\tcsn18446744073709551615_\ttid0\tcsn0_\n"
        "1_1_1_0\t0\ttid0\tcsn1_\ttid1\tcsn_1\n"
        "1_3_3_0\t1\ttid2\tcsn_2\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0_\n"
    )

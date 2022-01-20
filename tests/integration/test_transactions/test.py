import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/transactions.xml"], stay_alive=True, with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def tx(session, query):
    params = {'session_id': 'session_{}'.format(session)}
    return node.http_query(None, data=query, params=params)


def test_rollback_unfinished_on_restart(start_cluster):
    node.query('create table mt (n int, m int) engine=MergeTree order by n partition by n % 2')
    node.query('insert into mt values (1, 10), (2, 20)')
    tid0 = "(1,1,'00000000-0000-0000-0000-000000000000')"

    # it will hold a snapshot and avoid parts cleanup
    tx(0, 'begin transaction')

    tx(1, 'begin transaction')
    tid1 = tx(1, 'select transactionID()').strip()
    tx(1, "alter table mt drop partition id '1'")
    tx(1, 'commit')

    tx(1, 'begin transaction')
    tid2 = tx(1, 'select transactionID()').strip()
    tx(1, 'insert into mt values (3, 30), (4, 40)')
    tx(1, 'commit')

    node.query('system flush logs')
    csn1 = node.query("select csn from system.transactions_info_log where type='Commit' and tid={}".format(tid1)).strip()
    csn2 = node.query("select csn from system.transactions_info_log where type='Commit' and tid={}".format(tid2)).strip()

    tx(1, 'begin transaction')
    tid3 = tx(1, 'select transactionID()').strip()
    tx(1, 'insert into mt values (5, 50)')
    tx(1, "alter table mt update m = m+n in partition id '1' where 1")

    tx(2, 'begin transaction')
    tid4 = tx(2, 'select transactionID()').strip()
    tx(2, "optimize table mt partition id '0' final settings optimize_throw_if_noop = 1")

    tx(3, 'begin transaction')
    tid5 = tx(3, 'select transactionID()').strip()
    tx(3, 'insert into mt values (6, 70)')

    node.restart_clickhouse(kill=True)

    assert node.query('select *, _part from mt order by n') == '2\t20\t0_2_2_0\n3\t30\t1_3_3_0\n4\t40\t0_4_4_0\n'
    res = node.query("select name, active, mintid, 'csn' || toString(mincsn), maxtid, 'csn' || toString(maxcsn) from system.parts where table='mt' order by name")
    res = res.replace(tid0, 'tid0')
    res = res.replace(tid1, 'tid1').replace('csn' + csn1, 'csn_1')
    res = res.replace(tid2, 'tid2').replace('csn' + csn2, 'csn_2')
    res = res.replace(tid3, 'tid3')
    res = res.replace(tid4, 'tid4')
    res = res.replace(tid5, 'tid5')
    assert res == "0_2_2_0\t1\ttid0\tcsn1\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "0_2_4_1\t0\ttid4\tcsn18446744073709551615\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "0_4_4_0\t1\ttid2\tcsn_2\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "0_7_7_0\t0\ttid5\tcsn18446744073709551615\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "1_1_1_0\t0\ttid0\tcsn1\ttid1\tcsn_1\n" \
                  "1_3_3_0\t1\ttid2\tcsn_2\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "1_3_3_0_6\t0\ttid3\tcsn18446744073709551615\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "1_5_5_0\t0\ttid3\tcsn18446744073709551615\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n" \
                  "1_5_5_0_6\t0\ttid3\tcsn18446744073709551615\t(0,0,'00000000-0000-0000-0000-000000000000')\tcsn0\n"






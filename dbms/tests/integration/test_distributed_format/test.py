import time
import pytest

from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.client import QueryRuntimeException, QueryTimeoutExceedException

from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', config_dir="configs", main_configs=['configs/remote_servers.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_single_file(started_cluster):
    node.query("create table distr_1 (x UInt64, s String) engine = Distributed('test_cluster', database, table)")
    node.query("insert into distr_1 values (1, 'a'), (2, 'bb'), (3, 'ccc')")

    query = "select * from file('/var/lib/clickhouse/data/default/distr_1/shard1_replica1/1.bin', 'Distributed')"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    query = "create table t (dummy UInt32) engine = File('Distributed', '/var/lib/clickhouse/data/default/distr_1/shard1_replica1/1.bin');" \
            "select * from t"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    node.query("drop table distr_1")


def test_two_files(started_cluster):
    node.query("create table distr_2 (x UInt64, s String) engine = Distributed('test_cluster', database, table)")
    node.query("insert into distr_2 values (0, '_'), (1, 'a')")
    node.query("insert into distr_2 values (2, 'bb'), (3, 'ccc')")

    query = "select * from file('/var/lib/clickhouse/data/default/distr_2/shard1_replica1/{1,2,3,4}.bin', 'Distributed') order by x"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '0\t_\n1\ta\n2\tbb\n3\tccc\n'

    query = "create table t (dummy UInt32) engine = File('Distributed', '/var/lib/clickhouse/data/default/distr_2/shard1_replica1/{1,2,3,4}.bin');" \
            "select * from t order by x"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '0\t_\n1\ta\n2\tbb\n3\tccc\n'

    node.query("drop table distr_2")

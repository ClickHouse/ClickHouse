import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/remote_servers.xml'])

cluster_param = pytest.mark.parametrize("cluster", [
    ('test_cluster'),
    ('test_cluster_2'),
])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query("create database test")
        yield cluster

    finally:
        cluster.shutdown()


@cluster_param
def test_single_file(started_cluster, cluster):
    node.query(
        "create table test.distr_1 (x UInt64, s String) engine = Distributed('{}', database, table)".format(cluster))
    node.query("insert into test.distr_1 values (1, 'a'), (2, 'bb'), (3, 'ccc')",
               settings={"use_compact_format_in_distributed_parts_names": "1"})

    query = "select * from file('/var/lib/clickhouse/data/test/distr_1/shard1_replica1/1.bin', 'Distributed')"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    query = "create table t (x UInt64, s String) engine = File('Distributed', '/var/lib/clickhouse/data/test/distr_1/shard1_replica1/1.bin');" \
            "select * from t"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    node.query("drop table test.distr_1")


@cluster_param
def test_two_files(started_cluster, cluster):
    node.query(
        "create table test.distr_2 (x UInt64, s String) engine = Distributed('{}', database, table)".format(cluster))
    node.query("insert into test.distr_2 values (0, '_'), (1, 'a')", settings={
        "use_compact_format_in_distributed_parts_names": "1",
    })
    node.query("insert into test.distr_2 values (2, 'bb'), (3, 'ccc')", settings={
        "use_compact_format_in_distributed_parts_names": "1",
    })

    query = "select * from file('/var/lib/clickhouse/data/test/distr_2/shard1_replica1/{1,2,3,4}.bin', 'Distributed') order by x"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '0\t_\n1\ta\n2\tbb\n3\tccc\n'

    query = "create table t (x UInt64, s String) engine = File('Distributed', '/var/lib/clickhouse/data/test/distr_2/shard1_replica1/{1,2,3,4}.bin');" \
            "select * from t order by x"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '0\t_\n1\ta\n2\tbb\n3\tccc\n'

    node.query("drop table test.distr_2")


@cluster_param
def test_single_file_old(started_cluster, cluster):
    node.query(
        "create table test.distr_3 (x UInt64, s String) engine = Distributed('{}', database, table)".format(cluster))
    node.query("insert into test.distr_3 values (1, 'a'), (2, 'bb'), (3, 'ccc')", settings={
        "use_compact_format_in_distributed_parts_names": "0",
    })

    query = "select * from file('/var/lib/clickhouse/data/test/distr_3/default@not_existing:9000/1.bin', 'Distributed')"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    query = "create table t (x UInt64, s String) engine = File('Distributed', '/var/lib/clickhouse/data/test/distr_3/default@not_existing:9000/1.bin');" \
            "select * from t"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    node.query("drop table test.distr_3")

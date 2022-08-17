# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=line-too-long

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml", "configs/another_remote_servers.xml"],
    stay_alive=True,
)


cluster_param = pytest.mark.parametrize("cluster", [
    ('test_cluster_internal_replication'),
    ('test_cluster_no_internal_replication'),
])


def get_dist_path(cluster, table, dist_format):
    if dist_format == 0:
        return f'/var/lib/clickhouse/data/test/{table}/default@not_existing:9000'
    if cluster == 'test_cluster_internal_replication':
        return f'/var/lib/clickhouse/data/test/{table}/shard1_all_replicas'
    return f'/var/lib/clickhouse/data/test/{table}/shard1_replica1'


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

    path = get_dist_path(cluster, 'distr_1', 1)
    query = f"select * from file('{path}/1.bin', 'Distributed')"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    query = f"""
    create table t (x UInt64, s String) engine = File('Distributed', '{path}/1.bin');
    select * from t;
    """
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

    path = get_dist_path(cluster, 'distr_2', 1)
    query = f"select * from file('{path}/{{1,2,3,4}}.bin', 'Distributed') order by x"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '0\t_\n1\ta\n2\tbb\n3\tccc\n'

    query = f"""
    create table t (x UInt64, s String) engine = File('Distributed', '{path}/{{1,2,3,4}}.bin');
    select * from t order by x;
    """
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

    path = get_dist_path(cluster, 'distr_3', 0)
    query = f"select * from file('{path}/1.bin', 'Distributed')"
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    query = f"""
    create table t (x UInt64, s String) engine = File('Distributed', '{path}/1.bin');
    select * from t;
    """
    out = node.exec_in_container(['/usr/bin/clickhouse', 'local', '--stacktrace', '-q', query])

    assert out == '1\ta\n2\tbb\n3\tccc\n'

    node.query("drop table test.distr_3")


def test_remove_replica(started_cluster):
    node.query(
        "create table test.local_4 (x UInt64, s String) engine = MergeTree order by x"
    )
    node.query(
        "create table test.distr_4 (x UInt64, s String) engine = Distributed('test_cluster_remove_replica1', test, local_4)"
    )
    node.query(
        "insert into test.distr_4 values (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd')"
    )
    node.query("detach table test.distr_4")

    node.exec_in_container(
        [
            "sed",
            "-i",
            "s/test_cluster_remove_replica1/test_cluster_remove_replica_tmp/g",
            "/etc/clickhouse-server/config.d/another_remote_servers.xml",
        ]
    )
    node.exec_in_container(
        [
            "sed",
            "-i",
            "s/test_cluster_remove_replica2/test_cluster_remove_replica1/g",
            "/etc/clickhouse-server/config.d/another_remote_servers.xml",
        ]
    )
    node.query("SYSTEM RELOAD CONFIG")
    node.query("attach table test.distr_4", ignore_error=True)
    node.query("SYSTEM FLUSH DISTRIBUTED test.distr_4", ignore_error=True)
    assert node.query("select 1") == "1\n"

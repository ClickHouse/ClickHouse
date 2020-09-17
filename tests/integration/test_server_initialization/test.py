import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        instance = cluster.add_instance('dummy', clickhouse_path_dir='clickhouse_path', stay_alive=True)
        cluster.start()

        cluster_fail = ClickHouseCluster(__file__, name='fail')
        instance_fail = cluster_fail.add_instance('dummy_fail', clickhouse_path_dir='clickhouse_path_fail')
        with pytest.raises(Exception):
            cluster_fail.start()
        cluster_fail.shutdown()  # cleanup

        yield cluster

    finally:
        cluster.shutdown()


def test_sophisticated_default(started_cluster):
    instance = started_cluster.instances['dummy']
    instance.query("INSERT INTO sophisticated_default (c) VALUES (0)")
    assert instance.query("SELECT a, b, c FROM sophisticated_default") == "3\t9\t0\n"


def test_partially_dropped_tables(started_cluster):
    instance = started_cluster.instances['dummy']
    assert instance.exec_in_container(['bash', '-c', 'find /var/lib/clickhouse/*/default -name *.sql* | sort'],
                                      privileged=True, user='root') \
           == "/var/lib/clickhouse/metadata/default/should_be_restored.sql\n" \
              "/var/lib/clickhouse/metadata/default/sophisticated_default.sql\n"
    assert instance.query("SELECT n FROM should_be_restored") == "1\n2\n3\n"
    assert instance.query("SELECT count() FROM system.tables WHERE name='should_be_dropped'") == "0\n"


def test_live_view_dependency(started_cluster):
    instance = started_cluster.instances['dummy']
    instance.query("CREATE DATABASE a_load_first")
    instance.query("CREATE DATABASE b_load_second")
    instance.query("CREATE TABLE b_load_second.mt (a Int32) Engine=MergeTree order by tuple()")
    instance.query("CREATE LIVE VIEW a_load_first.lv AS SELECT sum(a) FROM b_load_second.mt",
                   settings={'allow_experimental_live_view': 1})
    instance.restart_clickhouse()

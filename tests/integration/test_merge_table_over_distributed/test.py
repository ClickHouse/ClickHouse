from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query('''
CREATE TABLE local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE local_table_2(id UInt32, val String) ENGINE = MergeTree ORDER BY id;
''')

        node1.query("INSERT INTO local_table VALUES (1, 'node1')")
        node2.query("INSERT INTO local_table VALUES (2, 'node2')")

        node1.query('''
CREATE TABLE distributed_table(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table);
CREATE TABLE distributed_table_2(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table_2);
CREATE TABLE merge_table(id UInt32, val String) ENGINE = Merge(default, '^distributed_table')
''')

        yield cluster

    finally:
        cluster.shutdown()


def test_global_in(started_cluster):
    assert node1.query("SELECT val FROM distributed_table WHERE id GLOBAL IN (SELECT toUInt32(3 - id) FROM local_table)").rstrip() \
        == 'node2'

    assert node1.query("SELECT val FROM merge_table WHERE id GLOBAL IN (SELECT toUInt32(3 - id) FROM local_table)").rstrip() \
        == 'node2'


def test_filtering(started_cluster):

    assert node1.query("SELECT id, val FROM merge_table WHERE id = 1").rstrip() == '1\tnode1'

    assert node1.query("SELECT id + 1, val FROM merge_table WHERE id = 1").rstrip() == '2\tnode1'

    assert node1.query("SELECT id + 1 FROM merge_table WHERE val = 'node1'").rstrip() == '2'

    assert node1.query("SELECT id + 1, val FROM merge_table PREWHERE id = 1 WHERE _table != '_dummy'").rstrip() == '2\tnode1'

    assert node1.query("SELECT count() FROM merge_table PREWHERE id = 1").rstrip() == '1'


def test_select_table_name_from_merge_over_distributed(started_cluster):

    node1.query("INSERT INTO local_table_2 VALUES (1, 'node1')")
    node2.query("INSERT INTO local_table_2 VALUES (2, 'node2')")

    node1.query("select _table == 'distributed_table' from merge_table")
    node1.query("select * from (select _table == 'distributed_table' from merge_table limit 1)")


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in cluster.instances.items():
            print name, instance.ip_address
        raw_input("Cluster created, press any key to destroy...")

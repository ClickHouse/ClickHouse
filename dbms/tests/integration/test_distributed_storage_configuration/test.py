# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node',
            config_dir='configs',
            tmpfs=['/disk1:size=100M', '/disk2:size=100M'])

@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def _files_in_dist_mon(node, root, table):
    return int(node.exec_in_container([
        'bash',
        '-c',
        # `-maxdepth 1` to avoid /tmp/ subdirectory
        'find /{root}/data/default/{table}/shard2_replica1 -maxdepth 1 -type f | wc -l'.format(root=root, table=table)
    ]).split('\n')[0])

def test_different_versions(start_cluster):
    node.query('CREATE TABLE foo (key Int) Engine=Memory()')
    node.query("""
    CREATE TABLE dist_foo (key Int)
    Engine=Distributed(
        test_cluster_two_shards,
        currentDatabase(),
        foo,
        key%2,
        'default'
    )
    """)
    # manual only
    node.query('SYSTEM STOP DISTRIBUTED SENDS dist_foo')

    node.query('INSERT INTO dist_foo SELECT * FROM numbers(100)')
    assert _files_in_dist_mon(node, 'disk1', 'dist_foo') == 1
    assert _files_in_dist_mon(node, 'disk2', 'dist_foo') == 0

    assert node.query('SELECT count() FROM dist_foo') == '100\n'
    node.query('SYSTEM FLUSH DISTRIBUTED dist_foo')
    assert node.query('SELECT count() FROM dist_foo') == '200\n'

    #
    # RENAME
    #
    node.query('RENAME TABLE dist_foo TO dist2_foo')

    node.query('INSERT INTO dist2_foo SELECT * FROM numbers(100)')
    assert _files_in_dist_mon(node, 'disk1', 'dist2_foo') == 0
    assert _files_in_dist_mon(node, 'disk2', 'dist2_foo') == 1

    assert node.query('SELECT count() FROM dist2_foo') == '300\n'
    node.query('SYSTEM FLUSH DISTRIBUTED dist2_foo')
    assert node.query('SELECT count() FROM dist2_foo') == '400\n'

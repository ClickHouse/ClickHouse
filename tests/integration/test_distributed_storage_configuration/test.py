# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node',
                            main_configs=["configs/config.d/storage_configuration.xml"],
                            tmpfs=['/disk1:size=100M', '/disk2:size=100M'])


@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()
        node.query('CREATE DATABASE test ENGINE=Ordinary') # Different paths with Atomic
        yield cluster
    finally:
        cluster.shutdown()


def _files_in_dist_mon(node, root, table):
    return int(node.exec_in_container([
        'bash',
        '-c',
        # `-maxdepth 1` to avoid /tmp/ subdirectory
        'find /{root}/data/test/{table}/default@127%2E0%2E0%2E2:9000 -maxdepth 1 -type f 2>/dev/null | wc -l'.format(
            root=root, table=table)
    ]).split('\n')[0])


def test_insert(start_cluster):
    node.query('CREATE TABLE test.foo (key Int) Engine=Memory()')
    node.query("""
    CREATE TABLE test.dist_foo (key Int)
    Engine=Distributed(
        test_cluster_two_shards,
        test,
        foo,
        key%2,
        'default'
    )
    """)
    # manual only (but only for remote node)
    node.query('SYSTEM STOP DISTRIBUTED SENDS test.dist_foo')

    node.query('INSERT INTO test.dist_foo SELECT * FROM numbers(100)', settings={
        'use_compact_format_in_distributed_parts_names': '0',
    })
    assert _files_in_dist_mon(node, 'disk1', 'dist_foo') == 1
    assert _files_in_dist_mon(node, 'disk2', 'dist_foo') == 0

    assert node.query('SELECT count() FROM test.dist_foo') == '100\n'
    node.query('SYSTEM FLUSH DISTRIBUTED test.dist_foo')
    assert node.query('SELECT count() FROM test.dist_foo') == '200\n'

    #
    # RENAME
    #
    node.query('RENAME TABLE test.dist_foo TO test.dist2_foo')

    node.query('INSERT INTO test.dist2_foo SELECT * FROM numbers(100)', settings={
        'use_compact_format_in_distributed_parts_names': '0',
    })
    assert _files_in_dist_mon(node, 'disk1', 'dist2_foo') == 0
    assert _files_in_dist_mon(node, 'disk2', 'dist2_foo') == 1

    assert node.query('SELECT count() FROM test.dist2_foo') == '300\n'
    node.query('SYSTEM FLUSH DISTRIBUTED test.dist2_foo')
    assert node.query('SELECT count() FROM test.dist2_foo') == '400\n'

    #
    # DROP
    #
    node.query('DROP TABLE test.dist2_foo')
    for disk in ['disk1', 'disk2']:
        node.exec_in_container([
            'bash', '-c',
            'test ! -e /{}/data/test/dist2_foo'.format(disk)
        ])

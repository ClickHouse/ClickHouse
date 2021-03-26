# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

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

def test_different_versions(start_cluster):
    query = 'SELECT count(ignore(*)) FROM (SELECT * FROM system.numbers LIMIT 1e7) GROUP BY number'
    settings = {
        'max_bytes_before_external_group_by': 1<<20,
        'max_bytes_before_external_sort':     1<<20,
    }

    assert node.contains_in_log('Setting up /disk1/ to store temporary data in it')
    assert node.contains_in_log('Setting up /disk2/ to store temporary data in it')

    node.query(query, settings=settings)
    assert node.contains_in_log('Writing part of aggregation data into temporary file /disk1/')
    assert node.contains_in_log('Writing part of aggregation data into temporary file /disk2/')

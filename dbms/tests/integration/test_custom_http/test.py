import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def add_instance(name, config_dir):
    print os.path.join(SCRIPT_PATH, 'common_configs', 'common_users.xml')
    print os.path.join(SCRIPT_PATH, 'common_configs', 'common_config.xml')
    return cluster.add_instance(name, config_dir=os.path.join(SCRIPT_PATH, config_dir),
                                main_configs=[os.path.join(SCRIPT_PATH, 'common_configs', 'common_config.xml')],
                                user_configs=[os.path.join(SCRIPT_PATH, 'common_configs', 'common_users.xml')])


normally_instance = add_instance("normally_node", "normally_configs")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        normally_instance.query('CREATE DATABASE `test`')
        normally_instance.query('CREATE TABLE `test`.`test` (`id` UInt8) Engine = Memory')
        yield cluster
    finally:
        cluster.shutdown()


def test_normally_match(started_cluster):
    assert normally_instance.http_request('test_for_only_insert_queries', method='PUT', data='(1)(2)(3)') == ''
    assert normally_instance.http_request(url='test_for_only_select_queries',
            params='max_threads=1', method='POST', data='max_alter_threads=2') == '1\n2\n'
    assert normally_instance.http_request('test_for_hybrid_insert_and_select_queries', method='POST', data='(4)') == '1\n2\n3\n4\n'
    assert 'Throw Exception' in normally_instance.http_request('test_for_throw_exception_when_after_select')

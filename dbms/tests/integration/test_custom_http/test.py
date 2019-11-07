import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
test_instance = cluster.add_instance('node', main_configs=['configs/custom_http_config.xml'])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        test_instance.query('CREATE DATABASE `test`')
        test_instance.query('CREATE TABLE `test`.`test` (`id` UInt8) Engine = Memory')
        yield cluster
    finally:
        cluster.shutdown()


def test_for_single_insert(started_cluster):
    assert test_instance.http_query('/test_for_single_insert', data='(1)(2)(3)') == '\n'

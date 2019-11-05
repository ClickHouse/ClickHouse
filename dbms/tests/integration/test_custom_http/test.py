import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/custom_http.xml'])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query('''
CREATE DATABASE `test`;

CREATE TABLE `test`.`test_custom_http` (`id` UInt8) Engine=Memory;
        ''')

        yield cluster
    finally:
        cluster.shutdown()

def test(started_cluster):
    node_ip = cluster.get_instance_ip(node)
    url = 'http://%s:8123/test/a/1/test_custom_http' % node_ip
    data="(1)"
    params = {'id':1}
    response = requests.post(url, params = params, data = data)

    assert response.text == '\n1\n1\n'

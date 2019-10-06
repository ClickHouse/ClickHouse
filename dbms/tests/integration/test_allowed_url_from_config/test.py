import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/config_with_hosts.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/config_with_only_primary_hosts.xml'])
node3 = cluster.add_instance('node3', main_configs=['configs/config_with_only_regexp_hosts.xml'])
node4 = cluster.add_instance('node4', main_configs=['configs/config_without_allowed_hosts.xml'])
node5 = cluster.add_instance('node5', main_configs=['configs/config_without_block_of_allowed_hosts.xml'])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_config_with_hosts(start_cluster):
    assert node1.query("CREATE TABLE table_test_1_1 (word String) Engine=URL('http://host:80', CSV)") == ""
    assert node1.query("CREATE TABLE table_test_1_2 (word String) Engine=URL('https://yandex.ru', CSV)") == ""
    assert "Unacceptable URL." in node1.query_and_get_error("CREATE TABLE table_test_1_4 (word String) Engine=URL('https://host:123', CSV)")
    assert "Unacceptable URL." in node1.query_and_get_error("CREATE TABLE table_test_1_4 (word String) Engine=URL('https://yandex2.ru', CSV)") 

def test_config_with_only_primary_hosts(start_cluster):
    assert node2.query("CREATE TABLE table_test_2_1 (word String) Engine=URL('https://host:80', CSV)") == ""
    assert node2.query("CREATE TABLE table_test_2_2 (word String) Engine=URL('https://host:123', CSV)") == ""
    assert node2.query("CREATE TABLE table_test_2_3 (word String) Engine=URL('https://yandex.ru', CSV)") == ""
    assert node2.query("CREATE TABLE table_test_2_4 (word String) Engine=URL('https://yandex.ru:87', CSV)") == ""
    assert "Unacceptable URL." in node2.query_and_get_error("CREATE TABLE table_test_2_5 (word String) Engine=URL('https://host', CSV)")
    assert "Unacceptable URL." in node2.query_and_get_error("CREATE TABLE table_test_2_5 (word String) Engine=URL('https://host:234', CSV)")
    assert "Unacceptable URL." in node2.query_and_get_error("CREATE TABLE table_test_2_6 (word String) Engine=URL('https://yandex2.ru', CSV)")

def test_config_with_only_regexp_hosts(start_cluster):
    assert node3.query("CREATE TABLE table_test_3_1 (word String) Engine=URL('https://host:80', CSV)") == ""
    assert node3.query("CREATE TABLE table_test_3_2 (word String) Engine=URL('https://yandex.ru', CSV)") == ""
    assert "Unacceptable URL." in node3.query_and_get_error("CREATE TABLE table_test_3_3 (word String) Engine=URL('https://host', CSV)")
    assert "Unacceptable URL." in node3.query_and_get_error("CREATE TABLE table_test_3_4 (word String) Engine=URL('https://yandex2.ru', CSV)")

def test_config_without_allowed_hosts(start_cluster):
    assert node4.query("CREATE TABLE table_test_4_1 (word String) Engine=URL('https://host:80', CSV)") == ""
    assert node4.query("CREATE TABLE table_test_4_2 (word String) Engine=URL('https://host', CSV)") == ""
    assert node4.query("CREATE TABLE table_test_4_3 (word String) Engine=URL('https://yandex.ru', CSV)") == ""
    assert node4.query("CREATE TABLE table_test_4_4 (word String) Engine=URL('ftp://something.com', CSV)") == ""

def test_config_without_block_of_allowed_hosts(start_cluster):
    assert node5.query("CREATE TABLE table_test_5_1 (word String) Engine=URL('https://host:80', CSV)") == ""
    assert node5.query("CREATE TABLE table_test_5_2 (word String) Engine=URL('https://host', CSV)") == ""
    assert node5.query("CREATE TABLE table_test_5_3 (word String) Engine=URL('https://yandex.ru', CSV)") == ""
    assert node5.query("CREATE TABLE table_test_5_4 (word String) Engine=URL('ftp://something.com', CSV)") == ""

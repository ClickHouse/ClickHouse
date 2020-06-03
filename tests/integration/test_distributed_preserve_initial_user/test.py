# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long
# pylint: disable=redefined-builtin

from time import sleep
import threading
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

n1 = cluster.add_instance('n1', main_configs=['configs/remote_servers.xml'], config_dir='configs')
n2 = cluster.add_instance('n2', main_configs=['configs/remote_servers.xml'], config_dir='configs', stay_alive=True)

def create_system_tables():
    n1.query("SYSTEM FLUSH LOGS")
    n2.query("SYSTEM FLUSH LOGS")

def create_tables():
    n1.query("""
    CREATE TABLE d1 AS system.one
    Engine=Distributed(c1, system, one)
    """)

    n1.query("""
    CREATE TABLE d2 AS system.one
    Engine=Distributed(c2, system, one)
    """)

@pytest.fixture(scope='module', autouse=True)
def start_cluster():
    try:
        cluster.start()
        create_tables()
        create_system_tables()
        yield cluster
    finally:
        cluster.shutdown()

def query_with_id(node, id, query, **kwargs):
    return node.query("WITH '{}' AS __id {}".format(id, query), **kwargs)

def get_query_user_info(node, query_pattern):
    node.query("SYSTEM FLUSH LOGS")
    return node.query("""
    SELECT user, initial_user
    FROM system.query_log
    WHERE
        query LIKE '%{}%' AND
        query NOT LIKE '%system.query_log%' AND
        type = 'QueryFinish'
    """.format(query_pattern)).strip().split('\t')

def test_user_default_1():
    query_with_id(n1, 'd1-user-default', 'SELECT * FROM d1')
    assert get_query_user_info(n1, 'd1-user-default') == ['default', 'default']
    assert get_query_user_info(n2, 'd1-user-default') == ['default', 'default']

def test_user_default_2():
    query_with_id(n1, 'd2-user-default', 'SELECT * FROM d2')
    assert get_query_user_info(n1, 'd2-user-default') == ['default', 'default'] # due to local
    assert get_query_user_info(n2, 'd2-user-default') == ['nopass',  'default']

def test_user_nopass():
    query_with_id(n1, 'd1-user-nopass', 'SELECT * FROM d1', user='nopass')
    assert get_query_user_info(n1, 'd1-user-nopass') == ['nopass', 'nopass']
    assert get_query_user_info(n2, 'd1-user-nopass') == ['nopass', 'nopass']

def test_user_pass():
    query_with_id(n1, 'd1-user-pass', 'SELECT * FROM d1', user='pass', password='foo')
    assert get_query_user_info(n1, 'd1-user-pass') == ['pass', 'pass']
    assert get_query_user_info(n2, 'd1-user-pass') == ['pass', 'pass']

def test_user_default_retry():
    n1.query('SELECT * FROM d1')
    # pause n2 instance to trigger connection retry on n1
    def cont():
        sleep(5)
        cluster.unpause_container('n2')
    thread = threading.Thread(target=cont)
    cluster.pause_container('n2')
    thread.start()
    query_with_id(n1, 'd1-retry-user-default', 'SELECT * FROM d1')
    thread.join()

    assert get_query_user_info(n1, 'd1-retry-user-default') == ['default', 'default']
    assert get_query_user_info(n2, 'd1-retry-user-default') == ['default', 'default']

# coding: utf-8

import os
import pytest
import pymysql.connections

from docker.models.containers import Container

from helpers.cluster import ClickHouseCluster


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, './configs'))

node = cluster.add_instance('node', with_mysql=True)

server_port = 9001


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope='module')
def mysql_container(started_cluster):
    # type: (ClickHouseCluster) -> Container
    yield started_cluster.docker_client.containers.get(started_cluster.get_instance_docker_id('mysql1'))


@pytest.fixture(scope='module')
def server_address(started_cluster):
    yield started_cluster.get_instance_ip('node')


def test_select(mysql_container, server_address):
    # type: (Container, str) -> None
    code, (stdout, stderr) = mysql_container.exec_run('''
        mysql --protocol tcp -h {host} -P {port} default -u default --enable-cleartext-plugin --password=123
        -e "select 1 as a;"
        -e "select 'тест' as b;"
    '''.format(host=server_address, port=server_port), demux=True)

    assert stdout == 'a\n1\nb\nтест\n'


def test_authentication(mysql_container, server_address):
    # type: (Container, str) -> None

    code, (stdout, stderr) = mysql_container.exec_run('''
        mysql --protocol tcp -h {host} -P {port} default -u default --enable-cleartext-plugin --password=abc -e "select 1 as a;"
    '''.format(host=server_address, port=server_port), demux=True)

    assert stderr == 'mysql: [Warning] Using a password on the command line interface can be insecure.\n' \
                     'ERROR 193 (00000): Wrong password for user default\n'


def test_change_database(mysql_container, server_address):
    # type: (Container, str) -> None

    code, (stdout, stderr) = mysql_container.exec_run('''
        mysql --protocol tcp -h {host} -P {port} default -u default --enable-cleartext-plugin --password=123
        -e "use system;"
        -e "select count(*) from (select name from tables limit 1);"
        -e "use system2;"
    '''.format(host=server_address, port=server_port), demux=True)

    assert stdout == 'count()\n1\n'
    assert stderr == "mysql: [Warning] Using a password on the command line interface can be insecure.\n" \
                     "ERROR 81 (00000) at line 1: Database system2 doesn't exist\n"


def test_py_client(server_address):
    with pytest.raises(pymysql.InternalError) as exc_info:
        pymysql.connections.Connection(host=server_address, user='default', password='abacab', database='default', port=server_port)

    assert exc_info.value.args == (193, 'Wrong password for user default')

    client = pymysql.connections.Connection(host=server_address, user='default', password='123', database='default', port=server_port)

    with pytest.raises(pymysql.InternalError) as exc_info:
        client.query('select name from tables')

    assert exc_info.value.args == (60, "Table default.tables doesn't exist.")

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("select 1 as a, 'тест' as b")
    assert cursor.fetchall() == [{'a': '1', 'b': 'тест'}]

    client.select_db('system')

    with pytest.raises(pymysql.InternalError) as exc_info:
        client.select_db('system2')

    assert exc_info.value.args == (81, "Database system2 doesn't exist")

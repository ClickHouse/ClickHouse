# coding: utf-8

import os
import docker
import pytest
import subprocess
import pymysql.connections

from docker.models.containers import Container

from helpers.cluster import ClickHouseCluster


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, './configs'))
node = cluster.add_instance('node')

server_port = 9001


@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip('node')
    finally:
        cluster.shutdown()


@pytest.fixture(scope='module')
def mysql_client():
    docker_compose = os.path.join(SCRIPT_DIR, 'clients', 'mysql', 'docker_compose.yml')
    subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d', '--build'])
    yield docker.from_env().containers.get(cluster.project_name + '_mysql1_1')


@pytest.fixture(scope='module')
def golang_container():
    docker_compose = os.path.join(SCRIPT_DIR, 'clients', 'golang', 'docker_compose.yml')
    subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d', '--build'])
    yield docker.from_env().containers.get(cluster.project_name + '_golang1_1')


def test_mysql_client(mysql_client, server_address):
    # type: (Container, str) -> None
    code, (stdout, stderr) = mysql_client.exec_run('''
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "select 1 as a;"
        -e "select 'тест' as b;"
    '''.format(host=server_address, port=server_port), demux=True)

    import pdb
    pdb.set_trace()
    assert stdout == 'a\n1\nb\nтест\n'

    code, (stdout, stderr) = mysql_client.exec_run('''
        mysql --protocol tcp -h {host} -P {port} default -u default --password=abc -e "select 1 as a;"
    '''.format(host=server_address, port=server_port), demux=True)

    assert stderr == 'mysql: [Warning] Using a password on the command line interface can be insecure.\n' \
                     'ERROR 193 (00000): Wrong password for user default\n'

    code, (stdout, stderr) = mysql_client.exec_run('''
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "use system;"
        -e "select count(*) from (select name from tables limit 1);"
        -e "use system2;"
    '''.format(host=server_address, port=server_port), demux=True)

    assert stdout == 'count()\n1\n'
    assert stderr == "mysql: [Warning] Using a password on the command line interface can be insecure.\n" \
                     "ERROR 81 (00000) at line 1: Database system2 doesn't exist\n"


def test_python_client(server_address):
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


def test_golang_client(server_address, golang_container):
    # type: (str, Container) -> None
    code, (stdout, stderr) = golang_container.exec_run('./main --host {host} --port {port} --user default --password 123 --database '
                                                       'abc'.format(host=server_address, port=server_port), demux=True)

    assert code == 1
    assert stderr == "Error 81: Database abc doesn't exist\n"

    code, (stdout, stderr) = golang_container.exec_run('./main --host {host} --port {port} --user default --password 123 --database '
                                                       'default'.format(host=server_address, port=server_port), demux=True)

    assert code == 0

    with open(os.path.join(SCRIPT_DIR, 'clients', 'golang', '0.reference')) as fp:
        reference = fp.read()
        assert stdout == reference

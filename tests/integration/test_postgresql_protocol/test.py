# -*- coding: utf-8 -*-

from __future__ import print_function

import datetime
import decimal
import docker
import psycopg2 as py_psql
import psycopg2.extras
import pytest
import os
import sys
import subprocess
import time
import uuid

from helpers.cluster import ClickHouseCluster

psycopg2.extras.register_uuid()

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(SCRIPT_DIR, './configs')

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', config_dir=config_dir, env_variables={'UBSAN_OPTIONS': 'print_stacktrace=1'})

server_port = 5433


@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip('node')
    finally:
        cluster.shutdown()


@pytest.fixture(scope='module')
def psql_client():
    docker_compose = os.path.join(SCRIPT_DIR, 'clients', 'psql', 'docker_compose.yml')
    subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d', '--build'])
    yield docker.from_env().containers.get(cluster.project_name + '_psql_1')


@pytest.fixture(scope='module')
def psql_server(psql_client):
    """Return PostgreSQL container when it is healthy."""
    retries = 30
    for i in range(retries):
        info = psql_client.client.api.inspect_container(psql_client.name)
        if info['State']['Health']['Status'] == 'healthy':
            break
        time.sleep(1)
    else:
        print(info['State'])
        raise Exception('PostgreSQL server has not started after {} retries.'.format(retries))

    return psql_client


@pytest.fixture(scope='module')
def java_container():
    docker_compose = os.path.join(SCRIPT_DIR, 'clients', 'java', 'docker_compose.yml')
    subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d', '--build'])
    yield docker.from_env().containers.get(cluster.project_name + '_java_1')


def test_psql_is_ready(psql_server):
    pass


def test_psql_client(psql_client, server_address):
    cmd_prefix = 'psql "sslmode=require host={server_address} port={server_port} user=default dbname=default password=123" '\
        .format(server_address=server_address, server_port=server_port)
    cmd_prefix += "--no-align --field-separator=' ' "

    code, (stdout, stderr) = psql_client.exec_run(cmd_prefix + '-c "SELECT 1 as a"', demux=True)
    assert stdout == '\n'.join(['a', '1', '(1 row)', ''])

    code, (stdout, stderr) = psql_client.exec_run(cmd_prefix + '''-c "SELECT 'колонка' as a"''', demux=True)
    assert stdout == '\n'.join(['a', 'колонка', '(1 row)', ''])

    code, (stdout, stderr) = psql_client.exec_run(
        cmd_prefix + '-c ' +
        '''
        "CREATE DATABASE x;
        USE x;
        CREATE TABLE table1 (column UInt32) ENGINE = Memory;
        INSERT INTO table1 VALUES (0), (1), (5);
        INSERT INTO table1 VALUES (0), (1), (5);
        SELECT * FROM table1 ORDER BY column;"
        ''',
        demux=True
    )
    assert stdout == '\n'.join(['column', '0', '0', '1', '1', '5', '5', '(6 rows)', ''])

    code, (stdout, stderr) = psql_client.exec_run(
        cmd_prefix + '-c ' +
        '''
        "DROP DATABASE x;
        CREATE TEMPORARY TABLE tmp (tmp_column UInt32);
        INSERT INTO tmp VALUES (0), (1);
        SELECT * FROM tmp ORDER BY tmp_column;"
        ''',
        demux=True
    )
    assert stdout == '\n'.join(['tmp_column', '0', '1', '(2 rows)', ''])


def test_python_client(server_address):
    with pytest.raises(py_psql.InternalError) as exc_info:
        ch = py_psql.connect(host=server_address, port=server_port, user='default', password='123', database='')
        cur = ch.cursor()
        cur.execute('select name from tables;')

    assert exc_info.value.args == ("Query execution failed.\nDB::Exception: Table default.tables doesn't exist.\nSSL connection has been closed unexpectedly\n",)

    ch = py_psql.connect(host=server_address, port=server_port, user='default', password='123', database='')
    cur = ch.cursor()

    cur.execute('select 1 as a, 2 as b')
    assert (cur.description[0].name, cur.description[1].name) == ('a', 'b')
    assert cur.fetchall() == [(1, 2)]

    cur.execute('CREATE DATABASE x')
    cur.execute('USE x')
    cur.execute('CREATE TEMPORARY TABLE tmp2 (ch Int8, i64 Int64, f64 Float64, str String, date Date, dec Decimal(19, 10), uuid UUID) ENGINE = Memory')
    cur.execute("insert into tmp2 (ch, i64, f64, str, date, dec, uuid) values (44, 534324234, 0.32423423, 'hello', '2019-01-23', 0.333333, '61f0c404-5cb3-11e7-907b-a6006ad3dba0')")
    cur.execute('select * from tmp2')
    assert cur.fetchall()[0] == ('44', 534324234, 0.32423423, 'hello', datetime.date(2019, 1, 23), decimal.Decimal('0.3333330000'), uuid.UUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))


def test_java_client(server_address, java_container):
    with open(os.path.join(SCRIPT_DIR, 'clients', 'java', '0.reference')) as fp:
        reference = fp.read()

    # database not exists exception.
    code, (stdout, stderr) = java_container.exec_run('java JavaConnectorTest --host {host} --port {port} --user default --database '
                                                     'abc'.format(host=server_address, port=server_port), demux=True)
    assert code == 1

    # non-empty password passed.
    code, (stdout, stderr) = java_container.exec_run('java JavaConnectorTest --host {host} --port {port} --user default --password 123 --database '
                                                     'default'.format(host=server_address, port=server_port), demux=True)
    print(stdout, stderr, file=sys.stderr)
    assert code == 0
    assert stdout == reference

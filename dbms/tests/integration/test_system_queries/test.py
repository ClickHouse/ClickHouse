import os
import os.path as p
import sys
import time
import datetime
import pytest
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    global instance
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance('ch1', config_dir="configs")
        cluster.start()

        instance = cluster.instances['ch1']
        instance.query('CREATE DATABASE dictionaries ENGINE = Dictionary')
        instance.query('CREATE TABLE dictionary_source (id UInt64, value UInt8) ENGINE = Memory')
        #print instance.query('SELECT * FROM system.dictionaries FORMAT Vertical')
        print "Started ", instance.ip_address

        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_SYSTEM_RELOAD_DICTIONARY(started_cluster):
    instance = cluster.instances['ch1']

    instance.query("SYSTEM RELOAD DICTIONARIES")
    assert TSV(instance.query("SELECT dictHas('clickhouse_flat', toUInt64(0)), dictHas('clickhouse_flat', toUInt64(1))")) == TSV("0\t0\n")

    instance.query("INSERT INTO dictionary_source VALUES (0, 0)")
    assert TSV(instance.query("SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictHas('clickhouse_cache', toUInt64(1))")) == TSV("0\t0\n")
    instance.query("INSERT INTO dictionary_source VALUES (1, 1)")
    assert TSV(instance.query("SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictHas('clickhouse_cache', toUInt64(1))")) == TSV("0\t0\n")

    instance.query("SYSTEM RELOAD DICTIONARY clickhouse_cache")
    assert TSV(instance.query("SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictGetUInt8('clickhouse_cache', 'value', toUInt64(1))")) == TSV("0\t1\n")
    assert TSV(instance.query("SELECT dictHas('clickhouse_flat', toUInt64(0)), dictHas('clickhouse_flat', toUInt64(1))")) == TSV("0\t0\n")

    instance.query("SYSTEM RELOAD DICTIONARIES")
    assert TSV(instance.query("SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictGetUInt8('clickhouse_cache', 'value', toUInt64(1))")) == TSV("0\t1\n")
    assert TSV(instance.query("SELECT dictGetUInt8('clickhouse_flat', 'value', toUInt64(0)), dictGetUInt8('clickhouse_flat', 'value', toUInt64(1))")) == TSV("0\t1\n")


def test_DROP_DNS_CACHE(started_cluster):
    instance = cluster.instances['ch1']

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 localhost > /etc/hosts'], privileged=True, user='root')
    instance.exec_in_container(['bash', '-c', 'echo ::1 localhost >> /etc/hosts'], privileged=True, user='root')

    instance.exec_in_container(['bash', '-c', 'echo 127.255.255.255 lost_host >> /etc/hosts'], privileged=True, user='root')
    instance.query("SYSTEM DROP DNS CACHE")

    with pytest.raises(QueryRuntimeException):
        instance.query("SELECT * FROM remote('lost_host', 'system', 'one')")

    instance.query("CREATE TABLE distributed_lost_host (dummy UInt8) ENGINE = Distributed(lost_host_cluster, 'system', 'one')")
    with pytest.raises(QueryRuntimeException):
        instance.query("SELECT * FROM distributed_lost_host")

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 localhost > /etc/hosts'], privileged=True, user='root')
    instance.exec_in_container(['bash', '-c', 'echo ::1 localhost >> /etc/hosts'], privileged=True, user='root')

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 lost_host >> /etc/hosts'], privileged=True, user='root')
    instance.query("SYSTEM DROP DNS CACHE")

    instance.query("SELECT * FROM remote('lost_host', 'system', 'one')")
    instance.query("SELECT * FROM distributed_lost_host")
    assert TSV(instance.query("SELECT DISTINCT host_name, host_address FROM system.clusters WHERE cluster='lost_host_cluster'")) == TSV("lost_host\t127.0.0.1\n")


def test_RELOAD_CONFIG_AND_MACROS(started_cluster):

    macros = "<yandex><macros><mac>ro</mac></macros></yandex>"
    create_macros = 'echo "{}" > /etc/clickhouse-server/config.d/macros.xml'.format(macros)

    instance = cluster.instances['ch1']

    instance.exec_in_container(['bash', '-c', create_macros], privileged=True, user='root')
    instance.query("SYSTEM RELOAD CONFIG")
    assert TSV(instance.query("select * from system.macros")) == TSV("mac\tro\n")

if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
       for name, instance in cluster.instances.items():
           print name, instance.ip_address
       raw_input("Cluster created, press any key to destroy...")

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

        yield cluster

    finally:
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

    with pytest.raises(Exception):
        instance.query("SELECT * FROM remote('aperol', 'system', 'one')")

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 aperol >> /etc/hosts'], privileged=True, user='root')
    instance.query("SYSTEM DROP DNS CACHE")

    instance.query("SELECT * FROM remote('aperol', 'system', 'one')")


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
       for name, instance in cluster.instances.items():
           print name, instance.ip_address
       raw_input("Cluster created, press any key to destroy...")

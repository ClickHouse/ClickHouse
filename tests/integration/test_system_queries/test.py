import os
import sys
import time
from contextlib import contextmanager

import pytest

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
        cluster.add_instance('ch1',
                             main_configs=["configs/config.d/clusters_config.xml", "configs/config.d/query_log.xml"],
                             dictionaries=["configs/dictionaries/dictionary_clickhouse_cache.xml",
                                           "configs/dictionaries/dictionary_clickhouse_flat.xml"])
        cluster.start()

        instance = cluster.instances['ch1']
        instance.query('CREATE DATABASE dictionaries ENGINE = Dictionary')
        instance.query('CREATE TABLE dictionary_source (id UInt64, value UInt8) ENGINE = Memory')
        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_SYSTEM_RELOAD_DICTIONARY(started_cluster):
    instance = cluster.instances['ch1']

    instance.query("SYSTEM RELOAD DICTIONARIES")
    assert TSV(instance.query(
        "SELECT dictHas('clickhouse_flat', toUInt64(0)), dictHas('clickhouse_flat', toUInt64(1))")) == TSV("0\t0\n")

    instance.query("INSERT INTO dictionary_source VALUES (0, 0)")
    assert TSV(instance.query(
        "SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictHas('clickhouse_cache', toUInt64(1))")) == TSV(
        "0\t0\n")
    instance.query("INSERT INTO dictionary_source VALUES (1, 1)")
    assert TSV(instance.query(
        "SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictHas('clickhouse_cache', toUInt64(1))")) == TSV(
        "0\t0\n")

    instance.query("SYSTEM RELOAD DICTIONARY clickhouse_cache")
    assert TSV(instance.query(
        "SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictGetUInt8('clickhouse_cache', 'value', toUInt64(1))")) == TSV(
        "0\t1\n")
    assert TSV(instance.query(
        "SELECT dictHas('clickhouse_flat', toUInt64(0)), dictHas('clickhouse_flat', toUInt64(1))")) == TSV("0\t0\n")

    instance.query("SYSTEM RELOAD DICTIONARIES")
    assert TSV(instance.query(
        "SELECT dictGetUInt8('clickhouse_cache', 'value', toUInt64(0)), dictGetUInt8('clickhouse_cache', 'value', toUInt64(1))")) == TSV(
        "0\t1\n")
    assert TSV(instance.query(
        "SELECT dictGetUInt8('clickhouse_flat', 'value', toUInt64(0)), dictGetUInt8('clickhouse_flat', 'value', toUInt64(1))")) == TSV(
        "0\t1\n")


def test_DROP_DNS_CACHE(started_cluster):
    instance = cluster.instances['ch1']

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 localhost > /etc/hosts'], privileged=True, user='root')
    instance.exec_in_container(['bash', '-c', 'echo ::1 localhost >> /etc/hosts'], privileged=True, user='root')

    instance.exec_in_container(['bash', '-c', 'echo 127.255.255.255 lost_host >> /etc/hosts'], privileged=True,
                               user='root')
    instance.query("SYSTEM DROP DNS CACHE")

    with pytest.raises(QueryRuntimeException):
        instance.query("SELECT * FROM remote('lost_host', 'system', 'one')")

    instance.query(
        "CREATE TABLE distributed_lost_host (dummy UInt8) ENGINE = Distributed(lost_host_cluster, 'system', 'one')")
    with pytest.raises(QueryRuntimeException):
        instance.query("SELECT * FROM distributed_lost_host")

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 localhost > /etc/hosts'], privileged=True, user='root')
    instance.exec_in_container(['bash', '-c', 'echo ::1 localhost >> /etc/hosts'], privileged=True, user='root')

    instance.exec_in_container(['bash', '-c', 'echo 127.0.0.1 lost_host >> /etc/hosts'], privileged=True, user='root')
    instance.query("SYSTEM DROP DNS CACHE")

    instance.query("SELECT * FROM remote('lost_host', 'system', 'one')")
    instance.query("SELECT * FROM distributed_lost_host")
    assert TSV(instance.query(
        "SELECT DISTINCT host_name, host_address FROM system.clusters WHERE cluster='lost_host_cluster'")) == TSV(
        "lost_host\t127.0.0.1\n")


def test_RELOAD_CONFIG_AND_MACROS(started_cluster):
    macros = "<yandex><macros><mac>ro</mac></macros></yandex>"
    create_macros = 'echo "{}" > /etc/clickhouse-server/config.d/macros.xml'.format(macros)

    instance = cluster.instances['ch1']

    instance.exec_in_container(['bash', '-c', create_macros], privileged=True, user='root')
    instance.query("SYSTEM RELOAD CONFIG")
    assert TSV(instance.query("select * from system.macros")) == TSV("instance\tch1\nmac\tro\n")


def test_system_flush_logs(started_cluster):
    instance = cluster.instances['ch1']
    instance.query('''
        SET log_queries = 0;
        SYSTEM FLUSH LOGS;
        TRUNCATE TABLE system.query_log;
    ''')
    for i in range(4):
        # Sleep to execute flushing from background thread at first query
        # by expiration of flush_interval_millisecond and test probable race condition.
        time.sleep(0.5)
        result = instance.query('''
            SELECT 1 FORMAT Null;
            SET log_queries = 0;
            SYSTEM FLUSH LOGS;
            SELECT count() FROM system.query_log;''')
        instance.query('''
            SET log_queries = 0;
            SYSTEM FLUSH LOGS;
            TRUNCATE TABLE system.query_log;
        ''')
        assert TSV(result) == TSV('4')


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")

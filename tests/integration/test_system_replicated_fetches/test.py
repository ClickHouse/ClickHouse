#!/usr/bin/env python3


import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry
import random
import string
import json

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def get_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def test_system_replicated_fetches(started_cluster):
    node1.query("CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '1') ORDER BY tuple()")
    node2.query("CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '2') ORDER BY tuple()")

    with PartitionManager() as pm:
        node2.query("SYSTEM STOP FETCHES t")
        node1.query("INSERT INTO t SELECT number, '{}' FROM numbers(10000)".format(get_random_string(104857)))
        pm.add_network_delay(node1, 80)
        node2.query("SYSTEM START FETCHES t")
        fetches_result = []
        for _ in range(1000):
            result = json.loads(node2.query("SELECT * FROM system.replicated_fetches FORMAT JSON"))
            if not result["data"]:
                if fetches_result:
                    break
                time.sleep(0.1)
            else:
                fetches_result.append(result["data"][0])
                print(fetches_result[-1])
                time.sleep(0.1)

    node2.query("SYSTEM SYNC REPLICA t", timeout=10)
    assert node2.query("SELECT COUNT() FROM t") == "10000\n"

    for elem in fetches_result:
        elem['bytes_read_compressed'] = float(elem['bytes_read_compressed'])
        elem['total_size_bytes_compressed'] = float(elem['total_size_bytes_compressed'])
        elem['progress'] = float(elem['progress'])
        elem['elapsed'] = float(elem['elapsed'])

    assert len(fetches_result) > 0
    first_non_empty = fetches_result[0]

    assert first_non_empty['database'] == "default"
    assert first_non_empty['table'] == "t"
    assert first_non_empty['source_replica_hostname'] == 'node1'
    assert first_non_empty['source_replica_port'] == 9009
    assert first_non_empty['source_replica_path'] == '/clickhouse/test/t/replicas/1'
    assert first_non_empty['interserver_scheme'] == 'http'
    assert first_non_empty['result_part_name'] == 'all_0_0_0'
    assert first_non_empty['result_part_path'].startswith('/var/lib/clickhouse/')
    assert first_non_empty['result_part_path'].endswith('all_0_0_0/')
    assert first_non_empty['partition_id'] == 'all'
    assert first_non_empty['URI'].startswith('http://node1:9009/?endpoint=DataPartsExchange')

    for elem in fetches_result:
        assert elem['bytes_read_compressed'] <= elem['total_size_bytes_compressed'], "Bytes read ({}) more than total bytes ({}). It's a bug".format(elem['bytes_read_compressed'], elem['total_size_bytes_compressed'])
        assert 0.0 <= elem['progress'] <= 1.0, "Progress shouldn't less than 0 and bigger than 1, got {}".format(elem['progress'])
        assert 0.0 <= elem['elapsed'], "Elapsed time must be greater than 0, got {}".format(elem['elapsed'])

    prev_progress = first_non_empty['progress']
    for elem in fetches_result:
        assert elem['progress'] >= prev_progress, "Progress decreasing prev{}, next {}? It's a bug".format(prev_progress, elem['progress'])
        prev_progress = elem['progress']

    prev_bytes = first_non_empty['bytes_read_compressed']
    for elem in fetches_result:
        assert elem['bytes_read_compressed'] >= prev_bytes, "Bytes read decreasing prev {}, next {}? It's a bug".format(prev_bytes, elem['bytes_read_compressed'])
        prev_bytes = elem['bytes_read_compressed']

    prev_elapsed = first_non_empty['elapsed']
    for elem in fetches_result:
        assert elem['elapsed'] >= prev_elapsed, "Elapsed time decreasing prev {}, next {}? It's a bug".format(prev_elapsed, elem['elapsed'])
        prev_elapsed = elem['elapsed']

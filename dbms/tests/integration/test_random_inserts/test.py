import time
import os
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV
from helpers.client import CommandRequest


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', config_dir='configs', with_zookeeper=True, macroses={"layer": 0, "shard": 0, "replica": 1})
node2 = cluster.add_instance('node2', config_dir='configs', with_zookeeper=True, macroses={"layer": 0, "shard": 0, "replica": 2})
nodes = [node1, node2]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_random_inserts(started_cluster):
    # Duration of the test, reduce it if don't want to wait
    DURATION_SECONDS = 10# * 60

    node1.query("""
        CREATE TABLE simple ON CLUSTER test_cluster (date Date, i UInt32, s String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple', '{replica}', date, i, 8192)""")

    with PartitionManager() as pm_random_drops:
        for sacrifice in nodes:
            pass # This test doesn't work with partition problems still
            #pm_random_drops._add_rule({'probability': 0.01, 'destination': sacrifice.ip_address, 'source_port': 2181, 'action': 'REJECT --reject-with tcp-reset'})
            #pm_random_drops._add_rule({'probability': 0.01, 'source': sacrifice.ip_address, 'destination_port': 2181, 'action': 'REJECT --reject-with tcp-reset'})

        min_timestamp = int(time.time())
        max_timestamp = min_timestamp + DURATION_SECONDS
        num_timestamps = max_timestamp - min_timestamp + 1

        bash_script = os.path.join(os.path.dirname(__file__), "test.sh")
        inserters = []
        for node in nodes:
            cmd = ['/bin/bash', bash_script, node.ip_address, str(min_timestamp), str(max_timestamp)]
            inserters.append(CommandRequest(cmd, timeout=DURATION_SECONDS * 2, stdin=''))
            print node.name, node.ip_address

        for inserter in inserters:
            inserter.get_answer()

    answer="{}\t{}\t{}\t{}\n".format(num_timestamps, num_timestamps, min_timestamp, max_timestamp)

    for node in nodes:
        res = node.query("SELECT count(), uniqExact(i), min(i), max(i) FROM simple")
        assert TSV(res) == TSV(answer), node.name + " : " + node.query("SELECT groupArray(_part), i, count() AS c FROM simple GROUP BY i ORDER BY c DESC LIMIT 1")

    node1.query("""DROP TABLE simple ON CLUSTER test_cluster""")

import time
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV
from helpers.client import CommandRequest


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', config_dir='configs', with_zookeeper=True)
node2 = cluster.add_instance('node2', config_dir='configs', with_zookeeper=True)
nodes = [node1, node2]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in nodes:
            node.query('''
CREATE TABLE simple (date Date, id UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
'''.format(replica=node.name))
            node.query("INSERT INTO simple VALUES (0, 0)")

            node.query('''
CREATE TABLE simple2 (date Date, id UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple2', '{replica}', date, id, 8192);
'''.format(replica=node.name))

        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_deduplication_window_in_seconds(started_cluster):
    node = node1

    node.query("INSERT INTO simple2 VALUES (0, 0)")
    time.sleep(1)
    node.query("INSERT INTO simple2 VALUES (0, 0)") # deduplication works here
    node.query("INSERT INTO simple2 VALUES (0, 1)")
    assert TSV(node.query("SELECT count() FROM simple2")) == TSV("2\n")

    # wait clean thread
    time.sleep(2)

    assert TSV.toMat(node.query("SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/0/simple2/blocks'"))[0][0] == "1"
    node.query("INSERT INTO simple2 VALUES (0, 0)") # deduplication doesn't works here, the first hash node was deleted
    assert TSV.toMat(node.query("SELECT count() FROM simple2"))[0][0] == "3"


def check_timeout_exception(e):
    s = str(e)
    #print s
    assert s.find('timed out!') >= 0 or s.find('Return code: -9') >= 0


# Currently this test just reproduce incorrect behavior that sould be fixed
def test_deduplication_works_in_case_of_intensive_inserts(started_cluster):
    inserters = []
    fetchers = []

    for node in nodes:
        host = node.ip_address

        inserters.append(CommandRequest(['/bin/bash'], timeout=10, stdin="""
set -e
for i in `seq 1000`; do
    clickhouse-client --host {} -q "INSERT INTO simple VALUES (0, 0)"
done
""".format(host)))

        fetchers.append(CommandRequest(['/bin/bash'], timeout=10, stdin="""
set -e
for i in `seq 1000`; do
    res=`clickhouse-client --host {} -q "SELECT count() FROM simple"`
    if [[ $res -ne 1 ]]; then
        echo "Selected $res elements! Host: {}" 1>&2
        exit -1
    fi;
done
""".format(host, node.name)))

    # There were not errors during INSERTs
    for inserter in inserters:
        try:
            inserter.get_answer()
        except Exception as e:
            check_timeout_exception(e)

    # There were not errors during SELECTs
    for fetcher in fetchers:
        try:
            fetcher.get_answer()
        except Exception as e:
            pass
            # Uncomment when problem will be fixed
            check_timeout_exception(e)

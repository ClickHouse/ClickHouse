"""
This is a basic test for de-duplication during data movement between shards.

The focus is on finding a way to propagate fingerprints to root executor and
use them for either de-duplicating or re-executing query by excluding parts
for which we have duplicate fingerprints.

1. We configure 2 nodes which have some sample data in a MergeTree table with
some parts containing a fingerprint.

2. Query the distributed table covering both nodes and check that data with
matching fingerprints is present just once in the end result.

Run test:

    $ ./runner --disable-net-host --binary /home/nv/play-clickhouse-data/build/clickhouse-master/programs/clickhouse \
        -- -k test_part_fingerprints -s

"""

import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def prepare_node(node, fingerprints=None):
    node.query("""
    CREATE TABLE t(key UInt64, value UInt64)
    ENGINE MergeTree()
    ORDER BY tuple()
    SETTINGS index_granularity = 1
    """)

    node.query("""
    CREATE TABLE d AS t ENGINE=Distributed(test_cluster, default, t)
    """)

    # Stop merges while populating test data
    node.query("SYSTEM STOP MERGES")

    # Create 5 parts
    for i in range(1, 6):
        node.query("INSERT INTO t VALUES ({}, {})".format(i, i))

    node.query("DETACH TABLE t")

    for part, fingerprint in fingerprints:
        script = """
        echo '{}' > /var/lib/clickhouse/data/default/t/{}/fingerprint.txt
        """.format(fingerprint, part)
        node.exec_in_container(["bash", "-c", script])

    # Attach table back
    node.query("ATTACH TABLE t")

    node.query("SYSTEM START MERGES")
    node.query("OPTIMIZE TABLE t FINAL")

    print(node.name)
    print(node.query("SELECT name, fingerprint FROM system.parts WHERE table = 't' AND active ORDER BY name"))

    # Presence of fingerprints prevents all merges in a partition
    assert '5' == node.query("SELECT count() FROM system.parts WHERE table = 't' AND active").strip()
    assert str(len(fingerprints)) == node.query("SELECT count() FROM system.parts WHERE table = 't' AND length(fingerprint) > 0").strip()


@pytest.fixture(scope="module")
def prepared_cluster(started_cluster):
    prepare_node(node1, fingerprints=[("all_3_3_0", "my_uniq_fp_1")])
    prepare_node(node2, fingerprints=[("all_3_3_0", "my_uniq_fp_1"), ("all_4_4_0", "my_uniq_fp2")])


def test_virtual_column(prepared_cluster):
    # Part containing `key=3` has the same fingerprint on both nodes,
    #   we expect it to be included only once in the end result.;
    # select query is using virtucal column _part_fingerprint to filter out part in one shard
    expected = """
    1	2
    2	2
    3	1
    4	2
    5	2
    """
    assert TSV(expected) == TSV(node1.query("""
    SELECT
        key,
        count() AS c
    FROM d
    WHERE ((_shard_num = 1) AND (_part_fingerprint != 'my_uniq_fp_1')) OR (_shard_num = 2)
    GROUP BY key
    ORDER BY
        key ASC
    """))


def test_basic(prepared_cluster):
    # Part containing `key=3` has the same fingerprint on both nodes,
    #   we expect it to be included only once in the end result.;
    expected = """
1	2
2	2
3	1
4	2
5	2
"""
    assert TSV(expected) == TSV(node1.query("SELECT key, count() c FROM d GROUP BY key ORDER BY key"))

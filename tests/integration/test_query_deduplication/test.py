import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

DUPLICATED_UUID = uuid.uuid4()

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    'node1',
    main_configs=['configs/remote_servers.xml', 'configs/deduplication_settings.xml'],
    user_configs=['configs/profiles.xml'])

node2 = cluster.add_instance(
    'node2',
    main_configs=['configs/remote_servers.xml', 'configs/deduplication_settings.xml'],
    user_configs=['configs/profiles.xml'])

node3 = cluster.add_instance(
    'node3',
    main_configs=['configs/remote_servers.xml', 'configs/deduplication_settings.xml'],
    user_configs=['configs/profiles.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def prepare_node(node, parts_uuid=None):
    node.query("""
    CREATE TABLE t(_prefix UInt8 DEFAULT 0, key UInt64, value UInt64)
    ENGINE MergeTree()
    ORDER BY tuple()
    PARTITION BY _prefix
    SETTINGS index_granularity = 1
    """)

    node.query("""
    CREATE TABLE d AS t ENGINE=Distributed(test_cluster, default, t)
    """)

    # Stop merges while populating test data
    node.query("SYSTEM STOP MERGES")

    # Create 5 parts
    for i in range(1, 6):
        node.query("INSERT INTO t VALUES ({}, {}, {})".format(i, i, i))

    node.query("DETACH TABLE t")

    if parts_uuid:
        for part, part_uuid in parts_uuid:
            script = """
            echo -n '{}' > /var/lib/clickhouse/data/default/t/{}/uuid.txt
            """.format(part_uuid, part)
            node.exec_in_container(["bash", "-c", script])

    # Attach table back
    node.query("ATTACH TABLE t")

    # NOTE:
    # due to absence of the ability to lock part, need to operate on parts with preventin merges
    # node.query("SYSTEM START MERGES")
    # node.query("OPTIMIZE TABLE t FINAL")

    print(node.name)
    print(node.query("SELECT name, uuid, partition FROM system.parts WHERE table = 't' AND active ORDER BY name"))

    assert '5' == node.query("SELECT count() FROM system.parts WHERE table = 't' AND active").strip()
    if parts_uuid:
        for part, part_uuid in parts_uuid:
            assert '1' == node.query(
                "SELECT count() FROM system.parts WHERE table = 't' AND uuid = '{}' AND active".format(
                    part_uuid)).strip()


@pytest.fixture(scope="module")
def prepared_cluster(started_cluster):
    print("duplicated UUID: {}".format(DUPLICATED_UUID))
    prepare_node(node1, parts_uuid=[("3_3_3_0", DUPLICATED_UUID)])
    prepare_node(node2, parts_uuid=[("3_3_3_0", DUPLICATED_UUID)])
    prepare_node(node3)


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
    WHERE ((_shard_num = 1) AND (_part_uuid != '{}')) OR (_shard_num = 2)
    GROUP BY key
    ORDER BY
        key ASC
    """.format(DUPLICATED_UUID)))


def test_with_deduplication(prepared_cluster):
    # Part containing `key=3` has the same fingerprint on both nodes,
    # we expect it to be included only once in the end result
    expected = """
1	3
2	3
3	2
4	3
5	3
"""
    assert TSV(expected) == TSV(node1.query(
        "SET allow_experimental_query_deduplication=1; SELECT key, count() c FROM d GROUP BY key ORDER BY key"))


def test_no_merge_with_deduplication(prepared_cluster):
    # Part containing `key=3` has the same fingerprint on both nodes,
    # we expect it to be included only once in the end result.
    # even with distributed_group_by_no_merge=1 the duplicated part should be excluded from the final result
    expected = """
1	1
2	1
3	1
4	1
5	1
1	1
2	1
3	1
4	1
5	1
1	1
2	1
4	1
5	1
"""
    assert TSV(expected) == TSV(node1.query("SELECT key, count() c FROM d GROUP BY key ORDER BY key", settings={
        "allow_experimental_query_deduplication": 1,
        "distributed_group_by_no_merge": 1,
    }))


def test_without_deduplication(prepared_cluster):
    # Part containing `key=3` has the same fingerprint on both nodes,
    # but allow_experimental_query_deduplication is disabled,
    # so it will not be excluded
    expected = """
1	3
2	3
3	3
4	3
5	3
"""
    assert TSV(expected) == TSV(node1.query(
        "SET allow_experimental_query_deduplication=0; SELECT key, count() c FROM d GROUP BY key ORDER BY key"))

import random
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

s0r0 = cluster.add_instance(
    's0r0',
    main_configs=['configs/remote_servers.xml', 'configs/merge_tree.xml'],
    with_zookeeper=True)

s0r1 = cluster.add_instance(
    's0r1',
    main_configs=['configs/remote_servers.xml', 'configs/merge_tree.xml'],
    with_zookeeper=True)

s1r0 = cluster.add_instance(
    's1r0',
    main_configs=['configs/remote_servers.xml', 'configs/merge_tree.xml'],
    with_zookeeper=True)

s1r1 = cluster.add_instance(
    's1r1',
    main_configs=['configs/remote_servers.xml', 'configs/merge_tree.xml'],
    with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_move(started_cluster):
    for shard_ix, rs in enumerate([[s0r0, s0r1], [s1r0, s1r1]]):
        for replica_ix, r in enumerate(rs):
            r.query("""
            CREATE TABLE test_move(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_move', '{}')
            ORDER BY tuple()
            """.format(shard_ix, replica_ix))

    s0r0.query("SYSTEM STOP MERGES test_move")

    s0r0.query("INSERT INTO test_move VALUES (1)")
    s0r0.query("INSERT INTO test_move VALUES (2)")

    assert "2" == s0r0.query("SELECT count() FROM test_move").strip()
    assert "0" == s1r0.query("SELECT count() FROM test_move").strip()

    s0r0.query("ALTER TABLE test_move MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_move'")

    print(s0r0.query("SELECT * FROM system.part_moves_between_shards"))

    s0r0.query("SYSTEM START MERGES test_move")
    s0r0.query("OPTIMIZE TABLE test_move FINAL")

    while True:
        time.sleep(3)

        print(s0r0.query("SELECT * FROM system.part_moves_between_shards"))

        # Eventually.
        if "DONE" == s0r0.query("SELECT state FROM system.part_moves_between_shards WHERE table = 'test_move'").strip():
            break

    for n in [s0r0, s0r1]:
        assert "1" == n.query("SELECT count() FROM test_move").strip()

    for n in [s1r0, s1r1]:
        assert "1" == n.query("SELECT count() FROM test_move").strip()

    # Move part back
    s1r0.query("ALTER TABLE test_move MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_0/tables/test_move'")

    while True:
        time.sleep(3)

        print(s1r0.query("SELECT * FROM system.part_moves_between_shards"))

        # Eventually.
        if "DONE" == s1r0.query("SELECT state FROM system.part_moves_between_shards WHERE table = 'test_move'").strip():
            break

    for n in [s0r0, s0r1]:
        assert "2" == n.query("SELECT count() FROM test_move").strip()

    for n in [s1r0, s1r1]:
        assert "0" == n.query("SELECT count() FROM test_move").strip()


def test_deduplication_while_move(started_cluster):
    for shard_ix, rs in enumerate([[s0r0, s0r1], [s1r0, s1r1]]):
        for replica_ix, r in enumerate(rs):
            r.query("""
            CREATE TABLE test_deduplication(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_deduplication', '{}')
            ORDER BY tuple()
            """.format(shard_ix, replica_ix))

            r.query("""
            CREATE TABLE t_d AS test_deduplication
            ENGINE Distributed('test_cluster', '', test_deduplication)
            """)

    s0r0.query("SYSTEM STOP MERGES test_deduplication")

    s0r0.query("INSERT INTO test_deduplication VALUES (1)")
    s0r0.query("INSERT INTO test_deduplication VALUES (2)")
    s0r1.query("SYSTEM SYNC REPLICA test_deduplication", timeout=20)

    assert "2" == s0r0.query("SELECT count() FROM test_deduplication").strip()
    assert "0" == s1r0.query("SELECT count() FROM test_deduplication").strip()

    s0r0.query("ALTER TABLE test_deduplication MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_deduplication'")
    s0r0.query("SYSTEM START MERGES test_deduplication")

    expected = """
1
2
"""

    # Verify that we get consisntent result at all times while the part is moving from one shard to another.
    while "DONE" != s0r0.query("SELECT state FROM system.part_moves_between_shards WHERE table = 'test_deduplication' ORDER BY create_time DESC LIMIT 1").strip():
        n = random.choice(list(started_cluster.instances.values()))

        assert TSV(n.query("SELECT * FROM t_d ORDER BY v", settings={
            "allow_experimental_query_deduplication": 1
        })) == TSV(expected)


def test_move_not_permitted(started_cluster):
    for ix, n in enumerate([s0r0, s1r0]):
        n.query("DROP TABLE IF EXISTS not_permitted")
        n.query("""
        CREATE TABLE not_permitted(v_{} UInt64)
        ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/not_permitted', 'r')
        ORDER BY tuple()
        """.format(ix, ix))

    s0r0.query("INSERT INTO not_permitted VALUES (1)")

    with pytest.raises(QueryRuntimeException) as exc:
        s0r0.query("ALTER TABLE not_permitted MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/not_permitted'")

    assert "DB::Exception: Table columns structure in ZooKeeper is different from local table structure." in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        s0r0.query("ALTER TABLE not_permitted MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_0/tables/not_permitted'")

    assert "DB::Exception: Source and destination are the same" in str(exc.value)

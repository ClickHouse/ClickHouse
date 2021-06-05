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


def test_part_move_kill_while_move(started_cluster):
    for shard_ix, rs in enumerate([[s0r0, s0r1], [s1r0, s1r1]]):
        for replica_ix, r in enumerate(rs):
            r.query("""
            CREATE TABLE test_part_move_kill(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_part_move_kill', '{}')
            ORDER BY tuple()
            """.format(shard_ix, replica_ix))

            r.query("""
            CREATE TABLE t_d AS test_part_move_kill
            ENGINE Distributed('test_cluster', currentDatabase(), test_part_move_kill)
            """)

    s0r0.query("SYSTEM STOP MERGES test_part_move_kill")

    s0r0.query("INSERT INTO test_part_move_kill VALUES (1)")
    s0r0.query("INSERT INTO test_part_move_kill VALUES (2)")
    s0r1.query("SYSTEM SYNC REPLICA test_part_move_kill", timeout=20)

    assert "2" == s0r0.query("SELECT count() FROM test_part_move_kill").strip()
    assert "0" == s1r0.query("SELECT count() FROM test_part_move_kill").strip()

    s0r0.query("ALTER TABLE test_part_move_kill MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_part_move_kill'")
    s0r0.query("SYSTEM START MERGES test_part_move_kill")

    # TODO(nv): Write the test.
    #   ...
    #   But how?
    #       Shutdown one of destination replica to make waiting for fetch fail.
    #       Assert on last_exception
    #
    #           Start a mutation while fetch is blocked to make a later attach
    #           fail.
    #
    #           Rollback.

    s0r0.query("""
        KILL PART_MOVE_TO_SHARD
        WHERE task_uuid = (SELECT task_uuid FROM system.part_moves_between_shards WHERE table = 'test_part_move_kill')
    """)

    while True:
        state = s0r0.query("SELECT state FROM system.part_moves_between_shards WHERE table = 'test_part_move_kill'").strip()
        print("Current state: {}".format(state))

        if state == "CANCELLED":
            break

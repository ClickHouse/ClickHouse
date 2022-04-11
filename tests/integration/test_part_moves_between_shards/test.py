import pytest
import random
import threading
import time

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

transient_ch_errors = [23, 32, 210]

cluster = ClickHouseCluster(__file__)

s0r0 = cluster.add_instance(
    "s0r0",
    main_configs=["configs/remote_servers.xml", "configs/merge_tree.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

s0r1 = cluster.add_instance(
    "s0r1",
    main_configs=["configs/remote_servers.xml", "configs/merge_tree.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

s1r0 = cluster.add_instance(
    "s1r0",
    main_configs=["configs/remote_servers.xml", "configs/merge_tree.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

s1r1 = cluster.add_instance(
    "s1r1",
    main_configs=["configs/remote_servers.xml", "configs/merge_tree.xml"],
    stay_alive=True,
    with_zookeeper=True,
)


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
            r.query(
                """
            DROP TABLE IF EXISTS test_move;
            CREATE TABLE test_move(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_move', '{}')
            ORDER BY tuple()
            """.format(
                    shard_ix, r.name
                )
            )

    s0r0.query("SYSTEM STOP MERGES test_move")
    s0r1.query("SYSTEM STOP MERGES test_move")

    s0r0.query("INSERT INTO test_move VALUES (1)")
    s0r0.query("INSERT INTO test_move VALUES (2)")

    assert "2" == s0r0.query("SELECT count() FROM test_move").strip()
    assert "0" == s1r0.query("SELECT count() FROM test_move").strip()

    s0r0.query(
        "ALTER TABLE test_move MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_move'"
    )

    print(s0r0.query("SELECT * FROM system.part_moves_between_shards"))

    s0r0.query("SYSTEM START MERGES test_move")
    s0r0.query("OPTIMIZE TABLE test_move FINAL")

    wait_for_state("DONE", s0r0, "test_move")

    for n in [s0r0, s0r1]:
        assert "1" == n.query("SELECT count() FROM test_move").strip()

    for n in [s1r0, s1r1]:
        assert "1" == n.query("SELECT count() FROM test_move").strip()

    # Move part back
    s1r0.query(
        "ALTER TABLE test_move MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_0/tables/test_move'"
    )

    wait_for_state("DONE", s1r0, "test_move")

    for n in [s0r0, s0r1]:
        assert "2" == n.query("SELECT count() FROM test_move").strip()

    for n in [s1r0, s1r1]:
        assert "0" == n.query("SELECT count() FROM test_move").strip()


def test_deduplication_while_move(started_cluster):
    for shard_ix, rs in enumerate([[s0r0, s0r1], [s1r0, s1r1]]):
        for replica_ix, r in enumerate(rs):
            r.query(
                """
            DROP TABLE IF EXISTS test_deduplication;
            CREATE TABLE test_deduplication(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_deduplication', '{}')
            ORDER BY tuple()
            """.format(
                    shard_ix, r.name
                )
            )

            r.query(
                """
            DROP TABLE IF EXISTS test_deduplication_d;
            CREATE TABLE test_deduplication_d AS test_deduplication
            ENGINE Distributed('test_cluster', '', test_deduplication)
            """
            )

    s0r0.query("SYSTEM STOP MERGES test_deduplication")
    s0r1.query("SYSTEM STOP MERGES test_deduplication")

    s0r0.query("INSERT INTO test_deduplication VALUES (1)")
    s0r0.query("INSERT INTO test_deduplication VALUES (2)")
    s0r1.query("SYSTEM SYNC REPLICA test_deduplication", timeout=20)

    assert "2" == s0r0.query("SELECT count() FROM test_deduplication").strip()
    assert "0" == s1r0.query("SELECT count() FROM test_deduplication").strip()

    s0r0.query(
        "ALTER TABLE test_deduplication MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_deduplication'"
    )
    s0r0.query("SYSTEM START MERGES test_deduplication")

    expected = """
1
2
"""

    def deduplication_invariant_test():
        n = random.choice(list(started_cluster.instances.values()))
        assert TSV(
            n.query(
                "SELECT * FROM test_deduplication_d ORDER BY v",
                settings={"allow_experimental_query_deduplication": 1},
            )
        ) == TSV(expected)

        # https://github.com/ClickHouse/ClickHouse/issues/34089
        assert TSV(
            n.query(
                "SELECT count() FROM test_deduplication_d",
                settings={"allow_experimental_query_deduplication": 1},
            )
        ) == TSV("2")

        assert TSV(
            n.query(
                "SELECT count() FROM test_deduplication_d",
                settings={
                    "allow_experimental_query_deduplication": 1,
                    "allow_experimental_projection_optimization": 1,
                },
            )
        ) == TSV("2")

    deduplication_invariant = ConcurrentInvariant(deduplication_invariant_test)
    deduplication_invariant.start()

    wait_for_state("DONE", s0r0, "test_deduplication")

    deduplication_invariant.stop_and_assert_no_exception()


def test_part_move_step_by_step(started_cluster):
    for shard_ix, rs in enumerate([[s0r0, s0r1], [s1r0, s1r1]]):
        for replica_ix, r in enumerate(rs):
            r.query(
                """
            DROP TABLE IF EXISTS test_part_move_step_by_step;
            CREATE TABLE test_part_move_step_by_step(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_part_move_step_by_step', '{}')
            ORDER BY tuple()
            """.format(
                    shard_ix, r.name
                )
            )

            r.query(
                """
            DROP TABLE IF EXISTS test_part_move_step_by_step_d;
            CREATE TABLE test_part_move_step_by_step_d AS test_part_move_step_by_step
            ENGINE Distributed('test_cluster', currentDatabase(), test_part_move_step_by_step)
            """
            )

    s0r0.query("SYSTEM STOP MERGES test_part_move_step_by_step")
    s0r1.query("SYSTEM STOP MERGES test_part_move_step_by_step")

    s0r0.query("INSERT INTO test_part_move_step_by_step VALUES (1)")
    s0r0.query("INSERT INTO test_part_move_step_by_step VALUES (2)")
    s0r1.query("SYSTEM SYNC REPLICA test_part_move_step_by_step", timeout=20)

    assert "2" == s0r0.query("SELECT count() FROM test_part_move_step_by_step").strip()
    assert "0" == s1r0.query("SELECT count() FROM test_part_move_step_by_step").strip()

    expected = """
1
2
"""

    def deduplication_invariant_test():
        n = random.choice(list(started_cluster.instances.values()))
        try:
            assert TSV(
                n.query(
                    "SELECT * FROM test_part_move_step_by_step_d ORDER BY v",
                    settings={"allow_experimental_query_deduplication": 1},
                )
            ) == TSV(expected)
        except QueryRuntimeException as e:
            # ignore transient errors that are caused by us restarting nodes
            if e.returncode not in transient_ch_errors:
                raise e

    deduplication_invariant = ConcurrentInvariant(deduplication_invariant_test)
    deduplication_invariant.start()

    # Stop a source replica to prevent SYNC_SOURCE succeeding.
    s0r1.stop_clickhouse()

    s0r0.query(
        "ALTER TABLE test_part_move_step_by_step MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_part_move_step_by_step'"
    )

    # Should hang on SYNC_SOURCE until all source replicas acknowledge new pinned UUIDs.
    wait_for_state(
        "SYNC_SOURCE",
        s0r0,
        "test_part_move_step_by_step",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Start all replicas in source shard but stop a replica in destination shard
    # to prevent SYNC_DESTINATION succeeding.
    s1r1.stop_clickhouse()
    s0r1.start_clickhouse()

    # After SYNC_SOURCE step no merges will be assigned.
    s0r0.query(
        "SYSTEM START MERGES test_part_move_step_by_step; OPTIMIZE TABLE test_part_move_step_by_step;"
    )
    s0r1.query(
        "SYSTEM START MERGES test_part_move_step_by_step; OPTIMIZE TABLE test_part_move_step_by_step;"
    )

    wait_for_state(
        "SYNC_DESTINATION",
        s0r0,
        "test_part_move_step_by_step",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Start previously stopped replica in destination shard to let SYNC_DESTINATION
    # succeed.
    # Stop the other replica in destination shard to prevent DESTINATION_FETCH succeed.
    s1r0.stop_clickhouse()
    s1r1.start_clickhouse()
    wait_for_state(
        "DESTINATION_FETCH",
        s0r0,
        "test_part_move_step_by_step",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Start previously stopped replica in destination shard to let DESTINATION_FETCH
    # succeed.
    # Stop the other replica in destination shard to prevent DESTINATION_ATTACH succeed.
    s1r1.stop_clickhouse()
    s1r0.start_clickhouse()
    wait_for_state(
        "DESTINATION_ATTACH",
        s0r0,
        "test_part_move_step_by_step",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Start all replicas in destination shard to let DESTINATION_ATTACH succeed.
    # Stop a source replica to prevent SOURCE_DROP succeeding.
    s0r0.stop_clickhouse()
    s1r1.start_clickhouse()
    wait_for_state(
        "SOURCE_DROP",
        s0r1,
        "test_part_move_step_by_step",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    s0r0.start_clickhouse()
    wait_for_state("DONE", s0r1, "test_part_move_step_by_step")
    deduplication_invariant.assert_no_exception()

    # No hung tasks in replication queue. Would timeout otherwise.
    for instance in started_cluster.instances.values():
        instance.query("SYSTEM SYNC REPLICA test_part_move_step_by_step")

    assert "1" == s0r0.query("SELECT count() FROM test_part_move_step_by_step").strip()
    assert "1" == s1r0.query("SELECT count() FROM test_part_move_step_by_step").strip()

    deduplication_invariant.stop_and_assert_no_exception()


def test_part_move_step_by_step_kill(started_cluster):
    for shard_ix, rs in enumerate([[s0r0, s0r1], [s1r0, s1r1]]):
        for replica_ix, r in enumerate(rs):
            r.query(
                """
            DROP TABLE IF EXISTS test_part_move_step_by_step_kill;
            CREATE TABLE test_part_move_step_by_step_kill(v UInt64)
            ENGINE ReplicatedMergeTree('/clickhouse/shard_{}/tables/test_part_move_step_by_step_kill', '{}')
            ORDER BY tuple()
            """.format(
                    shard_ix, r.name
                )
            )

            r.query(
                """
            DROP TABLE IF EXISTS test_part_move_step_by_step_kill_d;
            CREATE TABLE test_part_move_step_by_step_kill_d AS test_part_move_step_by_step_kill
            ENGINE Distributed('test_cluster', currentDatabase(), test_part_move_step_by_step_kill)
            """
            )

    s0r0.query("SYSTEM STOP MERGES test_part_move_step_by_step_kill")
    s0r1.query("SYSTEM STOP MERGES test_part_move_step_by_step_kill")

    s0r0.query("INSERT INTO test_part_move_step_by_step_kill VALUES (1)")
    s0r0.query("INSERT INTO test_part_move_step_by_step_kill VALUES (2)")
    s0r1.query("SYSTEM SYNC REPLICA test_part_move_step_by_step_kill", timeout=20)

    assert (
        "2"
        == s0r0.query("SELECT count() FROM test_part_move_step_by_step_kill").strip()
    )
    assert (
        "0"
        == s1r0.query("SELECT count() FROM test_part_move_step_by_step_kill").strip()
    )

    expected = """
1
2
"""

    def deduplication_invariant_test():
        n = random.choice(list(started_cluster.instances.values()))
        try:
            assert TSV(
                n.query(
                    "SELECT * FROM test_part_move_step_by_step_kill_d ORDER BY v",
                    settings={"allow_experimental_query_deduplication": 1},
                )
            ) == TSV(expected)
        except QueryRuntimeException as e:
            # ignore transient errors that are caused by us restarting nodes
            if e.returncode not in transient_ch_errors:
                raise e

    deduplication_invariant = ConcurrentInvariant(deduplication_invariant_test)
    deduplication_invariant.start()

    # Stop a source replica to prevent SYNC_SOURCE succeeding.
    s0r1.stop_clickhouse()

    s0r0.query(
        "ALTER TABLE test_part_move_step_by_step_kill MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/test_part_move_step_by_step_kill'"
    )

    # Should hang on SYNC_SOURCE until all source replicas acknowledge new pinned UUIDs.
    wait_for_state(
        "SYNC_SOURCE",
        s0r0,
        "test_part_move_step_by_step_kill",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Start all replicas in source shard but stop a replica in destination shard
    # to prevent SYNC_DESTINATION succeeding.
    s1r1.stop_clickhouse()
    s0r1.start_clickhouse()

    # After SYNC_SOURCE step no merges will be assigned.
    s0r0.query(
        "SYSTEM START MERGES test_part_move_step_by_step_kill; OPTIMIZE TABLE test_part_move_step_by_step_kill;"
    )
    s0r1.query(
        "SYSTEM START MERGES test_part_move_step_by_step_kill; OPTIMIZE TABLE test_part_move_step_by_step_kill;"
    )

    wait_for_state(
        "SYNC_DESTINATION",
        s0r0,
        "test_part_move_step_by_step_kill",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Start previously stopped replica in destination shard to let SYNC_DESTINATION
    # succeed.
    # Stop the other replica in destination shard to prevent DESTINATION_FETCH succeed.
    s1r0.stop_clickhouse()
    s1r1.start_clickhouse()
    wait_for_state(
        "DESTINATION_FETCH",
        s0r0,
        "test_part_move_step_by_step_kill",
        "Some replicas haven\\'t processed event",
    )

    # Start previously stopped replica in destination shard to let DESTINATION_FETCH
    # succeed.
    # Stop the other replica in destination shard to prevent DESTINATION_ATTACH succeed.
    s1r1.stop_clickhouse()
    s1r0.start_clickhouse()
    wait_for_state(
        "DESTINATION_ATTACH",
        s0r0,
        "test_part_move_step_by_step_kill",
        "Some replicas haven\\'t processed event",
    )
    deduplication_invariant.assert_no_exception()

    # Rollback here.
    s0r0.query(
        """
        KILL PART_MOVE_TO_SHARD
        WHERE task_uuid = (SELECT task_uuid FROM system.part_moves_between_shards WHERE table = 'test_part_move_step_by_step_kill')
    """
    )

    wait_for_state(
        "DESTINATION_ATTACH",
        s0r0,
        "test_part_move_step_by_step_kill",
        assert_exception_msg="Some replicas haven\\'t processed event",
        assert_rollback=True,
    )

    s1r1.start_clickhouse()

    wait_for_state(
        "CANCELLED", s0r0, "test_part_move_step_by_step_kill", assert_rollback=True
    )
    deduplication_invariant.assert_no_exception()

    # No hung tasks in replication queue. Would timeout otherwise.
    for instance in started_cluster.instances.values():
        instance.query("SYSTEM SYNC REPLICA test_part_move_step_by_step_kill")

    assert (
        "2"
        == s0r0.query("SELECT count() FROM test_part_move_step_by_step_kill").strip()
    )
    assert (
        "0"
        == s1r0.query("SELECT count() FROM test_part_move_step_by_step_kill").strip()
    )

    deduplication_invariant.stop_and_assert_no_exception()


def test_move_not_permitted(started_cluster):
    # Verify that invariants for part compatibility are checked.

    # Tests are executed in order. Make sure cluster is up if previous test
    # failed.
    s0r0.start_clickhouse()
    s1r0.start_clickhouse()

    for ix, n in enumerate([s0r0, s1r0]):
        n.query(
            """
        DROP TABLE IF EXISTS not_permitted_columns;
        
        CREATE TABLE not_permitted_columns(v_{ix} UInt64)
        ENGINE ReplicatedMergeTree('/clickhouse/shard_{ix}/tables/not_permitted_columns', 'r')
        ORDER BY tuple();
        """.format(
                ix=ix
            )
        )

        partition = "date"
        if ix > 0:
            partition = "v"

        n.query(
            """
        DROP TABLE IF EXISTS not_permitted_partition;
        CREATE TABLE not_permitted_partition(date Date, v UInt64)
        ENGINE ReplicatedMergeTree('/clickhouse/shard_{ix}/tables/not_permitted_partition', 'r')
        PARTITION BY ({partition})
        ORDER BY tuple();
        """.format(
                ix=ix, partition=partition
            )
        )

    s0r0.query("INSERT INTO not_permitted_columns VALUES (1)")
    s0r0.query("INSERT INTO not_permitted_partition VALUES ('2021-09-03', 1)")

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Source and destination are the same",
    ):
        s0r0.query(
            "ALTER TABLE not_permitted_columns MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_0/tables/not_permitted_columns'"
        )

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Table columns structure in ZooKeeper is different from local table structure.",
    ):
        s0r0.query(
            "ALTER TABLE not_permitted_columns MOVE PART 'all_0_0_0' TO SHARD '/clickhouse/shard_1/tables/not_permitted_columns'"
        )

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Existing table metadata in ZooKeeper differs in partition key expression.",
    ):
        s0r0.query(
            "ALTER TABLE not_permitted_partition MOVE PART '20210903_0_0_0' TO SHARD '/clickhouse/shard_1/tables/not_permitted_partition'"
        )


def wait_for_state(
    desired_state,
    instance,
    test_table,
    assert_exception_msg=None,
    assert_rollback=False,
):
    last_debug_print_time = time.time()

    print("Waiting to reach state: {}".format(desired_state))
    if assert_exception_msg:
        print("  with exception contents: {}".format(assert_exception_msg))
    if assert_rollback:
        print("     and rollback: {}".format(assert_rollback))

    while True:
        tasks = TSV.toMat(
            instance.query(
                "SELECT state, num_tries, last_exception, rollback FROM system.part_moves_between_shards WHERE table = '{}'".format(
                    test_table
                )
            )
        )
        assert len(tasks) == 1, "only one task expected in this test"

        if time.time() - last_debug_print_time > 30:
            last_debug_print_time = time.time()
            print("Current state: ", tasks)

        [state, num_tries, last_exception, rollback] = tasks[0]

        if state == desired_state:
            if assert_exception_msg and int(num_tries) < 3:
                # Let the task be retried a few times when expecting an exception
                # to make sure the exception is persistent and the code doesn't
                # accidentally continue to run when we expect it not to.
                continue

            if assert_exception_msg:
                assert assert_exception_msg in last_exception

            if assert_rollback:
                assert int(rollback) == 1, "rollback bit isn't set"

            break
        elif state in ["DONE", "CANCELLED"]:
            raise Exception(
                "Reached terminal state {}, but was waiting for {}".format(
                    state, desired_state
                )
            )

        time.sleep(0.1)


class ConcurrentInvariant:
    def __init__(self, invariant_test, loop_sleep=0.1):
        self.invariant_test = invariant_test
        self.loop_sleep = loop_sleep

        self.started = False
        self.exiting = False
        self.exception = None
        self.thread = threading.Thread(target=self._loop)

    def start(self):
        if self.started:
            raise Exception("invariant thread already started")

        self.started = True
        self.thread.start()

    def stop_and_assert_no_exception(self):
        self._assert_started()

        self.exiting = True
        self.thread.join()

        if self.exception:
            raise self.exception

    def assert_no_exception(self):
        self._assert_started()

        if self.exception:
            raise self.exception

    def _loop(self):
        try:
            while not self.exiting:
                self.invariant_test()
                time.sleep(self.loop_sleep)
        except Exception as e:
            self.exiting = True
            self.exception = e

    def _assert_started(self):
        if not self.started:
            raise Exception("invariant thread not started, forgot to call start?")

"""
Regression test for phantom entries in mutations' parts_to_do.

When PartCheckThread re-enqueues a GET_PART for a part that has already
been mutated, addPartToMutations re-adds the old part to parts_to_do.
On successful (skipped) completion, removePartsCoveredBy does not remove
the part itself, leaving the mutation stuck forever.

This test runs concurrent inserts, mutations, and part corruption+checks
to increase the chance of hitting the race condition.
"""

import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_phantom_parts_to_do_in_mutations(started_cluster):
    node.query(
        """
        CREATE TABLE phantom_r1 (key UInt64, value String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/phantom', 'r1')
        ORDER BY key
        SETTINGS old_parts_lifetime = 1,
                 cleanup_delay_period = 1,
                 cleanup_delay_period_random_add = 0,
                 cleanup_thread_preferred_points_per_iteration = 0;

        CREATE TABLE phantom_r2 (key UInt64, value String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/phantom', 'r2')
        ORDER BY key
        SETTINGS old_parts_lifetime = 1,
                 cleanup_delay_period = 1,
                 cleanup_delay_period_random_add = 0,
                 cleanup_thread_preferred_points_per_iteration = 0;

        SYSTEM SYNC REPLICA phantom_r1;
        SYSTEM SYNC REPLICA phantom_r2;
        """
    )

    insert_count = [0]
    stop = [False]

    def insert_thread(_):
        i = 0
        while not stop[0]:
            try:
                node.query(
                    f"INSERT INTO phantom_r1 VALUES ({i * 100}, 'data')"
                )
            except Exception:
                pass
            i += 1
        insert_count[0] = i

    def mutation_thread(_):
        while not stop[0]:
            try:
                node.query(
                    "ALTER TABLE phantom_r1 DELETE WHERE rand() % 10 = 0"
                )
            except Exception:
                pass
            time.sleep(0.1)

    def corrupt_and_check_thread(_):
        while not stop[0]:
            try:
                part_path = node.query(
                    """
                    SELECT path FROM system.parts
                    WHERE database = 'default' AND table = 'phantom_r2' AND active
                    ORDER BY rand() LIMIT 1
                    """
                ).strip()

                if part_path:
                    node.exec_in_container(
                        [
                            "bash",
                            "-c",
                            f"rm -f {part_path}data.bin {part_path}data.cmrk3",
                        ],
                        privileged=True,
                    )

                node.query(
                    "CHECK TABLE phantom_r2",
                    settings={
                        "check_query_single_value_result": 0,
                        "max_threads": 1,
                    },
                )
            except Exception:
                pass

            time.sleep(0.2)

    timeout = 15

    with Pool(3) as pool:
        r_insert = pool.apply_async(insert_thread, (None,))
        r_mutation = pool.apply_async(mutation_thread, (None,))
        r_corrupt = pool.apply_async(corrupt_and_check_thread, (None,))

        time.sleep(timeout)
        stop[0] = True

        r_insert.get()
        r_mutation.get()
        r_corrupt.get()

    # Kill stuck mutations and wait for running queries to finish
    node.query("KILL MUTATION WHERE database = 'default' AND table LIKE 'phantom_r%'")

    for _ in range(60):
        running = int(
            node.query(
                "SELECT count() FROM system.processes WHERE query LIKE '%phantom_r%' AND query NOT LIKE '%system.processes%'"
            ).strip()
        )
        if running == 0:
            break
        time.sleep(1)

    # Sync replicas to verify replication consistency
    node.query("SYSTEM SYNC REPLICA phantom_r1", timeout=120)
    node.query("SYSTEM SYNC REPLICA phantom_r2", timeout=120)

    r1_count = node.query("SELECT count(), sum(key) FROM phantom_r1").strip()
    r2_count = node.query("SELECT count(), sum(key) FROM phantom_r2").strip()
    assert r1_count == r2_count, f"Replicas diverged: r1={r1_count}, r2={r2_count}"

    node.query("DROP TABLE phantom_r1 SYNC")
    node.query("DROP TABLE phantom_r2 SYNC")

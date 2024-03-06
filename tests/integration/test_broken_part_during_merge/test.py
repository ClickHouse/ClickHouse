import pytest

from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk
import time

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_merge_and_part_corruption(started_cluster):
    node1.query(
        """
        CREATE TABLE replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt', '{replica}') ORDER BY id
        SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
            replica=node1.name
        )
    )

    node1.query("SYSTEM STOP REPLICATION QUEUES replicated_mt")
    for i in range(4):
        node1.query(
            "INSERT INTO replicated_mt SELECT toDate('2019-10-01'), number, number * number FROM numbers ({f}, 100000)".format(
                f=i * 100000
            )
        )

    assert (
        node1.query(
            "SELECT COUNT() FROM system.parts WHERE table='replicated_mt' AND active=1"
        )
        == "4\n"
    )

    # Need to corrupt "border part" (left or right). If we will corrupt something in the middle
    # clickhouse will not consider merge as broken, because we have parts with the same min and max
    # block numbers.
    corrupt_part_data_on_disk(node1, "replicated_mt", "all_3_3_0")

    with Pool(1) as p:

        def optimize_with_delay(x):
            node1.query("OPTIMIZE TABLE replicated_mt FINAL", timeout=30)

        # corrupt part after merge already assigned, but not started
        res_opt = p.apply_async(optimize_with_delay, (1,))
        node1.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        # start merge
        node1.query("SYSTEM START REPLICATION QUEUES replicated_mt")
        res_opt.get()

        # will hung if checked bug not fixed
        node1.query(
            "ALTER TABLE replicated_mt UPDATE value = 7 WHERE 1",
            settings={"mutations_sync": 2},
            timeout=30,
        )
        assert node1.query("SELECT sum(value) FROM replicated_mt") == "2100000\n"

    node1.query("DROP TABLE replicated_mt SYNC")

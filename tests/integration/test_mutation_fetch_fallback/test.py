import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for i, node in enumerate([node1, node2]):
            # node 2 will corrupt checksums, it must be down while node 1 commits mutation
            # with correct checksums to zookeeper.
            node.query_with_retry(
                """CREATE TABLE fetch_fallback (k int, v int, z int)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/t0', '{}')
                ORDER BY tuple()
                SETTINGS testing_mutate_corrupt_checksums = {}""".format(i, i == 1)
            )
        yield cluster

    finally:
        cluster.shutdown()


def test_mutation_fetch_fallback(start_cluster):
    node1.query("INSERT INTO fetch_fallback(k, v) VALUES (1, 3), (2, 7), (3, 4)")

    node2.stop_clickhouse()
    node1.query("ALTER TABLE fetch_fallback DROP COLUMN z", settings={"mutations_sync": 1})

    node2.start_clickhouse()
    node1.query("SYSTEM SYNC REPLICA fetch_fallback", timeout=10)
    node2.query("SYSTEM SYNC REPLICA fetch_fallback", timeout=10)

    node2.contains_in_log("We will download merged part from replica to force byte-identical result.")

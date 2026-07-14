from contextlib import contextmanager

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query(
                f"""
                CREATE TABLE local_table(id UInt32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/max_rows_read_leaf', '{node}') PARTITION BY toYYYYMM(d) ORDER BY d;

                CREATE TABLE distributed_table(id UInt32, d DateTime) ENGINE = Distributed(two_shards, default, local_table);

                CREATE OR REPLACE VIEW test_view AS select id from distributed_table;
"""
            )
        node1.query(
            "INSERT INTO local_table (id) select * from system.numbers limit 200"
        )
        node2.query(
            "INSERT INTO local_table (id) select * from system.numbers limit 200"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_max_rows_to_read_leaf_via_view(started_cluster):
    """
    Asserts the expected behaviour that we should be able to select
    the total amount of rows (400 -  200 from each shard) from a
    view that selects from a distributed table.
    """
    assert (
        node1.query(
            "SELECT count() from test_view SETTINGS max_rows_to_read_leaf=200, prefer_localhost_replica=0"
        ).rstrip()
        == "400"
    )
    with pytest.raises(
        QueryRuntimeException, match="controlled by 'max_rows_to_read_leaf'"
    ):
        # insert some more data and ensure we get a legitimate failure
        node2.query(
            "INSERT INTO local_table (id) select * from system.numbers limit 10"
        )
        node2.query(
            "SELECT count() from test_view SETTINGS max_rows_to_read_leaf=200, prefer_localhost_replica=0"
        )


if __name__ == "__main__":
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")

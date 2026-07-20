import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{num}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for num in range(6)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables(table_name):
    for idx, node in enumerate(nodes):
        node.query(
            f"DROP TABLE IF EXISTS {table_name}",
            settings={"database_atomic_wait_for_drop_and_detach_synchronously": True},
        )

        node.query(
            f"""
            CREATE TABLE {table_name} (value Int64)
            Engine=ReplicatedMergeTree('/test_parallel_replicas/shard/{table_name}', '{idx}')
            ORDER BY ()
            """
        )

    nodes[0].query(
        f"INSERT INTO {table_name} SELECT * FROM numbers(1000)",
        settings={"insert_deduplicate": 0},
    )
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {table_name}")

    for idx, node in enumerate(nodes):
        node.query("SYSTEM STOP REPLICATED SENDS")
        # the same data on all nodes except for a single value
        node.query(
            f"INSERT INTO {table_name} VALUES ({idx})",
            settings={"insert_deduplicate": 0},
        )


# check that we use the state of data parts from the initiator node (for some sort of determinism of what is been read).
# currently it is implemented only when we build local plan for the initiator node (we aim to make this behavior default)
def test_initiator_snapshot_is_used_for_reading(start_cluster):
    table_name = "t"
    _create_tables(table_name)

    for idx, node in enumerate(nodes):
        expected = 499500 + idx  # sum of all integers 0..999 + idx
        assert (
            node.query(
                f"SELECT sum(value) FROM {table_name}",
                settings={
                    "allow_experimental_parallel_reading_from_replicas": 2,
                    "max_parallel_replicas": 100,
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "parallel_replicas_local_plan": True,
                },
            )
            == f"{expected}\n"
        )

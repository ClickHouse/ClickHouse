import pytest
import re

from helpers.cluster import ClickHouseCluster
from random import randint

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
        macros={"replica": f"node{num}", "shard": "shard"},
    )
    for num in range(3)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables(table_name):
    nodes[0].query(
        f"DROP TABLE IF EXISTS {table_name} ON CLUSTER 'parallel_replicas'",
        settings={"database_atomic_wait_for_drop_and_detach_synchronously": True},
    )

    # big number of granules + low total size in bytes = super tiny granules = big min_marks_per_task
    # => big mark_segment_size will be chosen. it is not required to be big, just not equal to the default
    nodes[0].query(
        f"""
        CREATE TABLE {table_name} ON CLUSTER 'parallel_replicas' (value Int64)
        Engine=ReplicatedMergeTree('/test_parallel_replicas/shard/{table_name}', '{{replica}}')
        ORDER BY ()
        SETTINGS index_granularity = 1
        """
    )

    nodes[0].query(f"INSERT INTO {table_name} SELECT 42 FROM numbers(1000)")
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {table_name}")


# now mark_segment_size is part of the protocol and is communicated to the initiator.
# let's check that the correct value is actually used by the coordinator
def test_mark_segment_size_communicated_correctly(start_cluster):
    table_name = "t"
    _create_tables(table_name)

    for local_plan in [0, 1]:
        query_id = f"query_id_{randint(0, 1000000)}"
        nodes[0].query(
            f"SELECT sum(value) FROM {table_name}",
            settings={
                "allow_experimental_parallel_reading_from_replicas": 2,
                "max_parallel_replicas": 100,
                "cluster_for_parallel_replicas": "parallel_replicas",
                "parallel_replicas_mark_segment_size": 0,
                "parallel_replicas_local_plan": local_plan,
                "query_id": query_id,
            },
        )

        nodes[0].query("SYSTEM FLUSH LOGS")
        log_line = nodes[0].grep_in_log(
            f"{query_id}.*Reading state is fully initialized"
        )
        assert re.search(r"mark_segment_size: (\d+)", log_line).group(1) == "16384"

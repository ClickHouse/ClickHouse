# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query(
            """
        create table t (number UInt64)
        engine = Distributed(test_cluster_two_shards, system, numbers)
        """
        )
        yield cluster

    finally:
        cluster.shutdown()


def test_filled_async_drain_connection_pool(started_cluster):
    def execute_queries(_):
        for _ in range(100):
            node.query(
                "select * from t where number = 0 limit 2",
                settings={
                    "sleep_in_receive_cancel_ms": int(10e6),
                    "max_execution_time": 5,
                    # decrease drain_timeout to make test more stable
                    # (another way is to increase max_execution_time, but this will make test slower)
                    "drain_timeout": 1,
                },
            )

    any(map(execute_queries, range(10)))

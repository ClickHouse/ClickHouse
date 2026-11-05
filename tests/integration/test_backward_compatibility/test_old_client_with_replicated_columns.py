import logging

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
upstream = cluster.add_instance(
    "upstream", user_configs=["configs/enable_lazy_columns_replication.xml"]
)
backward = cluster.add_instance(
    "backward",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_states(start_cluster):
    result = backward.query(
        """
                   select materialize('a'), arrayJoin(range(number))
                   from remote('127.0.0.2', system.numbers_mt) limit 3
                   """
    )

    assert result == "a\t0\na\t0\na\t1\n"

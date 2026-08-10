import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

KEEPER_FEATURE_FLAGS = ["multi_read", "list_with_stat_and_data", "create_ttl"]

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=KEEPER_FEATURE_FLAGS,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    keeper_required_feature_flags=KEEPER_FEATURE_FLAGS,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_status_on_both_nodes(query_id, status):
    for node in [node1, node2]:
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.background_queries WHERE query_id = '{query_id}'",
            status,
            retry_count=120,
        )


def test_killed_server_shows_unknown(started_cluster):
    query_id = f"bgq_killed_{uuid.uuid4()}"
    node1.query(
        "SELECT count() FROM numbers(100000) WHERE NOT ignore(sleepEachRow(0.1)) SETTINGS max_block_size = 1",
        settings={"run_query_in_background": 1},
        query_id=query_id,
    )
    wait_for_status_on_both_nodes(query_id, "Running")

    node1.stop_clickhouse(kill=True)
    node1.start_clickhouse()

    wait_for_status_on_both_nodes(query_id, "Unknown")

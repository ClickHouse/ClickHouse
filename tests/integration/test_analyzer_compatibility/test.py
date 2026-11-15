import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

CLICKHOUSE_MAX_VERSION_WITH_ANALYZER_DISABLED_BY_DEFAULT = "24.2"

cluster = ClickHouseCluster(__file__)
current = cluster.add_instance(
    "current",
    main_configs=["configs/remote_servers.xml"],
)
# An old replica whose profile disables the analyzer.
backward = cluster.add_instance(
    "backward",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/old_analyzer.xml"],
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_MAX_VERSION_WITH_ANALYZER_DISABLED_BY_DEFAULT,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_new_initiator_enables_analyzer_on_old_replica(start_cluster):
    # The initiator marks `allow_experimental_analyzer = 1` as changed when it sends a query to a
    # replica, so an old replica with the analyzer disabled in its profile analyzes the query the
    # same way as the initiator.
    query_id = str(uuid.uuid4())
    current.query(
        "SELECT name FROM clusterAllReplicas('test_cluster_mixed', system.tables) SETTINGS serialize_query_plan = 0",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    analyzer_enabled = backward.query(
        f"""
SELECT DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}'"""
    )
    assert TSV(analyzer_enabled) == TSV("1")

import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

CLICKHOUSE_MAX_VERSION_WITH_ANALYZER_DISABLED_BY_DEFAULT = "24.2"

cluster = ClickHouseCluster(__file__)
# Here the analyzer is the only query analysis there is.
current = cluster.add_instance(
    "current",
    main_configs=["configs/remote_servers.xml"],
)
# Here the analyzer is disabled by default. The value is pinned explicitly all the same: only a
# changed setting is sent to a remote server, and this test is about what the other server does with
# the value it is sent.
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


def test_two_new_versions(start_cluster):
    # Two versions of ClickHouse in one cluster: one that analyzes a query the only supported way,
    # and one old enough to still have the query analysis that was retired in 26.9.

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    query_id = str(uuid.uuid4())
    current.query(
        "SELECT name FROM clusterAllReplicas('test_cluster_mixed', system.tables) settings serialize_query_plan=0;",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    assert (
        current.query(
            """
SELECT hostname() AS h, getSetting('allow_experimental_analyzer')
FROM clusterAllReplicas('test_cluster_mixed', system.one)
ORDER BY h settings serialize_query_plan=0;"""
        )
        == TSV([["backward", "true"], ["current", "true"]])
    )

    # The initiator turns the analyzer on explicitly on the old instance.
    analyzer_enabled = backward.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}';"""
    )

    assert TSV(analyzer_enabled) == TSV("1")

    query_id = str(uuid.uuid4())
    backward.query(
        "SELECT name FROM clusterAllReplicas('test_cluster_mixed', system.tables)",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    # The other direction: the old initiator sends `allow_experimental_analyzer = 0` along with the
    # query, because that is how it analyzes the query itself. Since 26.9 this instance has no other
    # query analysis to fall back to, so it ignores the value instead of agreeing with the initiator.
    # (The old version does not know the `enable_analyzer` alias, hence the canonical name here.)
    assert (
        backward.query(
            """
SELECT hostname() AS h, getSetting('allow_experimental_analyzer')
FROM clusterAllReplicas('test_cluster_mixed', system.one)
ORDER BY h;"""
        )
        == TSV([["backward", "false"], ["current", "true"]])
    )

    # And the value it recorded for its part of the query says so.
    analyzer_enabled = current.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish';"""
    )

    assert TSV(analyzer_enabled) == TSV("1")

    # A new-version initiator sends the setting under its canonical name, which the old version
    # understands.
    query_id = str(uuid.uuid4())
    current.query(
        "SELECT name FROM clusterAllReplicas('test_cluster_mixed', system.tables) SETTINGS enable_analyzer = 1, serialize_query_plan=0;",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    analyzer_enabled = current.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}';"""
    )

    assert TSV(analyzer_enabled) == TSV("1")

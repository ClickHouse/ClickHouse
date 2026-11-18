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
    # and one old enough to still have the query analysis that was retired in 26.9 and removed in 26.10.

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    query_id = str(uuid.uuid4())
    current.query(
        "SELECT name FROM clusterAllReplicas('test_cluster_mixed', system.tables) settings serialize_query_plan=0;",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    assert current.query(
        """
SELECT hostname() AS h, getSetting('allow_experimental_analyzer')
FROM clusterAllReplicas('test_cluster_mixed', system.one)
ORDER BY h settings serialize_query_plan=0;"""
    ) == TSV([["backward", "true"], ["current", "true"]])

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

    # The price of ignoring it: the two analyses do not name the result columns the same way, and the
    # initiator matches the block a shard returns by name. The analyzer resolves a function to its
    # canonical name, so a shard that had to analyze the query the old way for the names to line up
    # answers with `hostName()` where this initiator asked for `hostname()`. A cluster that runs with
    # the old query analysis has to turn the analyzer on everywhere before a server is upgraded to
    # 26.10, which is what the deprecation in 26.9 asked for.
    assert "NOT_FOUND_COLUMN_IN_BLOCK" in backward.query_and_get_error(
        """
SELECT hostname() AS h, getSetting('allow_experimental_analyzer')
FROM clusterAllReplicas('test_cluster_mixed', system.one)
ORDER BY h;"""
    )

    # The other direction: the old initiator sends `allow_experimental_analyzer = 0` along with the
    # query, because that is how it analyzes the query itself. Since 26.10 this instance has no other
    # query analysis to fall back to, so it ignores the value instead of agreeing with the initiator,
    # and the settings it recorded for its part of the query say so. (Asking the shards with
    # `getSetting` would not: an initiator this old folds it to a constant before sending the query,
    # so every shard would echo the initiator's own value.)
    analyzer_enabled = current.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish';"""
    )

    assert TSV(analyzer_enabled) == TSV("1")

    # A new-version initiator sends the setting under its canonical name, which the old version
    # understands. `enable_analyzer` is only an alias here; the old version has never heard of it and
    # would reject the query outright if it were sent under that name. What the old instance recorded
    # for its part of the query is the only place that shows which name arrived, so this asks it and
    # not the initiator: the initiator's own log would say `1` even if it had sent nothing at all,
    # because that is the value it ran with.
    query_id = str(uuid.uuid4())
    current.query(
        "SELECT name FROM clusterAllReplicas('test_cluster_mixed', system.tables) SETTINGS enable_analyzer = 1, serialize_query_plan=0;",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    analyzer_enabled = backward.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}';"""
    )

    assert TSV(analyzer_enabled) == TSV("1")

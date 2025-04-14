import uuid
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

CLICKHOUSE_MAX_VERSION_WITH_ANALYZER_DISABLED_BY_DEFAULT = "24.2"

cluster = ClickHouseCluster(__file__)
# Here analyzer is enabled by default
current = cluster.add_instance(
    "current",
    main_configs=["configs/remote_servers.xml"],
)
# Here analyzer is disabled by default
backward = cluster.add_instance(
    "backward",
    use_old_analyzer=True,
    main_configs=["configs/remote_servers.xml"],
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
    # Two new versions (both know about the analyzer)
    # One have it enabled by default, another one - disabled.

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    query_id = str(uuid.uuid4())
    current.query(
        "SELECT * FROM clusterAllReplicas('test_cluster_mixed', system.tables);",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    assert (
        current.query(
            """
SELECT hostname() AS h, getSetting('allow_experimental_analyzer')
FROM clusterAllReplicas('test_cluster_mixed', system.one)
ORDER BY h;"""
        )
        == TSV([["backward", "true"], ["current", "true"]])
    )

    # Should be enabled explicitly on the old instance.
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
        "SELECT * FROM clusterAllReplicas('test_cluster_mixed', system.tables)",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    # The old version doesn't know about the alias.
    # For this we will ask about the old experimental name.
    assert (
        backward.query(
            """
SELECT hostname() AS h, getSetting('allow_experimental_analyzer')
FROM clusterAllReplicas('test_cluster_mixed', system.one)
ORDER BY h;"""
        )
        == TSV([["backward", "false"], ["current", "false"]])
    )

    # Should be disabled everywhere
    analyzer_enabled = backward.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM clusterAllReplicas('test_cluster_mixed', system.query_log)
WHERE initial_query_id = '{query_id}';"""
    )

    assert TSV(analyzer_enabled) == TSV("0")

    # Only new version knows about the alias
    # and it will send the old setting `allow_experimental_analyzer`
    # to the remote server.
    query_id = str(uuid.uuid4())
    current.query(
        "SELECT * FROM clusterAllReplicas('test_cluster_mixed', system.tables) SETTINGS enable_analyzer = 1;",
        query_id=query_id,
    )

    current.query("SYSTEM FLUSH LOGS")
    backward.query("SYSTEM FLUSH LOGS")

    # Should be disabled explicitly everywhere.
    analyzer_enabled = current.query(
        f"""
SELECT
DISTINCT Settings['allow_experimental_analyzer']
FROM system.query_log
WHERE initial_query_id = '{query_id}';"""
    )

    assert TSV(analyzer_enabled) == TSV("1")

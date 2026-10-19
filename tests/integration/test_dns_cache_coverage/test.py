"""Host names the server resolves must go through the ClickHouse DNS cache.

Each name used here is resolved by exactly one code path, so finding it in `system.dns_cache`
witnesses that the path goes through `DNSResolver` instead of resolving the name on its own
through `Poco::Net::SocketAddress`.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/dns_paths.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_zookeeper_hosts_are_cached(started_cluster):
    """`ZooKeeper::connect` resolves every configured node, not only the one it connects to."""
    node.query("SELECT count() FROM system.zookeeper WHERE path = '/'")

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.dns_cache WHERE hostname IN ('zoo1', 'zoo2', 'zoo3')",
        "3\n",
    )


def test_graphite_host_is_cached(started_cluster):
    """`GraphiteWriter` resolves the configured Graphite host on every transmission."""
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.dns_cache WHERE hostname = 'localhost'",
        "1\n",
    )

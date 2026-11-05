import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# A server can get a new address after a restart while its old address goes to another server of
# the same cluster. A pooled connection opened to the old address must not keep reaching the other
# server once the DNS cache no longer lists that address for the host.

cluster = ClickHouseCluster(__file__)
# Fixed addresses need a network with a configured subnet, which the remote database disk setup
# does not provide.
initiator = cluster.add_instance(
    "initiator",
    main_configs=["configs/remote_servers.xml", "configs/dns_update_long.xml"],
    with_remote_database_disk=False,
)
node_a = cluster.add_instance(
    "node_a", ipv4_address="10.5.96.11", with_remote_database_disk=False
)
node_b = cluster.add_instance(
    "node_b", ipv4_address="10.5.96.12", with_remote_database_disk=False
)

FAN_OUT = "SELECT arraySort(groupArray(h)) FROM clusterAllReplicas('two_shards', view(SELECT hostName() AS h))"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_pooled_connection_reopened_after_address_reuse(started_cluster):
    # The initiator resolves the nodes through its hosts file, so the test controls what the
    # resolver returns before and after the addresses change.
    initiator.set_hosts(
        [(node_a.ipv4_address, "node_a"), (node_b.ipv4_address, "node_b")]
    )
    assert initiator.query(FAN_OUT) == "['node_a','node_b']\n"

    # node_a moves to a free address, then node_b takes node_a's old address. The initiator still
    # has node_a's old address in its DNS cache, so its next connection to node_a reaches node_b.
    old_address_of_node_a = node_a.ipv4_address
    cluster.restart_instance_with_ip_change(node_a, "10.5.96.13")
    cluster.restart_instance_with_ip_change(node_b, old_address_of_node_a)
    initiator.set_hosts(
        [(node_a.ipv4_address, "node_a"), (node_b.ipv4_address, "node_b")]
    )

    assert_eq_with_retry(initiator, FAN_OUT, "['node_b','node_b']", ignore_error=True)

    # Once the DNS cache no longer lists the old address for node_a, the pooled connection is
    # reopened to node_a's new address.
    initiator.query("SYSTEM DROP DNS CACHE")
    assert initiator.query(FAN_OUT) == "['node_a','node_b']\n"

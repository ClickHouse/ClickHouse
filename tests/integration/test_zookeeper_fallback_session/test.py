import time

import pytest

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager

cluster = ClickHouseCluster(
    __file__, zookeeper_config_path="configs/zookeeper_load_balancing.xml"
)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml", "configs/zookeeper_load_balancing.xml"],
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml", "configs/zookeeper_load_balancing.xml"],
)
node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml", "configs/zookeeper_load_balancing.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in [node1, node2, node3]:
            node.query("DROP TABLE IF EXISTS simple SYNC")
            node.query(
                """
            CREATE TABLE simple (date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
            """.format(
                    replica=node.name
                )
            )
        yield cluster
    finally:
        cluster.shutdown()


def assert_uses_zk_node(node: ClickHouseInstance, zk_node):
    def check_callback(host):
        return host.strip() == zk_node

    # We don't convert the column 'host' of system.zookeeper_connection to ip address any more.
    host = node.query_with_retry(
        "select host from system.zookeeper_connection", check_callback=check_callback
    )
    assert host.strip() == zk_node


def get_zk_applied_zxid(cluster: ClickHouseCluster, zk_node):
    """Last zxid applied by `zk_node` itself, or None if it cannot be read.

    `srvr` reports `KeeperStorage::getZXID()`, the very same value
    `KeeperTCPHandler` compares a reconnecting client's `last_zxid_seen`
    against, so this is exactly the quantity that decides a session refusal.
    """
    # This fixture's Keepers listen on cluster.zookeeper_port, not on the
    # 9181 default of keeper_utils.
    client = keeper_utils.get_keeper_socket(
        cluster, zk_node, cluster.zookeeper_port, timeout_sec=10
    )
    try:
        client.send(b"srvr")
        data = client.recv(100_000).decode()
    finally:
        client.close()

    for line in data.splitlines():
        if line.startswith("Zxid:"):
            return int(line.split()[1], 16)
    # A Keeper that is not currently serving requests answers with a plain
    # message and no Zxid line.
    return None


def get_max_client_zxid(nodes):
    """Highest zxid any of `nodes` has already seen, or None if one is reconnecting.

    `system.zookeeper_connection` has no row for a session that currently has no
    connected host, so a read taken while a reconnect is in flight is not a
    watermark and must not be mistaken for a low one.
    """
    seen = []
    for node in nodes:
        raw = node.query(
            "select last_zxid_seen from system.zookeeper_connection"
        ).strip()
        if not raw:
            return None
        seen.append(int(raw))
    return max(seen)


def wait_zk_node_caught_up(cluster: ClickHouseCluster, zk_node, nodes, timeout=60):
    """Wait until `zk_node` has applied everything `nodes` have already seen.

    A Keeper refuses a reconnecting client whose `last_zxid_seen` exceeds its
    own applied zxid, telling it to try another server, and the client falls
    through to the next host in the list. With `in_order` balancing no
    reconnect task is armed to undo that, so a node demoted this way stays
    demoted for the rest of the test.
    """
    deadline = time.monotonic() + timeout
    applied = client_zxid = None
    consecutive_ok = 0
    while time.monotonic() < deadline:
        applied = get_zk_applied_zxid(cluster, zk_node)
        # Read the clients after zk_node: their watermarks advance
        # asynchronously (every heartbeat response carries a zxid), so one
        # sampled earlier could already be stale and zk_node would still
        # refuse the new session.
        client_zxid = get_max_client_zxid(nodes)
        if applied is not None and client_zxid is not None and applied >= client_zxid:
            # Require two consecutive samples: a single one can be a stale
            # snapshot taken just before a watermark advanced again.
            consecutive_ok += 1
            if consecutive_ok >= 2:
                return
        else:
            consecutive_ok = 0
        time.sleep(0.2)

    raise AssertionError(
        f"{zk_node} did not catch up within {timeout}s: it has applied "
        f"{'unreadable (not serving requests?)' if applied is None else applied}"
        f", while the clients have already seen "
        f"{'unknown (reconnecting)' if client_zxid is None else client_zxid}. "
        f"{zk_node} would refuse their sessions and they would fall back to "
        f"another Keeper."
    )


def test_fallback_session(started_cluster: ClickHouseCluster):
    # only leave connecting to zoo3 possible
    with PartitionManager() as pm:
        for node in started_cluster.instances.values():
            for zk in ["zoo1", "zoo2"]:
                pm.add_rule(
                    {
                        "instance": node,
                        "source": node.ip_address,
                        "destination": cluster.get_instance_ip(zk),
                        "action": "REJECT --reject-with tcp-reset",
                        "protocol": "tcp",
                    }
                )

        for node in [node1, node2, node3]:
            # all nodes will have to switch to zoo3
            assert_uses_zk_node(node, "zoo3")

        node1.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(1))

        # and replication still works
        for node in [node2, node3]:
            assert (
                node.query_with_retry(
                    "SELECT count() from simple",
                    check_callback=lambda count: count.strip() == "1",
                )
                == "1\n"
            )

    # at this point network partitioning has been reverted.
    # the nodes should switch to zoo1 because of `in_order` load-balancing.
    # otherwise they would connect to a random replica

    # but there's no reason to reconnect because current session works
    # and there's no "optimal" node with `in_order` load-balancing
    # so we need to break the current session

    for node in [node1, node2, node3]:
        assert_uses_zk_node(node, "zoo3")

    # zoo1 is a Raft follower applying the tail of the log asynchronously, so
    # it can be behind what the nodes saw through zoo3 and would refuse their
    # new sessions. Waiting closes that catch-up window; the cluster keeps
    # committing, so this narrows the race rather than removing it.
    wait_zk_node_caught_up(started_cluster, "zoo1", [node1, node2, node3])

    with PartitionManager() as pm:
        for node in started_cluster.instances.values():
            pm.add_rule(
                {
                    "instance": node,
                    "source": node.ip_address,
                    "destination": cluster.get_instance_ip("zoo3"),
                    "action": "REJECT --reject-with tcp-reset",
                    "protocol": "tcp",
                }
            )

        for node in [node1, node2, node3]:
            assert_uses_zk_node(node, "zoo1")

    node1.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(2))
    for node in [node2, node3]:
        assert (
            node.query_with_retry(
                "SELECT count() from simple",
                check_callback=lambda count: count.strip() == "2",
            )
            == "2\n"
        )

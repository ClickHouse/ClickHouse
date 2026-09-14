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


def assert_uses_new_zk_session(node: ClickHouseInstance, zk_node, previous):
    """Wait until `node` holds a session other than `previous`, and on `zk_node`.

    The host on its own would also be satisfied by `previous`, which was established
    while zoo1 was the only Keeper reachable.
    """

    def check_callback(session):
        session = session.strip()
        return session != previous and session.split("\t")[0] == zk_node

    session = node.query_with_retry(
        "select host, client_id from system.zookeeper_connection",
        check_callback=check_callback,
    ).strip()

    assert session != previous, f"{node.name} kept the session it had before the heal"
    assert session.split("\t")[0] == zk_node, f"{node.name} is connected as {session!r}"


def get_zk_applied_zxid(cluster: ClickHouseCluster, zk_node):
    """Last zxid applied by `zk_node` itself, or None if it cannot be read.

    `srvr` reports `KeeperStorage::getZXID()`, the very same value
    `KeeperTCPHandler` compares a reconnecting client's `last_zxid_seen`
    against, so this is exactly the quantity that decides a session refusal.
    """
    # This fixture's Keepers listen on cluster.zookeeper_port, not on the
    # 9181 default of keeper_utils.
    data = keeper_utils.send_4lw_cmd(
        cluster, zk_node, "srvr", cluster.zookeeper_port, timeout_sec=10
    )

    for line in data.splitlines():
        if line.startswith("Zxid:"):
            return int(line.split()[1], 16)
    # A Keeper that is not currently serving requests answers with a plain
    # message and no Zxid line.
    return None


def wait_zk_node_caught_up(cluster: ClickHouseCluster, zk_node, target_zxid, timeout=60):
    """Wait until `zk_node` has applied everything up to `target_zxid`.

    A Keeper refuses a reconnecting client whose `last_zxid_seen` exceeds its
    own applied zxid, telling it to try another server, and the client falls
    through to the next host in the list. With `in_order` balancing no
    reconnect task is armed to undo that, so a node demoted this way stays
    demoted for the rest of the test.
    """
    deadline = time.monotonic() + timeout
    applied = None
    while time.monotonic() < deadline:
        applied = get_zk_applied_zxid(cluster, zk_node)
        if applied is not None and applied >= target_zxid:
            return
        time.sleep(0.2)

    raise AssertionError(
        f"{zk_node} did not reach zxid {target_zxid} within {timeout}s: it has "
        f"applied {'unreadable (not serving requests?)' if applied is None else applied}. "
        f"{zk_node} could refuse the nodes' sessions and they would fall back to "
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

    with PartitionManager() as pm:
        # Cut the nodes off every Keeper, not only the one they use, so that none of
        # them can start a session anywhere while zoo1 catches up.
        for node in [node1, node2, node3]:
            pm.drop_instance_zk_connections(
                node, action="REJECT --reject-with tcp-reset"
            )

        # The rule stops only what a node sends, so a reply already on its way could
        # still raise its `last_zxid_seen`. Finalizing joins the receiving thread,
        # after which the value cannot move.
        for node in [node1, node2, node3]:
            node.query("SYSTEM RECONNECT ZOOKEEPER")

        # zoo3 accepted these sessions and has answered every request since, so its
        # applied zxid bounds what any of the nodes has seen. zoo1 applies the tail of
        # the log asynchronously and refuses a client that has seen more.
        target_zxid = get_zk_applied_zxid(started_cluster, "zoo3")
        assert target_zxid is not None, "zoo3 is not serving requests"
        wait_zk_node_caught_up(started_cluster, "zoo1", target_zxid)

        # A session is established by walking the host list in order and stopping at
        # the first Keeper that answers, so with several reachable the outcome depends
        # on where a walk already is. Shutting zoo2 and zoo3 leaves only zoo1.
        for node in [node1, node2, node3]:
            for zk in ["zoo2", "zoo3"]:
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
            pm.restore_instance_zk_connections(
                node, action="REJECT --reject-with tcp-reset"
            )

        sessions = {}
        for node in [node1, node2, node3]:
            # Synchronisation, not the assertion: zoo1 is the only Keeper reachable.
            assert_uses_zk_node(node, "zoo1")
            sessions[node.name] = node.query_with_retry(
                "select host, client_id from system.zookeeper_connection",
                check_callback=lambda session: session.strip() != "",
            ).strip()

    # `PartitionManager` logs a failed `iptables -D` and carries on, so a rule left
    # behind would keep zoo1 the only reachable Keeper and satisfy the assertion below.
    for node in [node1, node2, node3]:
        rules = node.exec_in_container(
            ["iptables", "--wait", "-S", "OUTPUT"], user="root"
        )
        for zk in ["zoo2", "zoo3"]:
            assert cluster.get_instance_ip(zk) not in rules, rules

    # A node's `last_zxid_seen` was at or below what zoo1 had applied when it connected
    # to it, and everything it has seen since came from zoo1, so zoo1 cannot refuse the
    # new sessions. `in_order` alone decides the host they land on.
    for node in [node1, node2, node3]:
        node.query("SYSTEM RECONNECT ZOOKEEPER")

    for node in [node1, node2, node3]:
        assert_uses_new_zk_session(node, "zoo1", sessions[node.name])

    node1.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(2))
    for node in [node2, node3]:
        assert (
            node.query_with_retry(
                "SELECT count() from simple",
                check_callback=lambda count: count.strip() == "2",
            )
            == "2\n"
        )

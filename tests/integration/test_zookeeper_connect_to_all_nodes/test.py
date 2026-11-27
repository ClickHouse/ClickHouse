import time
import uuid

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

# keeper1, keeper2, keeper3 form cluster A; node_b is a single node cluster B and a client of both.
keeper1 = cluster.add_instance("keeper1", main_configs=["configs/keeper1.xml"])
keeper2 = cluster.add_instance("keeper2", main_configs=["configs/keeper2.xml"])
keeper3 = cluster.add_instance("keeper3", main_configs=["configs/keeper3.xml"])
node_b = cluster.add_instance("node_b", main_configs=["configs/node_b.xml"])
node = cluster.add_instance(
    "node", main_configs=["configs/zookeeper.xml", "configs/placement.xml"]
)

CLUSTER_A = ["keeper1", "keeper2", "keeper3"]
CHECK_QUERY = "SELECT count() FROM system.zookeeper WHERE path = '/'"
# A refusal is a connection loss: one try, so that the query reports it instead of retrying.
CHECK_QUERY_ONCE = CHECK_QUERY + " SETTINGS insert_keeper_max_retries = 0"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [keeper1, keeper2, keeper3, node_b])
        yield cluster
    finally:
        cluster.shutdown()


def set_zookeeper_config(
    hosts, connect_to_all_nodes=True, load_balancing="in_order", **settings
):
    """Every config but one uses `in_order` with keeper1 first, so only a metric of the policy can choose another node.
    `connect_to_all_nodes=False` omits the key, so that only a non-default policy can turn the mode on.
    The default wait is long: with every node up, a round ends as soon as all of them answered."""
    nodes = "".join(
        f"<node><host>{host}</host><port>9181</port></node>" for host in hosts
    )
    settings = {"connect_to_all_nodes_wait_ms": 10000, **settings}
    if connect_to_all_nodes:
        settings = {"connect_to_all_nodes": 1, **settings}
    extra = "".join(f"<{key}>{value}</{key}>" for key, value in settings.items())
    node.replace_config(
        "/etc/clickhouse-server/config.d/zookeeper.xml",
        f"<clickhouse><zookeeper>{nodes}"
        f"<zookeeper_load_balancing>{load_balancing}</zookeeper_load_balancing>"
        f"{extra}"
        "</zookeeper></clickhouse>",
    )
    node.query("SYSTEM RELOAD CONFIG")
    # The next query starts a new session and chooses a node again, even if the config did not change.
    node.query("SYSTEM RECONNECT ZOOKEEPER")


def connected_host():
    node.query(CHECK_QUERY)
    return node.query(
        "SELECT host FROM system.zookeeper_connection WHERE name = 'default'"
    ).strip()


def assert_error_with_retry(instance, query, message, retries=60):
    error = ""
    for _ in range(retries):
        error = instance.query_and_get_error(query)
        if message in error:
            return
        time.sleep(0.5)
    assert message in error


def create_znodes(keeper_name, count):
    client = keeper_utils.get_fake_zk(cluster, keeper_name)
    try:
        prefix = f"/znode_{uuid.uuid4().hex}"
        for result in [client.create_async(f"{prefix}_{i}", b"") for i in range(count)]:
            result.get()
    finally:
        client.stop()
        client.close()


def cluster_a_zxid():
    zxids = []
    for keeper in [keeper1, keeper2, keeper3]:
        for line in keeper_utils.send_4lw_cmd(cluster, keeper, "srvr").splitlines():
            if line.startswith("Zxid:"):
                zxids.append(int(line.split(":", 1)[1].strip(), 16))
    return max(zxids)


def test_different_clusters():
    # node_b may have connected to cluster A at startup, before its own Keeper listened. Its renewal carries cluster A's zxid,
    # which node_b's Keeper refuses unless its own zxid is ahead.
    create_znodes("node_b", cluster_a_zxid() + 1000)
    node_b.query("SYSTEM RECONNECT ZOOKEEPER")
    assert_error_with_retry(
        node_b, CHECK_QUERY_ONCE, "report different cluster configurations"
    )
    assert "report different cluster configurations" in node_b.query_and_get_error(
        "SELECT count() FROM system.zookeeper_connection"
    )

    set_zookeeper_config(CLUSTER_A)
    assert connected_host() == "keeper1"


def test_renewal_keeps_cluster():
    with PartitionManager() as pm:
        pm.partition_instances(node, node_b)
        set_zookeeper_config(CLUSTER_A + ["node_b"], connect_to_all_nodes_wait_ms=1000)
        assert connected_host() in CLUSTER_A

        # A Keeper refuses a session that has seen a newer zxid than its own, so the zxid of node_b has to be ahead.
        last_zxid_seen = int(
            node.query(
                "SELECT last_zxid_seen FROM system.zookeeper_connection WHERE name = 'default'"
            )
        )
        create_znodes("node_b", last_zxid_seen + 1000)

        pm.heal_all()
        for keeper in [keeper1, keeper2, keeper3]:
            pm.partition_instances(node, keeper)
        node.query("SYSTEM RECONNECT ZOOKEEPER")
        assert_error_with_retry(
            node, CHECK_QUERY_ONCE, "shares no server with the cluster of the previous session"
        )

        pm.heal_all()
        pm.partition_instances(node, node_b)
        node.query("SYSTEM RECONNECT ZOOKEEPER")
        assert connected_host() in CLUSTER_A


def test_move_to_optimal_node_checks_all_nodes():
    with PartitionManager() as pm:
        pm.partition_instances(node, keeper1)
        pm.partition_instances(node, node_b)
        # keeper1 is the optimal node of `first_or_random`, so a session elsewhere moves to it within seconds.
        set_zookeeper_config(
            CLUSTER_A + ["node_b"],
            load_balancing="first_or_random",
            connect_to_all_nodes_wait_ms=1000,
            fallback_session_lifetime="<min>1</min><max>2</max>",
        )
        assert connected_host() in ["keeper2", "keeper3"]

        last_zxid_seen = int(
            node.query(
                "SELECT last_zxid_seen FROM system.zookeeper_connection WHERE name = 'default'"
            )
        )
        create_znodes("node_b", last_zxid_seen + 1000)

        # The move starts a new session, which connects to all nodes and sees cluster B too.
        pm.heal_all()
        error = ""
        for _ in range(120):
            error = node.query_and_get_answer_with_error(CHECK_QUERY_ONCE)[1]
            if "report different cluster configurations" in error:
                break
            time.sleep(0.5)
        assert "report different cluster configurations" in error


def test_fewest_connections():
    clients = []
    try:
        for keeper in ["keeper1"] * 5 + ["keeper2"] * 5:
            clients.append(keeper_utils.get_fake_zk(cluster, keeper))

        set_zookeeper_config(CLUSTER_A, connect_to_all_nodes_policy="fewest_connections")
        assert connected_host() == "keeper3"

        # The availability zone is compared first: `node` is in the zone of keeper2.
        set_zookeeper_config(
            CLUSTER_A,
            connect_to_all_nodes_policy="fewest_connections",
            prefer_local_availability_zone=1,
            availability_zone_autodetect=1,
        )
        assert connected_host() == "keeper2"
    finally:
        for client in clients:
            client.stop()
            client.close()


def test_wait_and_fastest_connect():
    with PartitionManager() as pm:
        # Every packet from keeper1 is late, so connecting to it takes seconds.
        pm.add_network_delay(keeper1, 1000)

        set_zookeeper_config(
            CLUSTER_A, connection_timeout_ms=5000, connect_to_all_nodes_wait_ms=200
        )
        assert connected_host() != "keeper1"

        set_zookeeper_config(
            CLUSTER_A, connection_timeout_ms=5000, connect_to_all_nodes_wait_ms=30000
        )
        assert connected_host() == "keeper1"

        set_zookeeper_config(
            CLUSTER_A,
            connection_timeout_ms=5000,
            connect_to_all_nodes_wait_ms=30000,
            connect_to_all_nodes_policy="fastest_connect",
        )
        assert connected_host() in ["keeper2", "keeper3"]


def test_lowest_latency():
    for keeper in [keeper1, keeper2, keeper3]:
        keeper_utils.send_4lw_cmd(cluster, keeper, "srst")

    client = keeper_utils.get_fake_zk(cluster, "keeper1")
    try:
        prefix = f"/test_lowest_latency_{uuid.uuid4().hex}"
        with PartitionManager() as pm:
            # Each write waits for a round trip of keeper1 to the leader.
            pm.add_network_delay(keeper1, 300)
            for i in range(5):
                client.create(f"{prefix}_{i}", b"")
    finally:
        client.stop()
        client.close()

    set_zookeeper_config(
        CLUSTER_A, connect_to_all_nodes=False, connect_to_all_nodes_policy="lowest_latency"
    )
    assert connected_host() != "keeper1"

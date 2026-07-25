import time

import pytest

from helpers.cluster import ClickHouseCluster

# Regression test for stale ON CLUSTER registration when skip_distributed_ddl is enabled on an
# already-registered replica.
#
# node1 resolves "localhost" to itself, so it initially claims localhost as an active replica under
# /clickhouse/task_queue/replicas (the same situation as two cluster replicas sharing one host).
# markReplicasActive() walks the existing znodes under that path and reclaims any self-resolving
# host, so simply dropping localhost from the config-derived host set is not enough: after
# skip_distributed_ddl is enabled and node1 restarts, node1 must no longer reclaim the
# already-registered localhost, while keeping its own (still allowed) node1 registration.

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

REMOTE_SERVERS_PATH = "/etc/clickhouse-server/config.d/remote_servers.xml"
REPLICAS_DIR = "/clickhouse/task_queue/replicas"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def is_registered_active(zk, host_substr):
    """True if some /replicas/<host_id> whose id contains host_substr has an 'active' child."""
    if zk.exists(REPLICAS_DIR) is None:
        return False
    for host_id in zk.get_children(REPLICAS_DIR):
        if host_substr in host_id and zk.exists(f"{REPLICAS_DIR}/{host_id}/active"):
            return True
    return False


def wait_for(predicate, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(1)
    return predicate()


def test_skip_distributed_ddl_drops_stale_registration(started_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    try:
        # Baseline: with skip_distributed_ddl=0 node1 claims both node1 and localhost as active.
        assert wait_for(
            lambda: is_registered_active(zk, "localhost")
        ), "localhost should be registered active before skip_distributed_ddl is enabled"
        assert is_registered_active(zk, "node1"), "node1 should be registered active"

        # Enable skip_distributed_ddl for the localhost replica and restart the node. On restart the
        # DDLWorker re-initializes with the new config; it must not reclaim the localhost znode that
        # still exists from before, but must keep claiming its own (still allowed) node1 host.
        node1.replace_in_config(
            REMOTE_SERVERS_PATH,
            "<skip_distributed_ddl>0</skip_distributed_ddl>",
            "<skip_distributed_ddl>1</skip_distributed_ddl>",
        )
        node1.restart_clickhouse()

        assert wait_for(
            lambda: is_registered_active(zk, "node1")
        ), "node1 should be registered active after restart"
        assert not is_registered_active(
            zk, "localhost"
        ), "localhost must not be reclaimed as active after enabling skip_distributed_ddl"
    finally:
        zk.stop()
        zk.close()

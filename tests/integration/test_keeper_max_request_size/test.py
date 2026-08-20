import pytest

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml", "configs/overrides.xml"],
    with_zookeeper=False,
    use_keeper=False,
)
node_client_only = cluster.add_instance(
    "node_client_only",
    main_configs=["configs/keeper_config.xml", "configs/server_unlimited.xml", "configs/client_overrides.xml"],
    with_zookeeper=False,
    use_keeper=False,
)
node_both = cluster.add_instance(
    "node_both",
    main_configs=["configs/keeper_config.xml", "configs/client_overrides.xml"],
    with_zookeeper=False,
    use_keeper=False,
)
# no <zookeeper> section: the client config falls back to the keeper_server section
node_colocated = cluster.add_instance(
    "node_colocated",
    main_configs=["configs/keeper_config.xml", "configs/allow_write.xml"],
    with_zookeeper=False,
    use_keeper=False,
)
# server limit 0 and no client limit: nothing is enforced
node_unlimited = cluster.add_instance(
    "node_unlimited",
    main_configs=["configs/keeper_config.xml", "configs/server_unlimited.xml", "configs/overrides.xml"],
    with_zookeeper=False,
    use_keeper=False,
)
# client limit over the compressed client protocol
node_compressed = cluster.add_instance(
    "node_compressed",
    main_configs=["configs/keeper_config.xml", "configs/client_compressed.xml"],
    with_zookeeper=False,
    use_keeper=False,
)
# real Apache ZooKeeper (use_keeper=False), where /keeper is ordinary user-writable data
node_zk = cluster.add_instance(
    "node_zk",
    main_configs=["configs/allow_write.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _insert(target, path, count, size):
    target.query(
        "INSERT INTO system.zookeeper (name, path, value) "
        f"SELECT number::String, '{path}', repeat('a', {size}) FROM numbers({count})"
    )


# server limit only: the advertised limit is learned at connect, so the rejection
# must be client-side and deterministic (`exceeds limit`)
def test_server_limit_large_fails(started_cluster):
    keeper_utils.wait_until_connected(cluster, node)
    with pytest.raises(Exception, match="exceeds limit"):
        _insert(node, "/srv_large", 10000, 3000)


# server limit only: small request succeeds
def test_server_limit_small_ok(started_cluster):
    keeper_utils.wait_until_connected(cluster, node)
    _insert(node, "/srv_small", 100, 3000)


# client limit only: large request is rejected before it is sent
def test_client_limit_large_fails(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_client_only)
    with pytest.raises(Exception, match="exceeds limit"):
        _insert(node_client_only, "/cli_large", 100, 3000)


# client limit only: small request succeeds
def test_client_limit_small_ok(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_client_only)
    _insert(node_client_only, "/cli_small", 10, 100)


# both limits, client stricter: large request rejected by the client limit
def test_both_limits_large_fails(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_both)
    with pytest.raises(Exception, match="exceeds limit"):
        _insert(node_both, "/both_large", 100, 3000)


# both limits, client stricter: small request succeeds
def test_both_limits_small_ok(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_both)
    _insert(node_both, "/both_small", 10, 100)


# the advertised limit znode is served virtually and matches the configured value
def test_advertised_limit_znode(started_cluster):
    keeper_utils.wait_until_connected(cluster, node)
    zk = keeper_utils.get_fake_zk(cluster, "node")
    try:
        data, _ = zk.get("/keeper/max_request_size")
        assert data == b"1048576"
    finally:
        zk.stop()
        zk.close()


# co-located client without a <zookeeper> section mirrors the limit from coordination_settings
def test_colocated_mirror_limit(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_colocated)
    with pytest.raises(Exception, match="exceeds limit"):
        _insert(node_colocated, "/colo_large", 10000, 3000)


# a rejected request must not disrupt the session: the failure is observed,
# the session id stays the same, and the next request succeeds
def test_rejection_keeps_session(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_client_only)
    _insert(node_client_only, "/sess_ok_before", 10, 100)
    session_before = node_client_only.query(
        "SELECT client_id FROM system.zookeeper_connection"
    ).strip()
    with pytest.raises(Exception, match="exceeds limit"):
        _insert(node_client_only, "/sess_large", 100, 3000)
    _insert(node_client_only, "/sess_ok_after", 10, 100)
    session_after = node_client_only.query(
        "SELECT client_id FROM system.zookeeper_connection"
    ).strip()
    assert session_before == session_after


# server limit 0 with no client limit means unlimited: large writes pass
def test_unlimited_allows_large(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_unlimited)
    _insert(node_unlimited, "/unlim_large", 10000, 3000)


# smoke: the whole flow works over the compressed client protocol
def test_compressed_client(started_cluster):
    keeper_utils.wait_until_connected(cluster, node_compressed)
    with pytest.raises(Exception, match="exceeds limit"):
        _insert(node_compressed, "/compr_large", 100, 3000)
    _insert(node_compressed, "/compr_small", 10, 100)


# on plain Apache ZooKeeper `/keeper/max_request_size` is ordinary user data and the server
# does not advertise the MAX_REQUEST_SIZE feature flag, so a value written there by a user
# must not become a client-side limit
def test_plain_zookeeper_ignores_user_znode(started_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    try:
        zk.ensure_path("/keeper")
        if zk.exists("/keeper/max_request_size"):
            zk.set("/keeper/max_request_size", b"2048")
        else:
            zk.create("/keeper/max_request_size", b"2048")
        zk.ensure_path("/zk_large")
    finally:
        zk.stop()
        zk.close()
    # reconnect so the client re-runs the feature-flag discovery against the poisoned znode
    node_zk.restart_clickhouse()
    _insert(node_zk, "/zk_large", 100, 3000)

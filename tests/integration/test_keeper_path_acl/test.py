import pytest
from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import get_fake_zk
from kazoo.security import Id, ACL


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml", "configs/zookeeper_config.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_path_acl(started_cluster):
    """Test path_acl configuration parsing and ACL application"""
    node.query(
        "CREATE TABLE IF NOT EXISTS test_table (id UInt64) ENGINE = ReplicatedMergeTree('/test_path', 'replica1') ORDER BY id"
    )
    node.query("INSERT INTO test_table VALUES (1)")

    zk = get_fake_zk(started_cluster, "node")
    zk.add_auth("digest", "testuser:testpass")

    try:
        # Check /test_path created by ReplicatedMergeTree
        # This path should have path_acls
        acls_test_path, stat = zk.get_acls("/test_path")

        expected_acls = [
            ACL(
                perms=31,
                id=Id(scheme="digest", id="testuser:HqwT8VeO9JO57VYXpfSjGycetmc="),
            ),
            ACL(perms=3, id=Id(scheme="digest", id="user1:password1")),
        ]

        assert sorted(
            acls_test_path, key=lambda x: (x.perms, x.id.scheme, x.id.id)
        ) == sorted(expected_acls, key=lambda x: (x.perms, x.id.scheme, x.id.id))

        # Find a child of /test_path and verify its ACLs
        children = zk.get_children("/test_path")
        assert len(children) > 0
        child_path = f"/test_path/{children[0]}"
        acls_child, stat = zk.get_acls(child_path)
        expected_child_acls = [
            ACL(
                perms=31,
                id=Id(scheme="digest", id="testuser:HqwT8VeO9JO57VYXpfSjGycetmc="),
            ),
        ]
        assert acls_child == expected_child_acls

    finally:
        zk.stop()
        zk.close()

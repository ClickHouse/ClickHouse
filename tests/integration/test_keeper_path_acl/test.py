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
    node.query(
        "CREATE TABLE IF NOT EXISTS test_table_another (id UInt64) ENGINE = ReplicatedMergeTree('/test_path_another', 'replica1') ORDER BY id"
    )

    zk = get_fake_zk(started_cluster, "node")
    zk.add_auth("digest", "testuser:testpass")

    try:

        def compare_acls(first, second):
            key_acl = lambda x: (x.perms, x.id.scheme, x.id.id)
            assert sorted(first, key=key_acl) == sorted(second, key=key_acl)

        def check_path_acls(path, apply_acls_to_children):
            acls_test_path, stat = zk.get_acls(path)

            expected_acls = [
                ACL(
                    perms=31,
                    id=Id(scheme="digest", id="testuser:HqwT8VeO9JO57VYXpfSjGycetmc="),
                ),
                ACL(perms=3, id=Id(scheme="world", id="anyone")),
            ]

            compare_acls(acls_test_path, expected_acls)

            # Find a child of path and verify its ACLs
            children = zk.get_children(path)
            assert len(children) > 0
            child_path = f"/{path}/{children[0]}"
            acls_child, stat = zk.get_acls(child_path)
            expected_child_acls = (
                expected_acls
                if apply_acls_to_children
                else [
                    ACL(
                        perms=31,
                        id=Id(
                            scheme="digest", id="testuser:HqwT8VeO9JO57VYXpfSjGycetmc="
                        ),
                    ),
                ]
            )

            compare_acls(acls_child, expected_child_acls)

        check_path_acls("/test_path", apply_acls_to_children=False)
        check_path_acls("/test_path_another", apply_acls_to_children=True)

    finally:
        zk.stop()
        zk.close()

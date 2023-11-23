import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


# TODO myrrc this is a bit ugly as we need to test 3 scenarios, one needing configs at start time (vfs)
# thus we can't use pytest.mark.parametrize. The tradeoff is cluster setup/teardown for every scenario
@pytest.fixture(
    scope="module",
    params=[
        # vfs_configs, s3_policy, 0copy
        ([], "s3_encrypted_policy_with_diff_keys", False),
        ([], "s3_encrypted_policy", True),
        (["configs/vfs.xml"], "s3_encrypted_policy", False),
    ],
    ids=["ordinary", "0copy", "vfs"],
)
def start_cluster(request):
    vfs_config, s3_policy, use_zero_copy = request.param
    cluster = ClickHouseCluster(__file__)
    try:
        base_configs = [
            "configs/cluster.xml",
            "configs/disk_s3_encrypted.xml",
        ] + vfs_config

        node1 = cluster.add_instance(
            "node1",
            main_configs=base_configs
            + [
                "configs/disk_s3_encrypted_node1.xml",
            ],
            macros={"replica": "node1"},
            with_zookeeper=True,
            with_minio=True,
        )

        node2 = cluster.add_instance(
            "node2",
            main_configs=base_configs
            + [
                "configs/disk_s3_encrypted_node2.xml",
            ],
            macros={"replica": "node2"},
            with_zookeeper=True,
            with_minio=True,
        )

        cluster.start()
        yield node1, node2, s3_policy, use_zero_copy
    finally:
        cluster.shutdown()


def test_encrypted_disk_replication(start_cluster):
    node1, node2, storage_policy, use_zero_copy = start_cluster

    engine = "ReplicatedMergeTree('/clickhouse/tables/encrypted_test/', '{replica}')"
    settings = f"storage_policy='{storage_policy}', allow_remote_fs_zero_copy_replication={int(use_zero_copy)}"

    node1.query(
        f"""
        CREATE TABLE encrypted_test ON CLUSTER 'cluster' (
            id Int64,
            data String
        ) ENGINE={engine}
        ORDER BY id
        SETTINGS {settings}
    """
    )

    node1.query("INSERT INTO encrypted_test VALUES (0, 'a'), (1, 'b')")
    node2.query("INSERT INTO encrypted_test VALUES (2, 'c'), (3, 'd')")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' encrypted_test")

    select_query = "SELECT * FROM encrypted_test ORDER BY id"
    expected = TSV([[0, "a"], [1, "b"], [2, "c"], [3, "d"]])

    assert node1.query(select_query) == expected
    assert node2.query(select_query) == expected

    node1.query("DROP TABLE IF EXISTS encrypted_test ON CLUSTER 'cluster' SYNC")

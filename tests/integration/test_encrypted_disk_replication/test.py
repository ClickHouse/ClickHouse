import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/cluster.xml",
        "configs/disk_s3_encrypted.xml",
        "configs/disk_s3_encrypted_node1.xml",
    ],
    macros={"replica": "node1"},
    with_zookeeper=True,
    with_minio=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/cluster.xml",
        "configs/disk_s3_encrypted.xml",
        "configs/disk_s3_encrypted_node2.xml",
    ],
    macros={"replica": "node2"},
    with_zookeeper=True,
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node1.query("DROP TABLE IF EXISTS encrypted_test ON CLUSTER 'cluster' SYNC")


def create_table(
    zero_copy_replication=False, storage_policy="s3_encrypted_policy_with_diff_keys"
):
    engine = "ReplicatedMergeTree('/clickhouse/tables/encrypted_test/', '{replica}')"

    settings = f"storage_policy='{storage_policy}', allow_remote_fs_zero_copy_replication={int(zero_copy_replication)}"

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


def check_replication():
    node1.query("INSERT INTO encrypted_test VALUES (0, 'a'), (1, 'b')")
    node2.query("INSERT INTO encrypted_test VALUES (2, 'c'), (3, 'd')")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' encrypted_test")

    select_query = "SELECT * FROM encrypted_test ORDER BY id"

    assert node1.query(select_query) == TSV([[0, "a"], [1, "b"], [2, "c"], [3, "d"]])
    assert node2.query(select_query) == TSV([[0, "a"], [1, "b"], [2, "c"], [3, "d"]])


def test_replication():
    create_table(
        zero_copy_replication=False, storage_policy="s3_encrypted_policy_with_diff_keys"
    )
    check_replication()


def test_zero_copy_replication():
    create_table(zero_copy_replication=True, storage_policy="s3_encrypted_policy")
    check_replication()

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException


FIRST_PART_NAME = "all_1_1_0"

@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance("node",
                                    main_configs=["configs/storage.xml"],
                                    tmpfs=["/disk:size=100M"],
                                    with_minio=True)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.mark.parametrize("policy", ["encrypted_policy", "local_policy", "s3_policy"])
def test_encrypted_disk(cluster, policy):
    node = cluster.instances["node"]
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(policy)
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    node.query("OPTIMIZE TABLE encrypted_test FINAL")
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    node.query("DROP TABLE IF EXISTS encrypted_test NO DELAY")


@pytest.mark.parametrize("policy,disk,encrypted_disk", [("local_policy", "disk_local", "disk_local_encrypted"), ("s3_policy", "disk_s3", "disk_s3_encrypted")])
def test_part_move(cluster, policy, disk, encrypted_disk):
    node = cluster.instances["node"]
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(policy)
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(FIRST_PART_NAME, encrypted_disk))
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    with pytest.raises(QueryRuntimeException) as exc:
        node.query("ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(FIRST_PART_NAME, encrypted_disk))

    assert("Part '{}' is already on disk '{}'".format(FIRST_PART_NAME, encrypted_disk) in str(exc.value))

    node.query("ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(FIRST_PART_NAME, disk))
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("DROP TABLE IF EXISTS encrypted_test NO DELAY")


@pytest.mark.parametrize("policy,encrypted_disk", [("local_policy", "disk_local_encrypted"), ("s3_policy", "disk_s3_encrypted")])
def test_optimize_table(cluster, policy, encrypted_disk):
    node = cluster.instances["node"]
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(policy)
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(FIRST_PART_NAME, encrypted_disk))
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    node.query("OPTIMIZE TABLE encrypted_test FINAL")

    with pytest.raises(QueryRuntimeException) as exc:
        node.query("ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(FIRST_PART_NAME, encrypted_disk))

    assert("Part {} is not exists or not active".format(FIRST_PART_NAME) in str(exc.value))

    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    node.query("DROP TABLE IF EXISTS encrypted_test NO DELAY")

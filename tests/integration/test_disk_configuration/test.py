import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.blobs import wait_blobs_count_synchronization

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/storage_configuration.xml",
                "configs/config.d/include_from_path.xml",
                "configs/config.d/include_from.xml",
                "configs/config.d/remote_servers.xml",
            ],
            env_variables={
                "MINIO_SECRET": minio_secret_key,
            },
            with_zookeeper=True,
            stay_alive=True,
            with_minio=True,
            macros={"replica": "node1", "shard": "shard1"},
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/storage_configuration.xml",
                "configs/config.d/remote_servers.xml",
            ],
            metrika_xml="configs/metrika.xml",
            with_zookeeper=True,
            stay_alive=True,
            with_minio=True,
            macros={"replica": "node2", "shard": "shard1"},
        )
        cluster.add_instance(
            "node3",
            main_configs=[
                "configs/config.d/storage_configuration.xml",
                "configs/config.d/remote_servers.xml",
                "configs/config.d/mergetree_settings.xml",
            ],
            stay_alive=True,
            with_minio=True,
        )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_merge_tree_disk_setting(start_cluster):
    TABLE_NAME = "test_merge_tree_disk_setting"
    node1 = cluster.instances["node1"]
    minio = cluster.minio_client

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3';
    """
    )

    count = len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))

    node1.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))
        > count
    )

    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}_2 (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3';
    """
    )

    node1.query(f"INSERT INTO {TABLE_NAME}_2 SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}_2")) == 100

    assert (
        "__s3"
        in node1.query(
            f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}'"
        ).strip()
    )
    assert (
        "__s3"
        in node1.query(
            f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}_2'"
        ).strip()
    )

    node1.query("SYSTEM RELOAD CONFIG")
    assert not node1.contains_in_log(
        "An error has occurred while reloading storage policies, storage policies were not applied"
    )
    assert (
        "['s3']"
        in node1.query(
            "SELECT disks FROM system.storage_policies WHERE policy_name = '__s3'"
        ).strip()
    )

    node1.restart_clickhouse()

    assert (
        "_s3"
        in node1.query(
            f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}'"
        ).strip()
    )
    assert (
        "['s3']"
        in node1.query(
            "SELECT disks FROM system.storage_policies WHERE policy_name = '__s3'"
        ).strip()
    )
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    node1.query(f"DROP TABLE {TABLE_NAME} SYNC")
    node1.query(f"DROP TABLE {TABLE_NAME}_2 SYNC")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")


def test_merge_tree_custom_disk_setting(start_cluster):
    TABLE_NAME = "test_merge_tree_custom_disk_setting"
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    minio = cluster.minio_client
    zk_client = start_cluster.get_kazoo_client("zoo1")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    if not zk_client.exists("/minio"):
        zk_client.create("/minio")
    if not zk_client.exists("/minio/access_key_id"):
        zk_client.create("/minio/access_key_id")

    zk_client.set("/minio/access_key_id", b"minio")
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS
            disk = disk(
                type=s3,
                include = 'include_endpoint',
                access_key_id = 'from_zk /minio/access_key_id',
                secret_access_key='from_env MINIO_SECRET');
    """
    )

    # Check that data was indeed created on s3 with the needed path in s3

    count = len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))

    node1.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))
        > count
    )

    # Check that data for the second table was created on the same disk on the same path

    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}_2 (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS
            disk = disk(
                type=s3,
                endpoint='http://minio1:9001/root/data/',
                access_key_id='minio',
                secret_access_key='{minio_secret_key}');
    """
    )

    count = len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))
    node1.query(f"INSERT INTO {TABLE_NAME}_2 SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}_2")) == 100
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)))
        > count
    )

    # Check that data for a disk with a different path was created on the different path

    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}_3 (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS
            disk = disk(
                type=s3,
                endpoint='http://minio1:9001/root/data2/',
                access_key_id='minio',
                secret_access_key='{minio_secret_key}');
    """
    )

    list1 = list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    count1 = len(list1)

    node1.query(f"INSERT INTO {TABLE_NAME}_3 SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}_3")) == 100

    list2 = list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    count2 = len(list2)

    if count1 != count2:
        logging.info(f"list1: {list1}")
        logging.info(f"list2: {list2}")

    assert count1 == count2
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data2/", recursive=True)))
        > 0
    )

    # check DETACH ATTACH

    node1.query(f"DETACH TABLE {TABLE_NAME}")
    node1.query(f"ATTACH TABLE {TABLE_NAME}")

    node1.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 200

    # check after server restart the same disk path is used with the same metadata

    node1.restart_clickhouse()

    node1.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 300

    # check reload config does not wipe custom disk

    node1.query("SYSTEM RELOAD CONFIG")
    assert not node1.contains_in_log(
        "disappeared from configuration, this change will be applied after restart of ClickHouse"
    )
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 300

    # check replicated merge tree on cluster

    replica = "{replica}"
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}_4 ON CLUSTER 'cluster' (a Int32)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')
        ORDER BY tuple()
        SETTINGS
            disk = disk(
                name='test_name',
                type=s3,
                endpoint='http://minio1:9001/root/data2/',
                access_key_id='minio',
                secret_access_key='{minio_secret_key}');
    """
    )

    expected = """
        SETTINGS disk = disk(name = \\'test_name\\', type = s3, endpoint = \\'[HIDDEN]\\', access_key_id = \\'[HIDDEN]\\', secret_access_key = \\'[HIDDEN]\\'), index_granularity = 8192
    """

    assert expected.strip() in node1.query(f"SHOW CREATE TABLE {TABLE_NAME}_4").strip()
    assert expected.strip() in node2.query(f"SHOW CREATE TABLE {TABLE_NAME}_4").strip()

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    assert expected.strip() in node1.query(f"SHOW CREATE TABLE {TABLE_NAME}_4").strip()
    assert expected.strip() in node2.query(f"SHOW CREATE TABLE {TABLE_NAME}_4").strip()

    # check that disk names are the same for all replicas

    policy1 = node1.query(
        f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}_4'"
    ).strip()

    policy2 = node2.query(
        f"SELECT storage_policy FROM system.tables WHERE name = '{TABLE_NAME}_4'"
    ).strip()

    assert policy1 == policy2
    assert (
        node1.query(
            f"SELECT disks FROM system.storage_policies WHERE policy_name = '{policy1}'"
        ).strip()
        == node2.query(
            f"SELECT disks FROM system.storage_policies WHERE policy_name = '{policy2}'"
        ).strip()
    )

    node1.query(f"DROP TABLE {TABLE_NAME} SYNC")
    node1.query(f"DROP TABLE {TABLE_NAME}_2 SYNC")
    node1.query(f"DROP TABLE {TABLE_NAME}_3 SYNC")
    node1.query(f"DROP TABLE {TABLE_NAME}_4 SYNC")
    node2.query(f"DROP TABLE {TABLE_NAME}_4 SYNC")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")


def test_merge_tree_nested_custom_disk_setting(start_cluster):
    TABLE_NAME = "test_merge_tree_nested_custom_disk_setting"
    node = cluster.instances["node1"]
    minio = cluster.minio_client

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    node.query(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree() order by tuple()
        SETTINGS disk = disk(
                type=cache,
                max_size='1Gi',
                path='/var/lib/clickhouse/custom_disk_cache/',
                disk=disk(
                    type=s3,
                    endpoint='http://minio1:9001/root/data/',
                    access_key_id='minio',
                    secret_access_key='{minio_secret_key}'));
    """
    )

    node.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    node.query("SYSTEM CLEAR FILESYSTEM CACHE")

    # Check cache is filled
    assert 0 == int(node.query("SELECT count() FROM system.filesystem_cache"))
    assert 100 == int(node.query(f"SELECT count() FROM {TABLE_NAME}"))
    node.query(f"SELECT * FROM {TABLE_NAME}")
    assert 0 < int(node.query("SELECT count() FROM system.filesystem_cache"))

    # Check s3 is filled
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))) > 0
    )

    node.restart_clickhouse()

    assert 100 == int(node.query(f"SELECT count() FROM {TABLE_NAME}"))

    expected = """
        SETTINGS disk = disk(type = cache, max_size = \\'[HIDDEN]\\', path = \\'[HIDDEN]\\', disk = disk(type = s3, endpoint = \\'[HIDDEN]\\'
    """
    assert expected.strip() in node.query(f"SHOW CREATE TABLE {TABLE_NAME}").strip()
    node.query(f"DROP TABLE {TABLE_NAME} SYNC")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")


def test_merge_tree_setting_override(start_cluster):
    TABLE_NAME = "test_merge_tree_setting_override"
    node = cluster.instances["node3"]
    minio = cluster.minio_client

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    assert (
        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time"
        in node.query_and_get_error(
            f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3', storage_policy = 's3';
    """
        )
    )

    assert (
        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time"
        in node.query_and_get_error(
            f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS storage_policy = 's3';
        ALTER TABLE {TABLE_NAME} MODIFY SETTING disk = 's3';
    """
        )
    )

    node.query(f"DROP TABLE {TABLE_NAME} SYNC")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    assert (
        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time"
        in node.query_and_get_error(
            f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3';
        ALTER TABLE {TABLE_NAME} MODIFY SETTING storage_policy = 's3';
    """
        )
    )

    node.query(f"DROP TABLE {TABLE_NAME} SYNC")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    assert (
        "New storage policy `local` shall contain volumes of the old storage policy `s3`"
        in node.query_and_get_error(
            f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS storage_policy = 's3';
        ALTER TABLE {TABLE_NAME} MODIFY SETTING storage_policy = 'local';
    """
        )
    )

    node.query(f"DROP TABLE {TABLE_NAME} SYNC")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    # Using default policy so storage_policy and disk are not set at the same time
    assert (
        "New storage policy `__disk_local` shall contain disks of the old storage policy `hybrid`"
        in node.query_and_get_error(
            f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple();
        ALTER TABLE {TABLE_NAME} MODIFY SETTING disk = 'disk_local';
    """
        )
    )

    node.query(f"DROP TABLE {TABLE_NAME} SYNC")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    assert "Unknown storage policy" in node.query_and_get_error(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS storage_policy = 'kek';
    """
    )

    assert "Unknown disk" in node.query_and_get_error(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 'kek';
    """
    )

    node.query(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS
            disk = disk(
                type=s3,
                endpoint='http://minio1:9001/root/data/',
                access_key_id='minio',
                secret_access_key='{minio_secret_key}');
    """
    )

    node.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))) > 0
    )
    node.query(f"DROP TABLE {TABLE_NAME} SYNC")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    node.query(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32)
        ENGINE = MergeTree()
        ORDER BY tuple()
        SETTINGS disk = 's3'
    """
    )

    minio = cluster.minio_client
    node.query(f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)")
    assert int(node.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert (
        len(list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))) > 0
    )
    node.query(f"DROP TABLE {TABLE_NAME} SYNC")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")


@pytest.mark.parametrize("use_node", ["node1", "node2"])
def test_merge_tree_custom_encrypted_disk_include(start_cluster, use_node):
    """Test that encrypted disk configuration works with include parameter.

    This test creates an encrypted disk using the include parameter to reference
    encryption keys defined in a separate configuration file. It verifies that:
    - The include mechanism correctly merges encryption keys into the disk config
    - Data can be successfully encrypted and decrypted using the included keys
    """
    TABLE_NAME = f"test_merge_tree_custom_encrypted_disk_include_{use_node}"
    node = cluster.instances[use_node]
    minio = cluster.minio_client

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

    node.query(
        f"""
        CREATE TABLE {TABLE_NAME}_encrypted (
            id Int32,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS
            disk = disk(
                type = encrypted,
                disk = disk(
                    type = s3,
                    endpoint = 'http://minio1:9001/root/data/',
                    access_key_id = 'minio',
                    secret_access_key = '{minio_secret_key}'
                ),
                path = 'encrypted_test/',
                include = 'disk_encrypted_keys',
                algorithm = 'AES_256_CTR'
            );
        """
    )

    node.query(f"INSERT INTO {TABLE_NAME}_encrypted VALUES (1, 'test_data'), (2, 'more_data')")

    result = node.query(f"SELECT COUNT(*) FROM {TABLE_NAME}_encrypted")
    assert int(result.strip()) == 2

    result = node.query(f"SELECT data FROM {TABLE_NAME}_encrypted WHERE id = 1")
    assert result.strip() == "test_data"

    s3_objects = list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    assert len(s3_objects) > 0, "Data should be written to S3"

    node.query(f"DROP TABLE {TABLE_NAME}_encrypted SYNC")

    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data")
    wait_blobs_count_synchronization(minio, 0, bucket="root", path="data2")

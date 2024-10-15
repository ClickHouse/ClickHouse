from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/storage_configuration.xml",
    ],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_file_path_escaping(started_cluster):
    node.query(
        "CREATE DATABASE IF NOT EXISTS test ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        """
        CREATE TABLE test.`T.a_b,l-e!` (`~Id` UInt32)
        ENGINE = MergeTree() PARTITION BY `~Id` ORDER BY `~Id` SETTINGS min_bytes_for_wide_part = 0, replace_long_file_name_to_hash = 0;
        """
    )
    node.query("""INSERT INTO test.`T.a_b,l-e!` VALUES (1);""")
    node.query("""ALTER TABLE test.`T.a_b,l-e!` FREEZE;""")

    node.exec_in_container(
        [
            "bash",
            "-c",
            "test -f /var/lib/clickhouse/data/test/T%2Ea_b%2Cl%2De%21/1_1_1_0/%7EId.bin",
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "test -f /var/lib/clickhouse/shadow/1/data/test/T%2Ea_b%2Cl%2De%21/1_1_1_0/%7EId.bin",
        ]
    )

    node.query("CREATE DATABASE IF NOT EXISTS `test 2` ENGINE = Atomic")
    node.query(
        """
        CREATE TABLE `test 2`.`T.a_b,l-e!` UUID '12345678-1000-4000-8000-000000000001' (`~Id` UInt32)
        ENGINE = MergeTree() PARTITION BY `~Id` ORDER BY `~Id` SETTINGS min_bytes_for_wide_part = 0, replace_long_file_name_to_hash = 0;
        """
    )
    node.query("""INSERT INTO `test 2`.`T.a_b,l-e!` VALUES (1);""")
    node.query("""ALTER TABLE `test 2`.`T.a_b,l-e!` FREEZE;""")

    node.exec_in_container(
        [
            "bash",
            "-c",
            "test -f /var/lib/clickhouse/store/123/12345678-1000-4000-8000-000000000001/1_1_1_0/%7EId.bin",
        ]
    )
    # Check symlink
    node.exec_in_container(
        ["bash", "-c", "test -L /var/lib/clickhouse/data/test%202/T%2Ea_b%2Cl%2De%21"]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "test -f /var/lib/clickhouse/data/test%202/T%2Ea_b%2Cl%2De%21/1_1_1_0/%7EId.bin",
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "test -f /var/lib/clickhouse/shadow/2/store/123/12345678-1000-4000-8000-000000000001/1_1_1_0/%7EId.bin",
        ]
    )

    node.query("DROP TABLE test.`T.a_b,l-e!` SYNC")
    node.query("DROP TABLE `test 2`.`T.a_b,l-e!` SYNC")
    node.query("DROP DATABASE test")
    node.query("DROP DATABASE `test 2`")


def test_data_directory_symlinks(started_cluster):
    node.query("CREATE DATABASE IF NOT EXISTS test_symlinks ENGINE = Atomic")

    node.query(
        sql="""
            CREATE TABLE test_symlinks.default UUID '87654321-1000-4000-8000-000000000001'
            (Id UInt32)
            ENGINE = MergeTree()
            PARTITION BY Id
            ORDER BY Id
            """,
        database="test_symlinks",
    )

    node.query(
        """
        CREATE TABLE test_symlinks.s3 UUID '87654321-1000-4000-8000-000000000002'
        (Id UInt32)
        ENGINE = MergeTree()
        PARTITION BY Id
        ORDER BY Id
        SETTINGS storage_policy = 's3'
        """
    )

    node.query(
        """
        CREATE TABLE test_symlinks.jbod UUID '87654321-1000-4000-8000-000000000003'
        (Id UInt32)
        ENGINE = MergeTree()
        PARTITION BY Id
        ORDER BY Id
        SETTINGS storage_policy = 'jbod'
        """
    )

    clickhouse_dir = Path("/var/lib/clickhouse/")

    database_dir = clickhouse_dir / "data" / "test_symlinks"
    default_symlink = database_dir / "default"
    s3_symlink = database_dir / "s3"
    jbod_symlink = database_dir / "jbod"

    default_data = (
        clickhouse_dir / "store" / "876" / "87654321-1000-4000-8000-000000000001"
    )
    s3_data = (
        clickhouse_dir
        / "disks"
        / "s3"
        / "store"
        / "876"
        / "87654321-1000-4000-8000-000000000002"
    )
    jbod_data = Path("jbod1") / "store" / "876" / "87654321-1000-4000-8000-000000000003"

    node.restart_clickhouse()

    assert (
        node.exec_in_container(["bash", "-c", f"ls -l {default_symlink}"])
        .strip()
        .endswith(f"{default_symlink} -> {default_data}")
    )
    assert (
        node.exec_in_container(["bash", "-c", f"ls -l {s3_symlink}"])
        .strip()
        .endswith(f"{s3_symlink} -> {s3_data}")
    )
    assert (
        node.exec_in_container(["bash", "-c", f"ls -l {jbod_symlink}"])
        .strip()
        .endswith(f"{jbod_symlink} -> /{jbod_data}")
    )

    node.query("DROP TABLE test_symlinks.default SYNC")
    node.query("DROP TABLE test_symlinks.s3 SYNC")
    node.query("DROP TABLE test_symlinks.jbod SYNC")
    node.query("DROP DATABASE test_symlinks")

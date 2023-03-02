import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_file_path_escaping(started_cluster):
    node.query("CREATE DATABASE IF NOT EXISTS test ENGINE = Ordinary")
    node.query(
        """
        CREATE TABLE test.`T.a_b,l-e!` (`~Id` UInt32)
        ENGINE = MergeTree() PARTITION BY `~Id` ORDER BY `~Id` SETTINGS min_bytes_for_wide_part = 0;
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


def test_file_path_escaping_atomic_db(started_cluster):
    node.query("CREATE DATABASE IF NOT EXISTS `test 2` ENGINE = Atomic")
    node.query(
        """
        CREATE TABLE `test 2`.`T.a_b,l-e!` UUID '12345678-1000-4000-8000-000000000001' (`~Id` UInt32)
        ENGINE = MergeTree() PARTITION BY `~Id` ORDER BY `~Id` SETTINGS min_bytes_for_wide_part = 0;
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

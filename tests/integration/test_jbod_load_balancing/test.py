# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/storage_configuration.xml",
    ],
    tmpfs=[
        "/jbod1:size=100M",
        "/jbod2:size=200M",
        "/jbod3:size=300M",
    ],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_jbod_load_balancing_round_robin(start_cluster):
    try:
        node.query(
            """
            CREATE TABLE data_round_robin (p UInt8)
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_round_robin';

            SYSTEM STOP MERGES data_round_robin;

            INSERT INTO data_round_robin SELECT * FROM numbers(10);
            INSERT INTO data_round_robin SELECT * FROM numbers(10);
            INSERT INTO data_round_robin SELECT * FROM numbers(10);
            INSERT INTO data_round_robin SELECT * FROM numbers(10);
        """
        )

        parts = node.query(
            """
        SELECT count(), disk_name
        FROM system.parts
        WHERE table = 'data_round_robin'
        GROUP BY disk_name
        ORDER BY disk_name
        """
        )
        parts = [l.split("\t") for l in parts.strip().split("\n")]
        assert parts == [
            ["2", "jbod1"],
            ["1", "jbod2"],
            ["1", "jbod3"],
        ]
    finally:
        node.query("DROP TABLE IF EXISTS data_round_robin SYNC")


def test_jbod_load_balancing_least_used(start_cluster):
    try:
        node.query(
            """
            CREATE TABLE data_least_used (p UInt8)
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_least_used';

            SYSTEM STOP MERGES data_least_used;

            INSERT INTO data_least_used SELECT * FROM numbers(10);
            INSERT INTO data_least_used SELECT * FROM numbers(10);
            INSERT INTO data_least_used SELECT * FROM numbers(10);
            INSERT INTO data_least_used SELECT * FROM numbers(10);
        """
        )

        parts = node.query(
            """
        SELECT count(), disk_name
        FROM system.parts
        WHERE table = 'data_least_used'
        GROUP BY disk_name
        ORDER BY disk_name
        """
        )
        parts = [l.split("\t") for l in parts.strip().split("\n")]
        assert parts == [
            ["4", "jbod3"],
        ]
    finally:
        node.query("DROP TABLE IF EXISTS data_least_used SYNC")


def test_jbod_load_balancing_least_used_next_disk(start_cluster):
    try:
        node.query(
            """
            CREATE TABLE data_least_used_next_disk
            (
                s String CODEC(NONE)
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_least_used';

            SYSTEM STOP MERGES data_least_used_next_disk;

            -- 100MiB each part, 3 parts in total
            INSERT INTO data_least_used_next_disk SELECT repeat('a', 100) FROM numbers(3e6) SETTINGS max_block_size='1Mi';
        """
        )

        parts = node.query(
            """
        SELECT count(), disk_name
        FROM system.parts
        WHERE table = 'data_least_used_next_disk'
        GROUP BY disk_name
        ORDER BY disk_name
        """
        )
        parts = [l.split("\t") for l in parts.strip().split("\n")]
        assert parts == [
            ["1", "jbod2"],
            ["2", "jbod3"],
        ]
    finally:
        node.query("DROP TABLE IF EXISTS data_least_used_next_disk SYNC")


def test_jbod_load_balancing_least_used_detect_background_changes(start_cluster):
    def get_parts_on_disks():
        parts = node.query(
            """
        SELECT count(), disk_name
        FROM system.parts
        WHERE table = 'data_least_used_detect_background_changes'
        GROUP BY disk_name
        ORDER BY disk_name
        """
        )
        parts = [l.split("\t") for l in parts.strip().split("\n")]
        return parts

    try:
        node.query(
            """
            CREATE TABLE data_least_used_detect_background_changes (p UInt8)
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_least_used';

            SYSTEM STOP MERGES data_least_used_detect_background_changes;
            """
        )

        node.exec_in_container(["fallocate", "-l200M", "/jbod3/.test"])
        node.query(
            """
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
        """
        )
        parts = get_parts_on_disks()
        assert parts == [
            ["4", "jbod2"],
        ]

        node.exec_in_container(["rm", "/jbod3/.test"])
        node.query(
            """
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
            INSERT INTO data_least_used_detect_background_changes SELECT * FROM numbers(10);
        """
        )
        parts = get_parts_on_disks()
        assert parts == [
            # previous INSERT
            ["4", "jbod2"],
            # this INSERT
            ["4", "jbod3"],
        ]
    finally:
        node.exec_in_container(["rm", "-f", "/jbod3/.test"])
        node.query(
            "DROP TABLE IF EXISTS data_least_used_detect_background_changes SYNC"
        )

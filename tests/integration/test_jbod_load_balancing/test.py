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
        "/test_jbod_load_balancing_jbod1:size=100M",
        "/test_jbod_load_balancing_jbod2:size=200M",
        "/test_jbod_load_balancing_jbod3:size=300M",
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

            -- 100MiB each part, 3 parts in total.
            -- max_insert_threads = 1 keeps the parts written one after another: with
            -- concurrent writers a reserve can observe another part mid-write, and
            -- which disk is least used at that moment is interleaving dependent.
            INSERT INTO data_least_used_next_disk SELECT repeat('a', 100) FROM numbers(3e6) SETTINGS max_block_size='1Mi', max_insert_threads = 1;
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

        node.exec_in_container(["fallocate", "-l200M", "/test_jbod_load_balancing_jbod3/.test"])
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

        node.exec_in_container(["rm", "/test_jbod_load_balancing_jbod3/.test"])
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
        node.exec_in_container(["rm", "-f", "/test_jbod_load_balancing_jbod3/.test"])
        node.query(
            "DROP TABLE IF EXISTS data_least_used_detect_background_changes SYNC"
        )


def test_least_used_reservation_tries_other_disks(start_cluster):
    table = "least_used_reservation"
    medium_file = "/test_jbod_load_balancing_jbod2/reservation_filler"
    large_file = "/test_jbod_load_balancing_jbod3/reservation_filler"

    try:
        # Both larger disks have more free bytes than the smallest disk, but
        # neither has 75% of its capacity free.
        node.exec_in_container(["fallocate", "-l", "70M", medium_file])
        node.exec_in_container(["fallocate", "-l", "130M", large_file])
        node.query(
            f"""
            CREATE TABLE {table} (n UInt8) ENGINE = MergeTree ORDER BY n
            SETTINGS storage_policy = 'jbod_least_used',
                     min_free_disk_ratio_to_perform_insert = 0.75
            """
        )
        node.query(f"INSERT INTO {table} VALUES (1)")
        assert node.query(
            f"SELECT disk_name FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active"
        ).strip() == "jbod1"
    finally:
        node.exec_in_container(["rm", "-f", medium_file, large_file])
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")

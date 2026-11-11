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


def get_parts_on_disks(table):
    parts = node.query(
        f"""
    SELECT count(), disk_name
    FROM system.parts
    WHERE table = '{table}'
    GROUP BY disk_name
    ORDER BY disk_name
    """
    )
    parts = [l.split("\t") for l in parts.strip().split("\n")]
    return parts


def test_jbod_load_balancing_least_used_detect_background_changes(start_cluster):
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
        parts = get_parts_on_disks("data_least_used_detect_background_changes")
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
        parts = get_parts_on_disks("data_least_used_detect_background_changes")
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


def test_jbod_load_balancing_least_used_ttl(start_cluster):
    # Between two reads of the space of every disk, the volume updates the space of the disk that a
    # reservation takes. Reloading the config creates the volume anew, so it starts from the space of 100, 200
    # and 300 MiB. Each insert reserves the 143 MiB of its block, although its compressed part is small. The
    # first insert takes `jbod3`, which then has 157 MiB left, so the second insert takes `jbod2`.
    try:
        node.query("SYSTEM RELOAD CONFIG")
        node.query(
            """
            CREATE TABLE data_least_used_ttl (s String)
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_least_used_ttl';

            SYSTEM STOP MERGES data_least_used_ttl;

            INSERT INTO data_least_used_ttl SELECT repeat('a', 1000000) FROM numbers(150) SETTINGS max_insert_threads = 1;
            INSERT INTO data_least_used_ttl SELECT repeat('a', 1000000) FROM numbers(150) SETTINGS max_insert_threads = 1;
            """
        )
        parts = get_parts_on_disks("data_least_used_ttl")
        assert parts == [
            ["1", "jbod2"],
            ["1", "jbod3"],
        ]
    finally:
        node.query("DROP TABLE IF EXISTS data_least_used_ttl SYNC")


def test_jbod_load_balancing_least_used_ttl_full_disk(start_cluster):
    # `jbod3` is filled to 8 KiB free behind the back of the volume, which may still count its old space and
    # try it first. A reservation tries every disk once, so the insert takes `jbod1` or `jbod2`, whatever
    # space the volume counts for each disk.
    disk = "/test_jbod_load_balancing_jbod3"
    try:
        node.query(
            """
            CREATE TABLE data_least_used_ttl_full_disk (p UInt8)
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_least_used_ttl';
            """
        )
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"fallocate -l $(( $(df --output=avail -B1 {disk} | tail -1) - 8192 )) {disk}/.filler",
            ]
        )
        node.query(
            "INSERT INTO data_least_used_ttl_full_disk SELECT * FROM numbers(10)"
        )
        parts = get_parts_on_disks("data_least_used_ttl_full_disk")
        assert parts in ([["1", "jbod1"]], [["1", "jbod2"]])
    finally:
        node.exec_in_container(["rm", "-f", f"{disk}/.filler"])
        node.query("DROP TABLE IF EXISTS data_least_used_ttl_full_disk SYNC")

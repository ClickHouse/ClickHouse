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


def test_jbod_load_balancing_least_used_default_ttl(start_cluster):
    # The other least_used tests pin least_used_ttl_ms = 0, which rebuilds the volume's
    # disk queue on every reservation. This one leaves the setting at its default so the
    # cached queue is the thing under test: five 62MiB parts fit the volume (600MiB) but
    # not any single disk, so placement has to move on once a disk fills up.
    #
    # The reload re-creates the volume, re-seeding its cached per-disk free space from
    # statvfs, so a repeated run does not inherit the previous run's cache. It is fixture
    # isolation only: with the reload in place this test still fails on an unfixed server.
    node.query("SYSTEM RELOAD CONFIG")
    try:
        node.query(
            """
            CREATE TABLE data_least_used_default_ttl
            (
                s String CODEC(NONE)
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy = 'jbod_least_used_default_ttl';

            SYSTEM STOP MERGES data_least_used_default_ttl;
            """
        )

        # Sequential inserts, one thread each, so every reservation observes the previous
        # part already finalized.
        for _ in range(5):
            node.query(
                """
                INSERT INTO data_least_used_default_ttl
                SELECT repeat('a', 100) FROM numbers(6e5) SETTINGS max_insert_threads = 1;
                """
            )

        # Exact placement is not asserted: which disk wins depends on how many bytes each
        # part rounds up to. What must hold is that the parts get spread instead of piling
        # onto one disk until it is full.
        total, disks_used = (
            node.query(
                """
                SELECT count(), uniqExact(disk_name)
                FROM system.parts
                WHERE table = 'data_least_used_default_ttl' AND active
                """
            )
            .strip()
            .split("\t")
        )
        assert int(total) == 5
        assert int(disks_used) >= 2
    finally:
        node.query("DROP TABLE IF EXISTS data_least_used_default_ttl SYNC")


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

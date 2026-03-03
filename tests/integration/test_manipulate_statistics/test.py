import logging
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    user_configs=["config/config.xml"],
    with_zookeeper=True,
    macros={"replica": "a", "shard": "shard1"},
)

node2 = cluster.add_instance(
    "node2",
    user_configs=["config/config.xml"],
    with_zookeeper=True,
    macros={"replica": "b", "shard": "shard1"},
)

node3 = cluster.add_instance(
    "node3",
    user_configs=["config/config.xml"],
    with_zookeeper=True,
)

node_old = cluster.add_instance(
    "node_old",
    image="clickhouse/clickhouse-server",
    tag="25.12",
    with_installed_binary=True,
    stay_alive=True,
    user_configs=["config/config.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def check_stats_in_part(
    node, table, part_name, column_name, exists_in_part, exists_packed_file_on_disk
):
    output = node.query(
        f"SELECT notEmpty(statistics) FROM system.parts_columns WHERE table = '{table}' AND name = '{part_name}' AND column = '{column_name}'"
    ).strip()

    if exists_in_part:
        assert output == "1"
    else:
        assert output == "" or output == "0"

    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(
            table, part_name
        )
    ).strip()

    assert len(part_path) != 0

    output = node.exec_in_container(
        [
            "bash",
            "-c",
            "find {p} -type f -name statistics.packed".format(
                p=part_path, col=column_name
            ),
        ],
        privileged=True,
    )
    logging.debug(
        f"Checking stats file in {part_path} for column {column_name}, got {output}"
    )
    if exists_packed_file_on_disk:
        assert len(output) != 0
    else:
        assert len(output) == 0


def run_test_single_node(started_cluster):
    node1.query("INSERT INTO test_stat VALUES (1,2,3), (4,5,6)")

    check_stats_in_part(node1, "test_stat", "all_1_1_0", "a", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0", "b", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0", "c", True, True)

    node1.query("ALTER TABLE test_stat DROP STATISTICS a")

    check_stats_in_part(node1, "test_stat", "all_1_1_0_2", "a", False, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_2", "b", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_2", "c", True, True)

    node1.query("ALTER TABLE test_stat CLEAR STATISTICS b, c")

    check_stats_in_part(node1, "test_stat", "all_1_1_0_3", "a", False, False)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_3", "b", False, False)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_3", "c", False, False)

    node1.query("ALTER TABLE test_stat MATERIALIZE STATISTICS b, c")

    check_stats_in_part(node1, "test_stat", "all_1_1_0_4", "a", False, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_4", "b", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_4", "c", True, True)

    node1.query("ALTER TABLE test_stat ADD STATISTICS a type tdigest")
    node1.query("ALTER TABLE test_stat MATERIALIZE STATISTICS a")

    check_stats_in_part(node1, "test_stat", "all_1_1_0_5", "a", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_5", "b", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_5", "c", True, True)

    node1.query("ALTER TABLE test_stat DROP COLUMN c")
    check_stats_in_part(node1, "test_stat", "all_1_1_0_6", "a", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_6", "b", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_6", "c", False, True)

    node1.query("ALTER TABLE test_stat RENAME COLUMN b TO c")

    check_stats_in_part(node1, "test_stat", "all_1_1_0_7", "a", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_7", "b", False, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_7", "c", True, True)

    node1.query("ALTER TABLE test_stat RENAME COLUMN c TO b")

    check_stats_in_part(node1, "test_stat", "all_1_1_0_8", "a", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_8", "b", True, True)
    check_stats_in_part(node1, "test_stat", "all_1_1_0_8", "c", False, True)


def test_single_node_wide(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_stat")

    node1.query(
        """
        CREATE TABLE test_stat(a Int64 STATISTICS(tdigest), b Int64 STATISTICS(tdigest), c Int64 STATISTICS(tdigest))
        ENGINE = MergeTree() ORDER BY a
        SETTINGS min_bytes_for_wide_part = 0;
    """
    )
    run_test_single_node(started_cluster)


def test_single_node_normal(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_stat")

    node1.query(
        """
        CREATE TABLE test_stat(a Int64 STATISTICS(tdigest), b Int64 STATISTICS(tdigest), c Int64 STATISTICS(tdigest))
        ENGINE = MergeTree() ORDER BY a;
    """
    )
    run_test_single_node(started_cluster)


def test_replicated_table_ddl(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_stat SYNC")
    node2.query("DROP TABLE IF EXISTS test_stat SYNC")

    node1.query(
        """
        CREATE TABLE test_stat(a Int64 STATISTICS(tdigest, uniq), b Int64 STATISTICS(tdigest, uniq), c Int64 STATISTICS(tdigest))
        ENGINE = ReplicatedMergeTree('/clickhouse/test/statistics', '1') ORDER BY a;
    """
    )
    node2.query(
        """
        CREATE TABLE test_stat(a Int64 STATISTICS(tdigest, uniq), b Int64 STATISTICS(tdigest, uniq), c Int64 STATISTICS(tdigest))
        ENGINE = ReplicatedMergeTree('/clickhouse/test/statistics', '2') ORDER BY a;
    """
    )

    node1.query(
        "ALTER TABLE test_stat MODIFY STATISTICS c TYPE tdigest, uniq",
        settings={"alter_sync": "2"},
    )
    node1.query("ALTER TABLE test_stat DROP STATISTICS b", settings={"alter_sync": "2"})

    assert (
        node2.query("SHOW CREATE TABLE test_stat")
        == "CREATE TABLE default.test_stat\\n(\\n    `a` Int64 STATISTICS(tdigest, uniq),\\n    `b` Int64,\\n    `c` Int64 STATISTICS(tdigest, uniq)\\n)\\nENGINE = ReplicatedMergeTree(\\'/clickhouse/test/statistics\\', \\'2\\')\\nORDER BY a\\nSETTINGS index_granularity = 8192\n"
    )

    node2.query("insert into test_stat values(1,2,3), (2,3,4)")
    check_stats_in_part(node2, "test_stat", "all_0_0_0", "a", True, True)
    check_stats_in_part(node2, "test_stat", "all_0_0_0", "c", True, True)
    node1.query(
        "ALTER TABLE test_stat RENAME COLUMN c TO d", settings={"alter_sync": "2"}
    )
    assert node2.query("select sum(a), sum(d) from test_stat") == "3\t7\n"
    check_stats_in_part(node2, "test_stat", "all_0_0_0_1", "a", True, True)
    check_stats_in_part(node2, "test_stat", "all_0_0_0_1", "c", False, True)
    check_stats_in_part(node2, "test_stat", "all_0_0_0_1", "d", True, True)
    node1.query(
        "ALTER TABLE test_stat CLEAR STATISTICS d",
        settings={"alter_sync": "2", "mutations_sync": 2},
    )
    node1.query(
        "ALTER TABLE test_stat ADD STATISTICS b type tdigest",
        settings={"alter_sync": "2"},
    )
    check_stats_in_part(node2, "test_stat", "all_0_0_0_2", "a", True, True)
    check_stats_in_part(node2, "test_stat", "all_0_0_0_2", "b", False, True)
    check_stats_in_part(node2, "test_stat", "all_0_0_0_2", "d", False, True)
    node1.query(
        "ALTER TABLE test_stat MATERIALIZE STATISTICS b",
        settings={"alter_sync": "2", "mutations_sync": 2},
    )
    check_stats_in_part(node2, "test_stat", "all_0_0_0_3", "a", True, True)
    check_stats_in_part(node2, "test_stat", "all_0_0_0_3", "b", True, True)

    node1.query("DROP TABLE IF EXISTS test_stat SYNC")
    node2.query("DROP TABLE IF EXISTS test_stat SYNC")


def test_replicated_db(started_cluster):
    node1.query("DROP DATABASE IF EXISTS test SYNC")
    node2.query("DROP DATABASE IF EXISTS test SYNC")
    node1.query(
        "CREATE DATABASE test ENGINE = Replicated('/test/shared_stats', '{shard}', '{replica}')"
    )
    node2.query(
        "CREATE DATABASE test ENGINE = Replicated('/test/shared_stats', '{shard}', '{replica}')"
    )
    node1.query(
        "CREATE TABLE test.test_stats (a Int64, b Int64) ENGINE = ReplicatedMergeTree() ORDER BY()"
    )
    node2.query("ALTER TABLE test.test_stats MODIFY COLUMN b Float64")
    node2.query("ALTER TABLE test.test_stats MODIFY STATISTICS b TYPE tdigest")


def test_setting_materialize_statistics_on_merge(started_cluster):
    node3.query("DROP TABLE IF EXISTS test_stats SYNC")
    node3.query(
        "CREATE TABLE test_stats (a Int64 STATISTICS(tdigest), b Int64 STATISTICS(tdigest)) ENGINE = MergeTree() ORDER BY() settings materialize_statistics_on_merge = 0"
    )
    node3.query("SYSTEM STOP MERGES")
    node3.query("insert into test_stats values(1,2)")
    node3.query("insert into test_stats values(2,3)")
    check_stats_in_part(node3, "test_stats", "all_1_1_0", "a", True, True)
    check_stats_in_part(node3, "test_stats", "all_1_1_0", "b", True, True)
    check_stats_in_part(node3, "test_stats", "all_2_2_0", "a", True, True)
    check_stats_in_part(node3, "test_stats", "all_2_2_0", "b", True, True)
    node3.query("SYSTEM START MERGES")
    node3.query("OPTIMIZE TABLE test_stats FINAL")
    check_stats_in_part(node3, "test_stats", "all_1_2_1", "a", False, False)
    check_stats_in_part(node3, "test_stats", "all_1_2_1", "b", False, False)
    node3.query("DROP TABLE IF EXISTS test_stats SYNC")


def test_statistics_upgrade_from_old_version(started_cluster):
    node = node_old

    node.query(
        """
        CREATE TABLE test_stat_upgrade(
            a Int64,
            b Int64,
            c Int64
        )
        ENGINE = MergeTree() ORDER BY a
        SETTINGS
            min_bytes_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0,
            auto_statistics_types = 'tdigest,minmax,countmin,uniq';
    """
    )

    node.query("INSERT INTO test_stat_upgrade VALUES (1,2,3), (4,5,6)")

    def get_part_path(node):
        part_path = node.query(
            "SELECT path FROM system.parts WHERE table = 'test_stat_upgrade' AND active = 1"
        ).strip()

        assert len(part_path) != 0
        return part_path

    def get_old_stats_files(node):
        part_path = get_part_path(node)

        old_stats_files = node.exec_in_container(
            [
                "bash",
                "-c",
                "find {} -type f -name 'statistics_*.stats'".format(part_path),
            ],
            privileged=True,
        ).strip()

        return old_stats_files

    def verify_old_stats_files(node, part_name):
        old_stats_files = get_old_stats_files(node)

        assert "statistics_a.stats" in old_stats_files
        assert "statistics_b.stats" in old_stats_files
        assert "statistics_c.stats" in old_stats_files

        check_stats_in_part(node, "test_stat_upgrade", part_name, "a", True, False)
        check_stats_in_part(node, "test_stat_upgrade", part_name, "b", True, False)
        check_stats_in_part(node, "test_stat_upgrade", part_name, "c", True, False)

    # Verify on old version.
    verify_old_stats_files(node, "all_1_1_0")
    # Restart with a new version.
    node.restart_with_latest_version()
    # Verify on new version.
    verify_old_stats_files(node, "all_1_1_0")
    # Materialize on a new version.
    node.query("ALTER TABLE test_stat_upgrade MATERIALIZE STATISTICS a, b, c SETTINGS mutations_sync = 2")

    check_stats_in_part(node, "test_stat_upgrade", "all_1_1_0_2", "a", True, True)
    check_stats_in_part(node, "test_stat_upgrade", "all_1_1_0_2", "b", True, True)
    check_stats_in_part(node, "test_stat_upgrade", "all_1_1_0_2", "c", True, True)

    old_stats_files = get_old_stats_files(node)

    # Verify old per-column statistics files are removed on a new version.
    assert (
        len(old_stats_files) == 0
    ), "Old per-column statistics files should be removed, found: {}".format(
        old_stats_files
    )

    node.query("DROP TABLE IF EXISTS test_stat_upgrade SYNC")
    node.restart_with_original_version()

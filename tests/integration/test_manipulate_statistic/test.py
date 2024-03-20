import pytest
import logging

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", user_configs=["config/config.xml"], with_zookeeper=False
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def check_stat_file_on_disk(node, table, part_name, column_name, exist):
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
            "find {p} -type f -name statistic_{col}.stat".format(
                p=part_path, col=column_name
            ),
        ],
        privileged=True,
    )
    logging.debug(
        f"Checking stat file in {part_path} for column {column_name}, got {output}"
    )
    if exist:
        assert len(output) != 0
    else:
        assert len(output) == 0


def run_test_single_node(started_cluster):
    node1.query("INSERT INTO test_stat VALUES (1,2,3), (4,5,6)")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0", "a", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0", "c", True)

    node1.query("ALTER TABLE test_stat DROP STATISTIC a type tdigest")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_2", "a", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_2", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_2", "c", True)

    node1.query("ALTER TABLE test_stat CLEAR STATISTIC b, c type tdigest")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_3", "a", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_3", "b", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_3", "c", False)

    node1.query("ALTER TABLE test_stat MATERIALIZE STATISTIC b, c type tdigest")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_4", "a", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_4", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_4", "c", True)

    node1.query("ALTER TABLE test_stat ADD STATISTIC a type tdigest")
    node1.query("ALTER TABLE test_stat MATERIALIZE STATISTIC a type tdigest")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_5", "a", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_5", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_5", "c", True)

    node1.query("ALTER TABLE test_stat DROP COLUMN c")
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_6", "a", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_6", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_6", "c", False)

    node1.query("ALTER TABLE test_stat RENAME COLUMN b TO c")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_7", "a", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_7", "b", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_7", "c", True)

    node1.query("ALTER TABLE test_stat RENAME COLUMN c TO b")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_8", "a", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_8", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_8", "c", False)


def test_single_node_wide(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_stat")

    node1.query(
        """
        CREATE TABLE test_stat(a Int64 STATISTIC(tdigest), b Int64 STATISTIC(tdigest), c Int64 STATISTIC(tdigest))
        ENGINE = MergeTree() ORDER BY a
        SETTINGS min_bytes_for_wide_part = 0;
    """
    )
    run_test_single_node(started_cluster)


def test_single_node_normal(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_stat")

    node1.query(
        """
        CREATE TABLE test_stat(a Int64 STATISTIC(tdigest), b Int64 STATISTIC(tdigest), c Int64 STATISTIC(tdigest))
        ENGINE = MergeTree() ORDER BY a;
    """
    )
    run_test_single_node(started_cluster)

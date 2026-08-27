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
            "find {p} -type f -name statistics_{col}.stats".format(
                p=part_path, col=column_name
            ),
        ],
        privileged=True,
    )
    logging.debug(
        f"Checking stats file in {part_path} for column {column_name}, got {output}"
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

    node1.query("ALTER TABLE test_stat DROP STATISTICS a")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_2", "a", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_2", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_2", "c", True)

    node1.query("ALTER TABLE test_stat CLEAR STATISTICS b, c")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_3", "a", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_3", "b", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_3", "c", False)

    node1.query("ALTER TABLE test_stat MATERIALIZE STATISTICS b, c")

    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_4", "a", False)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_4", "b", True)
    check_stat_file_on_disk(node1, "test_stat", "all_1_1_0_4", "c", True)

    node1.query("ALTER TABLE test_stat ADD STATISTICS a type tdigest")
    node1.query("ALTER TABLE test_stat MATERIALIZE STATISTICS a")

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
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0", "a", True)
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0", "c", True)
    node1.query(
        "ALTER TABLE test_stat RENAME COLUMN c TO d", settings={"alter_sync": "2"}
    )
    assert node2.query("select sum(a), sum(d) from test_stat") == "3\t7\n"
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_1", "a", True)
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_1", "c", False)
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_1", "d", True)
    node1.query(
        "ALTER TABLE test_stat CLEAR STATISTICS d",
        settings={"alter_sync": "2", "mutations_sync": 2},
    )
    node1.query(
        "ALTER TABLE test_stat ADD STATISTICS b type tdigest",
        settings={"alter_sync": "2"},
    )
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_2", "a", True)
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_2", "b", False)
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_2", "d", False)
    node1.query(
        "ALTER TABLE test_stat MATERIALIZE STATISTICS b",
        settings={"alter_sync": "2", "mutations_sync": 2},
    )
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_3", "a", True)
    check_stat_file_on_disk(node2, "test_stat", "all_0_0_0_3", "b", True)


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

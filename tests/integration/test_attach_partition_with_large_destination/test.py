import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_force_drop_flag(node):
    force_drop_flag_path = "/var/lib/clickhouse/flags/force_drop_table"
    node.exec_in_container(
        [
            "bash",
            "-c",
            "touch {} && chmod a=rw {}".format(
                force_drop_flag_path, force_drop_flag_path
            ),
        ],
        user="root",
    )


@pytest.mark.parametrize("engine", ["Ordinary", "Atomic"])
def test_attach_partition_with_large_destination(started_cluster, engine):
    # Initialize
    node.query(
        "CREATE DATABASE db ENGINE={}".format(engine),
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        "CREATE TABLE db.destination (n UInt64) ENGINE=ReplicatedMergeTree('/test/destination', 'r1') ORDER BY n PARTITION BY n % 2"
    )
    node.query(
        "CREATE TABLE db.source_1 (n UInt64) ENGINE=ReplicatedMergeTree('/test/source_1', 'r1') ORDER BY n PARTITION BY n % 2"
    )
    node.query("INSERT INTO db.source_1 VALUES (1), (2), (3), (4)")
    node.query(
        "CREATE TABLE db.source_2 (n UInt64) ENGINE=ReplicatedMergeTree('/test/source_2', 'r1') ORDER BY n PARTITION BY n % 2"
    )
    node.query("INSERT INTO db.source_2 VALUES (5), (6), (7), (8)")

    # Attach partition when destination partition is empty
    node.query("ALTER TABLE db.destination ATTACH PARTITION 0 FROM db.source_1")
    assert node.query("SELECT n FROM db.destination ORDER BY n") == "2\n4\n"

    # REPLACE PARTITION should still respect max_partition_size_to_drop
    assert node.query_and_get_error(
        "ALTER TABLE db.destination REPLACE PARTITION 0 FROM db.source_2"
    )
    assert node.query("SELECT n FROM db.destination ORDER BY n") == "2\n4\n"

    # Attach partition when destination partition is larger than max_partition_size_to_drop
    node.query("ALTER TABLE db.destination ATTACH PARTITION 0 FROM db.source_2")
    assert node.query("SELECT n FROM db.destination ORDER BY n") == "2\n4\n6\n8\n"

    # Cleanup
    create_force_drop_flag(node)
    node.query("DROP TABLE db.source_1 SYNC")
    create_force_drop_flag(node)
    node.query("DROP TABLE db.source_2 SYNC")
    create_force_drop_flag(node)
    node.query("DROP TABLE db.destination SYNC")
    node.query("DROP DATABASE db")

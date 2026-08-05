import pytest

import helpers.client
import helpers.cluster

cluster = helpers.cluster.ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/logs_config.xml", "configs/cluster.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/logs_config.xml", "configs/cluster.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/logs_config.xml", "configs/cluster.xml"],
    user_configs=["configs/max_threads.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_trivial_alter_in_partition_merge_tree_without_where(started_cluster):
    try:
        name = "test_trivial_alter_in_partition_merge_tree_without_where"
        node1.query("DROP TABLE IF EXISTS {}".format(name))
        node1.query(
            "CREATE TABLE {} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p".format(
                name
            )
        )
        node1.query("INSERT INTO {} VALUES (1, 2), (2, 3)".format(name))
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 1 SETTINGS mutations_sync = 2".format(
                    name
                )
            )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 2 SETTINGS mutations_sync = 2".format(
                    name
                )
            )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} DELETE IN PARTITION 1 SETTINGS mutations_sync = 2".format(
                    name
                )
            )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} DELETE IN PARTITION 2 SETTINGS mutations_sync = 2".format(
                    name
                )
            )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["5"]
    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


def test_trivial_alter_in_partition_merge_tree_with_where(started_cluster):
    try:
        name = "test_trivial_alter_in_partition_merge_tree_with_where"
        node1.query("DROP TABLE IF EXISTS {}".format(name))
        node1.query(
            "CREATE TABLE {} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p".format(
                name
            )
        )
        node1.query("INSERT INTO {} VALUES (1, 2), (2, 3)".format(name))
        node1.query(
            "ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        assert node1.query("SELECT x FROM {} ORDER BY p".format(name)).splitlines() == [
            "2",
            "4",
        ]
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        node1.query(
            "ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["6"]
        node1.query(
            "ALTER TABLE {} DELETE IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["2"]
        node1.query(
            "ALTER TABLE {} DELETE IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        assert node1.query("SELECT sum(x) FROM {}".format(name)).splitlines() == ["2"]
    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


def test_trivial_alter_in_partition_replicated_merge_tree(started_cluster):
    try:
        name = "test_trivial_alter_in_partition_replicated_merge_tree"

        node1.query("DROP TABLE IF EXISTS {}".format(name))
        node2.query("DROP TABLE IF EXISTS {}".format(name))

        for node in (node1, node2):
            node.query(
                "CREATE TABLE {name} (p Int64, x Int64) ENGINE=ReplicatedMergeTree('/clickhouse/{name}', '{{instance}}') ORDER BY tuple() PARTITION BY p".format(
                    name=name
                )
            )

        node1.query("INSERT INTO {} VALUES (1, 2)".format(name))
        node2.query("INSERT INTO {} VALUES (2, 3)".format(name))

        node1.query(
            "ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 2 WHERE 1 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == [
                "6"
            ]
        node1.query(
            "ALTER TABLE {} UPDATE x = x + 1 IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == [
                "6"
            ]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} DELETE IN PARTITION 2 SETTINGS mutations_sync = 2".format(
                    name
                )
            )
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == [
                "6"
            ]
        node1.query(
            "ALTER TABLE {} DELETE IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == [
                "2"
            ]
        node1.query(
            "ALTER TABLE {} DELETE IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2".format(
                name
            )
        )
        for node in (node1, node2):
            assert node.query("SELECT sum(x) FROM {}".format(name)).splitlines() == [
                "2"
            ]
    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))
        node2.query("DROP TABLE IF EXISTS {}".format(name))


def test_mutation_max_streams(started_cluster):
    try:
        node3.query("DROP TABLE IF EXISTS t_mutations")

        node3.query("CREATE TABLE t_mutations (a UInt32) ENGINE = MergeTree ORDER BY a")
        node3.query("INSERT INTO t_mutations SELECT number FROM numbers(10000000)")

        node3.query(
            "ALTER TABLE t_mutations DELETE WHERE a = 300000",
            settings={"mutations_sync": "2"},
        )

        assert node3.query("SELECT count() FROM t_mutations") == "9999999\n"
    finally:
        node3.query("DROP TABLE IF EXISTS t_mutations")

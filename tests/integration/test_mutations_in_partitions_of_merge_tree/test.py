import time

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
        node1.query(f"DROP TABLE IF EXISTS {name}")
        node1.query(
            f"CREATE TABLE {name} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p"
        )
        node1.query(f"INSERT INTO {name} VALUES (1, 2), (2, 3)")
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                f"ALTER TABLE {name} UPDATE x = x + 1 IN PARTITION 1 SETTINGS mutations_sync = 2"
            )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                f"ALTER TABLE {name} UPDATE x = x + 1 IN PARTITION 2 SETTINGS mutations_sync = 2"
            )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                f"ALTER TABLE {name} DELETE IN PARTITION 1 SETTINGS mutations_sync = 2"
            )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["5"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                f"ALTER TABLE {name} DELETE IN PARTITION 2 SETTINGS mutations_sync = 2"
            )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["5"]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name}")


def test_trivial_alter_in_partition_merge_tree_with_where(started_cluster):
    try:
        name = "test_trivial_alter_in_partition_merge_tree_with_where"
        node1.query(f"DROP TABLE IF EXISTS {name}")
        node1.query(
            f"CREATE TABLE {name} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p"
        )
        node1.query(f"INSERT INTO {name} VALUES (1, 2), (2, 3)")
        node1.query(
            f"ALTER TABLE {name} UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        assert node1.query(f"SELECT x FROM {name} ORDER BY p").splitlines() == [
            "2",
            "4",
        ]
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["6"]
        node1.query(
            f"ALTER TABLE {name} UPDATE x = x + 1 IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["6"]
        node1.query(
            f"ALTER TABLE {name} DELETE IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["2"]
        node1.query(
            f"ALTER TABLE {name} DELETE IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == ["2"]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name}")


def test_trivial_alter_in_partition_replicated_merge_tree(started_cluster):
    try:
        name = "test_trivial_alter_in_partition_replicated_merge_tree"

        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {name} SYNC")

        for node in (node1, node2):
            node.query(
                f"CREATE TABLE {name} (p Int64, x Int64) ENGINE=ReplicatedMergeTree('/clickhouse/{name}', '{{instance}}') ORDER BY tuple() PARTITION BY p"
            )

        node1.query(f"INSERT INTO {name} VALUES (1, 2)")
        node2.query(f"INSERT INTO {name} VALUES (2, 3)")

        node1.query(
            f"ALTER TABLE {name} UPDATE x = x + 1 IN PARTITION 2 WHERE 1 SETTINGS mutations_sync = 2"
        )
        for node in (node1, node2):
            assert node.query(f"SELECT sum(x) FROM {name}").splitlines() == ["6"]
        node1.query(
            f"ALTER TABLE {name} UPDATE x = x + 1 IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        for node in (node1, node2):
            assert node.query(f"SELECT sum(x) FROM {name}").splitlines() == ["6"]
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                f"ALTER TABLE {name} DELETE IN PARTITION 2 SETTINGS mutations_sync = 2"
            )
        for node in (node1, node2):
            assert node.query(f"SELECT sum(x) FROM {name}").splitlines() == ["6"]
        node1.query(
            f"ALTER TABLE {name} DELETE IN PARTITION 2 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        for node in (node1, node2):
            assert node.query(f"SELECT sum(x) FROM {name}").splitlines() == ["2"]
        node1.query(
            f"ALTER TABLE {name} DELETE IN PARTITION 1 WHERE p = 2 SETTINGS mutations_sync = 2"
        )
        for node in (node1, node2):
            assert node.query(f"SELECT sum(x) FROM {name}").splitlines() == ["2"]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_alter_in_partition_merge_tree_invalid_valid_valid(started_cluster):
    try:
        name = "test_alter_in_partition_merge_tree_invalid_valid_valid"
        node1.query(f"DROP TABLE IF EXISTS {name}")
        node1.query(
            f"CREATE TABLE {name} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p"
        )
        node1.query(f"INSERT INTO {name} VALUES (1, 2), (2, 3)")
        node1.query(f"ALTER TABLE {name} UPDATE x = x / (x - x) IN PARTITION 1 WHERE 1")
        node1.query(f"ALTER TABLE {name} UPDATE x = x + 1 WHERE 1")
        node1.query(
            f"ALTER TABLE {name} UPDATE x = x * 2 IN PARTITION 2 WHERE 1 SETTINGS mutations_sync = 2"
        )
        assert node1.query(f"SELECT x FROM {name} ORDER BY p").splitlines() == [
            "2",
            "8",
        ]
        # assert node1.query(f"SELECT * from system.mutations WHERE table = '{name}'") == [""]

        node1.query(
            f"KILL MUTATION WHERE table = '{name}' AND mutation_id = 'mutation_3.txt'"
        )
        time.sleep(5)
        assert node1.query(f"SELECT x FROM {name} ORDER BY p").splitlines() == [
            "3",
            "8",
        ]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name}")


def test_alter_in_partition_merge_tree_updates_with_errors(started_cluster):
    try:
        name = "test_alter_in_partition_merge_tree"
        node1.query(f"DROP TABLE IF EXISTS {name}")
        node1.query(
            f"CREATE TABLE {name} (p Int64, x Int64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY p"
        )

        data = []
        errors = set()

        for p in range(50):
            node1.query(
                f"INSERT INTO {name} VALUES "
                + ", ".join((f"({p}, {i})" for i in range(p + 1)))
            )
            data.append(list(range(p + 1)))

        for p in range(50):
            if p % 13 == 12:
                node1.query(
                    f"ALTER TABLE {name} UPDATE x = x / (x - x) IN PARTITION {p} WHERE 1"
                )
                errors.add(p)

            if p % 11 == 10:
                node1.query(f"ALTER TABLE {name} UPDATE x = x + {p % 2} + 1 WHERE 1")
                data = [
                    [x + p % 2 + 1 for x in y] if i not in errors else y
                    for i, y in enumerate(data)
                ]

            elif p % 23 == 22:
                node1.restart_clickhouse(kill=True)

            else:
                node1.query(
                    f"ALTER TABLE {name} UPDATE x = x + {p % 2} IN PARTITION {p} WHERE 1"
                )
                if p not in errors:
                    data[p] = [x + p % 2 for x in data[p]]

        for p in range(0, 100):
            node1.query(f"INSERT INTO {name} VALUES ({p}, 1)")

        data.append([100])

        node1.query(
            f"ALTER TABLE {name} UPDATE x = x IN PARTITION -1 WHERE 1 SETTINGS mutations_sync = 2"
        )

        print(node1.query(f"SELECT * FROM system.mutations"))

        assert node1.query(f"SELECT sum(x) FROM {name}").splitlines() == [
            str(sum((y for x in data for y in x)))
        ]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name}")


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

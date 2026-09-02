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
        node1.query(
            f"KILL MUTATION WHERE table = '{name}' AND mutation_id = 'mutation_3.txt'"
        )
        node1.query_with_retry(
            f"SELECT x FROM {name} WHERE p = 1",
            check_callback=lambda res: int(res) == 3,
        )

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

        node1.query_with_retry(
            "SELECT count() FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{name}' "
            "AND is_done = 0 AND latest_fail_reason = ''",
            check_callback=lambda res: int(res) == 0,
        )

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


def test_legacy_mutation_file_partition_scope_upgrade(started_cluster):
    """
    A legacy `mutation_*.txt` file (written before the partition scope of the commands was
    pinned to `IN PARTITION ID`) is rewritten when it is loaded. Without that rewrite, every
    load resolves the `IN PARTITION` literal through the current partition key again, so a
    key-safe partition key type change (e.g. `Enum8 -> Int8`) made after the load would still
    leave the table unloadable on the next restart. The legacy shape is fabricated by turning
    the pinned `IN PARTITION ID '1'` of a freshly written mutation file back into the original
    `IN PARTITION 'a'` literal. Note that the file keeps the shape it always had, so a
    rewritten file is still readable by a binary without this feature.

    This has to be an integration test rather than a stateless one: it edits a real local
    `mutation_*.txt` file of a plain `MergeTree` table, and the stateless suite runs against
    arbitrary server configurations where that file either does not exist or does not mean
    what the test assumes.
    """
    name = "test_legacy_mutation_file"

    def count_pinned_scopes(path):
        return int(
            node1.exec_in_container(
                ["bash", "-c", f"grep -c -F \"IN PARTITION ID \" {path} || true"]
            ).strip()
        )

    try:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node1.query(
            f"CREATE TABLE {name} (p Enum8('a' = 1, 'b' = 2), n Int64) "
            "ENGINE = MergeTree PARTITION BY p ORDER BY tuple()"
        )
        node1.query(f"INSERT INTO {name} VALUES ('a', 1), ('b', 2)")

        # The mutation file of a finished mutation is kept on disk and is read back by
        # `loadMutations` on every ATTACH, exactly like the file of a pending one, so the
        # mutation does not have to be kept pending for the loading path under test.
        node1.query(
            f"ALTER TABLE {name} UPDATE n = n + 100 IN PARTITION 'a' WHERE 1",
            settings={"mutations_sync": "2"},
        )

        data_dir = node1.query(
            "SELECT data_paths[1] FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{name}'"
        ).strip()
        mutation_file = node1.exec_in_container(
            ["bash", "-c", f"ls {data_dir}mutation_*.txt"]
        ).strip()

        node1.query(f"DETACH TABLE {name}")

        # Fabricate the legacy format: turn the pinned partition id back into the original
        # literal. The command text of the file quotes the literals, so the partition id
        # appears as `ID \'1\'`.
        sed_program = r"s|IN PARTITION ID \\'1\\'|IN PARTITION \\'a\\'|"
        node1.exec_in_container(["sed", "-i", sed_program, mutation_file])
        assert count_pinned_scopes(mutation_file) == 0

        # Loading the legacy file resolves the scope through the (unchanged) partition key
        # and rewrites the file with the scope pinned.
        node1.query(f"ATTACH TABLE {name}")
        assert count_pinned_scopes(mutation_file) == 1

        # A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the
        # numeric on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
        node1.query(
            f"ALTER TABLE {name} MODIFY COLUMN p Int8", settings={"alter_sync": "2"}
        )

        # Without the upgrade this reattach would fail to load the table: the legacy file
        # would come through the fallback again and re-parse the stale literal against the
        # new key type.
        node1.query(f"DETACH TABLE {name}")
        node1.query(f"ATTACH TABLE {name}")

        assert node1.query(f"SELECT p, n FROM {name} ORDER BY p, n") == "1\t101\n2\t2\n"
        assert (
            node1.query(
                "SELECT count() FROM system.mutations "
                f"WHERE database = currentDatabase() AND table = '{name}' AND NOT is_done"
            )
            == "0\n"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")

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


@pytest.mark.parametrize("with_obsolete_patches", [False, True])
def test_legacy_finished_mutation_file_key_change(started_cluster, with_obsolete_patches):
    """
    Upgrade regression: an older server version applied an `IN PARTITION <value>` mutation of
    `MergeTree` to all parts and kept the original literal in `mutation_*.txt`
    (`finished_mutations_to_keep`). A key-safe partition key type change (`Enum8 -> Int8`) after
    that mutation has finished, but before the first load with the current version, makes the
    literal undecodable. The finished entry must not prevent the table from loading.

    With `with_obsolete_patches`, a lightweight update leaves patch parts with a lower data version
    that are already materialized into all regular parts and are retained only because of
    `remove_unused_patch_parts = 0`. They must not make the finished mutation look pending.
    """
    name = "test_legacy_finished_mutation_file_key_change"
    table_settings = ""
    if with_obsolete_patches:
        table_settings = (
            " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,"
            " remove_unused_patch_parts = 0"
        )

    try:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node1.query(
            f"CREATE TABLE {name} (p Enum8('a' = 1, 'b' = 2), n Int64) "
            "ENGINE = MergeTree PARTITION BY p ORDER BY tuple()" + table_settings
        )
        node1.query(f"INSERT INTO {name} VALUES ('a', 1), ('b', 2)")
        if with_obsolete_patches:
            node1.query(
                f"UPDATE {name} SET n = n + 10 WHERE 1",
                settings={"allow_experimental_lightweight_update": "1"},
            )
        base = 10 if with_obsolete_patches else 0

        node1.query(
            f"ALTER TABLE {name} UPDATE n = n + 100 IN PARTITION 'a' WHERE 1",
            settings={"mutations_sync": "2"},
        )
        # An older server version would have mutated the parts of all partitions; a global
        # mutation does the same here.
        node1.query(
            f"ALTER TABLE {name} UPDATE n = n WHERE 1", settings={"mutations_sync": "2"}
        )
        node1.query(
            f"ALTER TABLE {name} MODIFY COLUMN p Int8", settings={"alter_sync": "2"}
        )

        data_dir = node1.query(
            "SELECT data_paths[1] FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{name}'"
        ).strip()
        first_mutation = node1.query(
            "SELECT mutation_id FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{name}' "
            "ORDER BY length(mutation_id), mutation_id LIMIT 1"
        ).strip()
        mutation_file = data_dir + first_mutation

        node1.query(f"DETACH TABLE {name}")

        # Fabricate the legacy format: put the original literal back instead of the pinned
        # partition id.
        sed_program = r"s|IN PARTITION ID \\'1\\'|IN PARTITION \\'a\\'|"
        node1.exec_in_container(["sed", "-i", sed_program, mutation_file])
        assert (
            node1.exec_in_container(
                ["bash", "-c", f"grep -c -F \"IN PARTITION \\\\'a\\\\'\" {mutation_file} || true"]
            ).strip()
            == "1"
        )

        node1.query(f"ATTACH TABLE {name}")

        assert node1.query(
            "SELECT command, is_done FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{name}' ORDER BY mutation_id"
        ) == (
            "(UPDATE n = n + 100 IN PARTITION \\'a\\' WHERE 1)\t1\n"
            "(UPDATE n = n WHERE 1)\t1\n"
        )
        assert (
            node1.query(f"SELECT p, n FROM {name} ORDER BY p")
            == f"1\t{101 + base}\n2\t{2 + base}\n"
        )

        # The table stays usable: new mutations of the converted partition key work.
        node1.query(
            f"ALTER TABLE {name} UPDATE n = n + 1000 IN PARTITION 2 WHERE 1",
            settings={"mutations_sync": "2"},
        )
        assert (
            node1.query(f"SELECT p, n FROM {name} ORDER BY p")
            == f"1\t{101 + base}\n2\t{1002 + base}\n"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_legacy_pending_mutation_file_key_change(started_cluster):
    """
    Upgrade regression: an older server version kept the original `IN PARTITION <value>` literal
    in `mutation_*.txt`, and a key-safe partition key type change (`Enum8 -> Int8`) was made
    while the mutation was still pending, before the first load with the current version. The
    literal is undecodable now, but the table must still load, and the mutation must stay
    visible and cancellable, as it was before the partition scope was resolved at load.
    """
    name = "test_legacy_pending_mutation_file_key_change"

    try:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node1.query(
            f"CREATE TABLE {name} (p Enum8('a' = 1, 'b' = 2), n Int64) "
            "ENGINE = MergeTree PARTITION BY p ORDER BY tuple()"
        )
        node1.query(f"INSERT INTO {name} VALUES ('a', 1), ('b', 2)")
        node1.query(f"SYSTEM STOP MERGES {name}")
        node1.query(f"ALTER TABLE {name} UPDATE n = n + 100 IN PARTITION 'a' WHERE 1")
        node1.query(
            f"ALTER TABLE {name} MODIFY COLUMN p Int8", settings={"alter_sync": "2"}
        )

        data_dir = node1.query(
            "SELECT data_paths[1] FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{name}'"
        ).strip()
        mutation_id = node1.query(
            "SELECT mutation_id FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{name}' AND NOT is_done"
        ).strip()
        mutation_file = data_dir + mutation_id

        node1.query(f"DETACH TABLE {name}")

        # Fabricate the legacy format: put the original literal back instead of the pinned
        # partition id.
        sed_program = r"s|IN PARTITION ID \\'1\\'|IN PARTITION \\'a\\'|"
        node1.exec_in_container(["sed", "-i", sed_program, mutation_file])
        assert (
            node1.exec_in_container(
                ["bash", "-c", f"grep -c -F \"IN PARTITION \\\\'a\\\\'\" {mutation_file} || true"]
            ).strip()
            == "1"
        )

        node1.query(f"ATTACH TABLE {name}")

        assert node1.query(
            "SELECT command, is_done FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{name}'"
        ) == "(UPDATE n = n + 100 IN PARTITION \\'a\\' WHERE 1)\t0\n"

        node1.query(
            f"KILL MUTATION WHERE database = currentDatabase() AND table = '{name}' "
            f"AND mutation_id = '{mutation_id}'"
        )
        assert (
            node1.query(
                "SELECT count() FROM system.mutations "
                f"WHERE database = currentDatabase() AND table = '{name}'"
            )
            == "0\n"
        )
        assert node1.query(f"SELECT p, n FROM {name} ORDER BY p") == "1\t1\n2\t2\n"

        # The table stays usable: new mutations of the converted partition key work.
        node1.query(
            f"ALTER TABLE {name} UPDATE n = n + 1000 IN PARTITION 2 WHERE 1",
            settings={"mutations_sync": "2"},
        )
        assert node1.query(f"SELECT p, n FROM {name} ORDER BY p") == "1\t1\n2\t1002\n"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")

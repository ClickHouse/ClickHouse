import os
import shutil
import time
import re
import pytest
import threading

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain
from helpers.network import PartitionManager

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
dummy_node = cluster.add_instance(
    "dummy_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
competing_node = cluster.add_instance(
    "competing_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 3},
)
snapshotting_node = cluster.add_instance(
    "snapshotting_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    macros={"shard": 2, "replica": 1},
)
snapshot_recovering_node = cluster.add_instance(
    "snapshot_recovering_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
)

all_nodes = [
    main_node,
    dummy_node,
    competing_node,
    snapshotting_node,
    snapshot_recovering_node,
]

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def assert_create_query(nodes, table_name, expected):
    replace_uuid = lambda x: re.sub(uuid_regex, "uuid", x)
    query = "show create table {}".format(table_name)
    for node in nodes:
        assert_eq_with_retry(node, query, expected, get_result=replace_uuid)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_create_replicated_table(started_cluster):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica' || '1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )
    assert (
        "Explicit zookeeper_path and replica_name are specified"
        in main_node.query_and_get_error(
            "CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) "
            "ENGINE=ReplicatedMergeTree('/test/tmp', 'r') ORDER BY k PARTITION BY toYYYYMM(d);"
        )
    )

    assert (
        "Explicit zookeeper_path and replica_name are specified"
        in main_node.query_and_get_error(
            "CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) "
            "ENGINE=ReplicatedMergeTree('/test/tmp', 'r') ORDER BY k PARTITION BY toYYYYMM(d);"
        )
    )

    assert (
        "This syntax for *MergeTree engine is deprecated"
        in main_node.query_and_get_error(
            "CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) "
            "ENGINE=ReplicatedMergeTree('/test/tmp/{shard}', '{replica}', d, k, 8192);"
        )
    )

    main_node.query(
        "CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
    )

    expected = (
        "CREATE TABLE testdb.replicated_table\\n(\\n    `d` Date,\\n    `k` UInt64,\\n    `i32` Int32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\n"
        "PARTITION BY toYYYYMM(d)\\nORDER BY k\\nSETTINGS index_granularity = 8192"
    )
    assert_create_query([main_node, dummy_node], "testdb.replicated_table", expected)
    # assert without replacing uuid
    assert main_node.query("show create testdb.replicated_table") == dummy_node.query(
        "show create testdb.replicated_table"
    )
    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_simple_alter_table(started_cluster, engine):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )
    # test_simple_alter_table
    name = "testdb.alter_test_{}".format(engine)
    main_node.query(
        "CREATE TABLE {} "
        "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
        "ENGINE = {} PARTITION BY StartDate ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);".format(
            name, engine
        )
    )
    main_node.query("ALTER TABLE {} ADD COLUMN Added0 UInt32;".format(name))
    main_node.query("ALTER TABLE {} ADD COLUMN Added2 UInt32;".format(name))
    main_node.query(
        "ALTER TABLE {} ADD COLUMN Added1 UInt32 AFTER Added0;".format(name)
    )
    main_node.query(
        "ALTER TABLE {} ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;".format(
            name
        )
    )
    main_node.query(
        "ALTER TABLE {} ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;".format(
            name
        )
    )
    main_node.query(
        "ALTER TABLE {} ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;".format(
            name
        )
    )

    full_engine = (
        engine
        if not "Replicated" in engine
        else engine + "(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')"
    )
    expected = (
        "CREATE TABLE {}\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n"
        "    `ToDrop` UInt32,\\n    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n"
        "    `AddedNested1.A` Array(UInt32),\\n    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n"
        "    `AddedNested2.A` Array(UInt32),\\n    `AddedNested2.B` Array(UInt64)\\n)\\n"
        "ENGINE = {}\\nPARTITION BY StartDate\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\n"
        "SETTINGS index_granularity = 8192".format(name, full_engine)
    )

    assert_create_query([main_node, dummy_node], name, expected)

    # test_create_replica_after_delay
    competing_node.query(
        "CREATE DATABASE IF NOT EXISTS testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica3');"
    )

    name = "testdb.alter_test_{}".format(engine)
    main_node.query("ALTER TABLE {} ADD COLUMN Added3 UInt32;".format(name))
    main_node.query("ALTER TABLE {} DROP COLUMN AddedNested1;".format(name))
    main_node.query("ALTER TABLE {} RENAME COLUMN Added1 TO AddedNested1;".format(name))

    full_engine = (
        engine
        if not "Replicated" in engine
        else engine + "(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')"
    )
    expected = (
        "CREATE TABLE {}\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n"
        "    `ToDrop` UInt32,\\n    `Added0` UInt32,\\n    `AddedNested1` UInt32,\\n    `Added2` UInt32,\\n"
        "    `AddedNested2.A` Array(UInt32),\\n    `AddedNested2.B` Array(UInt64),\\n    `Added3` UInt32\\n)\\n"
        "ENGINE = {}\\nPARTITION BY StartDate\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\n"
        "SETTINGS index_granularity = 8192".format(name, full_engine)
    )

    assert_create_query([main_node, dummy_node, competing_node], name, expected)
    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")
    competing_node.query("DROP DATABASE testdb SYNC")


def get_table_uuid(database, name):
    return main_node.query(
        f"SELECT uuid FROM system.tables WHERE database = '{database}' and name = '{name}'"
    ).strip()


@pytest.fixture(scope="module", name="attachable_part")
def fixture_attachable_part(started_cluster):
    main_node.query(f"CREATE DATABASE testdb_attach_atomic ENGINE = Atomic")
    main_node.query(
        f"CREATE TABLE testdb_attach_atomic.test (CounterID UInt32) ENGINE = MergeTree ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO testdb_attach_atomic.test VALUES (123)")
    main_node.query(
        f"ALTER TABLE testdb_attach_atomic.test FREEZE WITH NAME 'test_attach'"
    )
    table_uuid = get_table_uuid("testdb_attach_atomic", "test")
    return os.path.join(
        main_node.path,
        f"database/shadow/test_attach/store/{table_uuid[:3]}/{table_uuid}/all_1_1_0",
    )


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_attach(started_cluster, attachable_part, engine):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    name = "alter_attach_test_{}".format(engine)
    main_node.query(
        f"CREATE TABLE testdb.{name} (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    table_uuid = get_table_uuid("testdb", name)
    # Provide and attach a part to the main node
    shutil.copytree(
        attachable_part,
        os.path.join(
            main_node.path,
            f"database/store/{table_uuid[:3]}/{table_uuid}/detached/all_1_1_0",
        ),
    )
    main_node.query(f"ALTER TABLE testdb.{name} ATTACH PART 'all_1_1_0'")
    # On the main node, data is attached
    assert main_node.query(f"SELECT CounterID FROM testdb.{name}") == "123\n"
    # On the other node, data is replicated only if using a Replicated table engine
    if engine == "ReplicatedMergeTree":
        assert dummy_node.query(f"SELECT CounterID FROM testdb.{name}") == "123\n"
    else:
        assert dummy_node.query(f"SELECT CounterID FROM testdb.{name}") == ""
    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_drop_part(started_cluster, engine):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    table = f"alter_drop_{engine}"
    part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
    main_node.query(
        f"CREATE TABLE testdb.{table} (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO testdb.{table} VALUES (123)")
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO testdb.{table} VALUES (456)")
    main_node.query(f"ALTER TABLE testdb.{table} DROP PART '{part_name}'")
    assert main_node.query(f"SELECT CounterID FROM testdb.{table}") == ""
    if engine == "ReplicatedMergeTree":
        # The DROP operation is still replicated at the table engine level
        assert dummy_node.query(f"SELECT CounterID FROM testdb.{table}") == ""
    else:
        assert dummy_node.query(f"SELECT CounterID FROM testdb.{table}") == "456\n"
    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_detach_part(started_cluster, engine):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    table = f"alter_detach_{engine}"
    part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
    main_node.query(
        f"CREATE TABLE testdb.{table} (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO testdb.{table} VALUES (123)")
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO testdb.{table} VALUES (456)")
    main_node.query(f"ALTER TABLE testdb.{table} DETACH PART '{part_name}'")
    detached_parts_query = f"SELECT name FROM system.detached_parts WHERE database='testdb' AND table='{table}'"
    assert main_node.query(detached_parts_query) == f"{part_name}\n"
    if engine == "ReplicatedMergeTree":
        # The detach operation is still replicated at the table engine level
        assert dummy_node.query(detached_parts_query) == f"{part_name}\n"
    else:
        assert dummy_node.query(detached_parts_query) == ""
    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_drop_detached_part(started_cluster, engine):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    table = f"alter_drop_detached_{engine}"
    part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
    main_node.query(
        f"CREATE TABLE testdb.{table} (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO testdb.{table} VALUES (123)")
    main_node.query(f"ALTER TABLE testdb.{table} DETACH PART '{part_name}'")
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO testdb.{table} VALUES (456)")
        dummy_node.query(f"ALTER TABLE testdb.{table} DETACH PART '{part_name}'")
    main_node.query(f"ALTER TABLE testdb.{table} DROP DETACHED PART '{part_name}'")
    detached_parts_query = f"SELECT name FROM system.detached_parts WHERE database='testdb' AND table='{table}'"
    assert main_node.query(detached_parts_query) == ""
    assert dummy_node.query(detached_parts_query) == f"{part_name}\n"

    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")


def test_alter_fetch(started_cluster):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    main_node.query(
        "CREATE TABLE testdb.fetch_source (CounterID UInt32) ENGINE = ReplicatedMergeTree ORDER BY (CounterID)"
    )
    main_node.query(
        "CREATE TABLE testdb.fetch_target (CounterID UInt32) ENGINE = ReplicatedMergeTree ORDER BY (CounterID)"
    )
    main_node.query("INSERT INTO testdb.fetch_source VALUES (123)")
    table_uuid = get_table_uuid("testdb", "fetch_source")
    main_node.query(
        f"ALTER TABLE testdb.fetch_target FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/{table_uuid}/{{shard}}' "
    )
    detached_parts_query = "SELECT name FROM system.detached_parts WHERE database='testdb' AND table='fetch_target'"
    assert main_node.query(detached_parts_query) == "all_0_0_0\n"
    assert dummy_node.query(detached_parts_query) == ""

    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")


def test_alters_from_different_replicas(started_cluster):
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    # test_alters_from_different_replicas
    competing_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica3');"
    )

    main_node.query(
        "CREATE TABLE testdb.concurrent_test "
        "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
        "ENGINE = MergeTree PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);"
    )

    main_node.query(
        "CREATE TABLE testdb.dist AS testdb.concurrent_test ENGINE = Distributed(testdb, testdb, concurrent_test, CounterID)"
    )

    dummy_node.stop_clickhouse(kill=True)

    settings = {"distributed_ddl_task_timeout": 5}
    assert (
        "There are 1 unfinished hosts (0 of them are currently active)"
        in competing_node.query_and_get_error(
            "ALTER TABLE testdb.concurrent_test ADD COLUMN Added0 UInt32;",
            settings=settings,
        )
    )
    settings = {
        "distributed_ddl_task_timeout": 5,
        "distributed_ddl_output_mode": "null_status_on_timeout",
    }
    assert "shard1\treplica2\tQUEUED\t" in main_node.query(
        "ALTER TABLE testdb.concurrent_test ADD COLUMN Added2 UInt32;",
        settings=settings,
    )
    settings = {
        "distributed_ddl_task_timeout": 5,
        "distributed_ddl_output_mode": "never_throw",
    }
    assert "shard1\treplica2\tQUEUED\t" in competing_node.query(
        "ALTER TABLE testdb.concurrent_test ADD COLUMN Added1 UInt32 AFTER Added0;",
        settings=settings,
    )
    dummy_node.start_clickhouse()
    main_node.query(
        "ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;"
    )
    competing_node.query(
        "ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;"
    )
    main_node.query(
        "ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;"
    )

    expected = (
        "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32,\\n"
        "    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n    `AddedNested1.A` Array(UInt32),\\n"
        "    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n    `AddedNested2.A` Array(UInt32),\\n"
        "    `AddedNested2.B` Array(UInt64)\\n)\\n"
        "ENGINE = MergeTree\\nPARTITION BY toYYYYMM(StartDate)\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\nSETTINGS index_granularity = 8192"
    )

    assert_create_query([main_node, competing_node], "testdb.concurrent_test", expected)

    # test_create_replica_after_delay
    main_node.query("DROP TABLE testdb.concurrent_test SYNC")
    main_node.query(
        "CREATE TABLE testdb.concurrent_test "
        "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
        "ENGINE = ReplicatedMergeTree ORDER BY CounterID;"
    )

    expected = (
        "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
    )

    assert_create_query([main_node, competing_node], "testdb.concurrent_test", expected)

    main_node.query(
        "INSERT INTO testdb.dist (CounterID, StartDate, UserID) SELECT number, addDays(toDate('2020-02-02'), number), intHash32(number) FROM numbers(10)"
    )

    # test_replica_restart
    main_node.restart_clickhouse()

    expected = (
        "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
    )

    # test_snapshot_and_snapshot_recover
    snapshotting_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard2', 'replica1');"
    )
    snapshot_recovering_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard2', 'replica2');"
    )
    assert_create_query(all_nodes, "testdb.concurrent_test", expected)

    main_node.query("SYSTEM FLUSH DISTRIBUTED testdb.dist")
    main_node.query(
        "ALTER TABLE testdb.concurrent_test UPDATE StartDate = addYears(StartDate, 1) WHERE 1"
    )
    res = main_node.query("ALTER TABLE testdb.concurrent_test DELETE WHERE UserID % 2")
    assert (
        "shard1\treplica1\tOK" in res
        and "shard1\treplica2\tOK" in res
        and "shard1\treplica3\tOK" in res
    )
    assert "shard2\treplica1\tOK" in res and "shard2\treplica2\tOK" in res

    expected = (
        "1\t1\tmain_node\n"
        "1\t2\tdummy_node\n"
        "1\t3\tcompeting_node\n"
        "2\t1\tsnapshotting_node\n"
        "2\t2\tsnapshot_recovering_node\n"
    )
    assert (
        main_node.query(
            "SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster='testdb'"
        )
        == expected
    )

    # test_drop_and_create_replica
    main_node.query("DROP DATABASE testdb SYNC")
    main_node.query(
        "CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )

    expected = (
        "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
    )

    assert_create_query([main_node, competing_node], "testdb.concurrent_test", expected)
    assert_create_query(all_nodes, "testdb.concurrent_test", expected)

    for node in all_nodes:
        node.query("SYSTEM SYNC REPLICA testdb.concurrent_test")

    expected = (
        "0\t2021-02-02\t4249604106\n"
        "1\t2021-02-03\t1343103100\n"
        "4\t2021-02-06\t3902320246\n"
        "7\t2021-02-09\t3844986530\n"
        "9\t2021-02-11\t1241149650\n"
    )

    assert_eq_with_retry(
        dummy_node,
        "SELECT CounterID, StartDate, UserID FROM testdb.dist ORDER BY CounterID",
        expected,
    )
    main_node.query("DROP DATABASE testdb SYNC")
    dummy_node.query("DROP DATABASE testdb SYNC")
    competing_node.query("DROP DATABASE testdb SYNC")
    snapshotting_node.query("DROP DATABASE testdb SYNC")
    snapshot_recovering_node.query("DROP DATABASE testdb SYNC")


def create_some_tables(db):
    settings = {"distributed_ddl_task_timeout": 0}
    main_node.query(
        "CREATE TABLE {}.t1 (n int) ENGINE=Memory".format(db), settings=settings
    )
    dummy_node.query(
        "CREATE TABLE {}.t2 (s String) ENGINE=Memory".format(db), settings=settings
    )
    main_node.query(
        "CREATE TABLE {}.mt1 (n int) ENGINE=MergeTree order by n".format(db),
        settings=settings,
    )
    dummy_node.query(
        "CREATE TABLE {}.mt2 (n int) ENGINE=MergeTree order by n".format(db),
        settings=settings,
    )
    main_node.query(
        "CREATE TABLE {}.rmt1 (n int) ENGINE=ReplicatedMergeTree order by n".format(db),
        settings=settings,
    )
    dummy_node.query(
        "CREATE TABLE {}.rmt2 (n int) ENGINE=ReplicatedMergeTree order by n".format(db),
        settings=settings,
    )
    main_node.query(
        "CREATE TABLE {}.rmt3 (n int) ENGINE=ReplicatedMergeTree order by n".format(db),
        settings=settings,
    )
    dummy_node.query(
        "CREATE TABLE {}.rmt5 (n int) ENGINE=ReplicatedMergeTree order by n".format(db),
        settings=settings,
    )
    main_node.query(
        "CREATE MATERIALIZED VIEW {}.mv1 (n int) ENGINE=ReplicatedMergeTree order by n AS SELECT n FROM recover.rmt1".format(
            db
        ),
        settings=settings,
    )
    dummy_node.query(
        "CREATE MATERIALIZED VIEW {}.mv2 (n int) ENGINE=ReplicatedMergeTree order by n  AS SELECT n FROM recover.rmt2".format(
            db
        ),
        settings=settings,
    )
    main_node.query(
        "CREATE DICTIONARY {}.d1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())".format(db)
    )
    dummy_node.query(
        "CREATE DICTIONARY {}.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt2' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())".format(db)
    )


def test_recover_staled_replica(started_cluster):
    main_node.query(
        "CREATE DATABASE recover ENGINE = Replicated('/clickhouse/databases/recover', 'shard1', 'replica1');"
    )
    started_cluster.get_kazoo_client("zoo1").set(
        "/clickhouse/databases/recover/logs_to_keep", b"10"
    )
    dummy_node.query(
        "CREATE DATABASE recover ENGINE = Replicated('/clickhouse/databases/recover', 'shard1', 'replica2');"
    )

    settings = {"distributed_ddl_task_timeout": 0}
    create_some_tables("recover")

    for table in ["t1", "t2", "mt1", "mt2", "rmt1", "rmt2", "rmt3", "rmt5"]:
        main_node.query("INSERT INTO recover.{} VALUES (42)".format(table))
    for table in ["t1", "t2", "mt1", "mt2"]:
        dummy_node.query("INSERT INTO recover.{} VALUES (42)".format(table))
    for table in ["rmt1", "rmt2", "rmt3", "rmt5"]:
        main_node.query("SYSTEM SYNC REPLICA recover.{}".format(table))

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(dummy_node)
        dummy_node.query_and_get_error("RENAME TABLE recover.t1 TO recover.m1")

        main_node.query_with_retry(
            "RENAME TABLE recover.t1 TO recover.m1", settings=settings
        )
        main_node.query_with_retry(
            "ALTER TABLE recover.mt1  ADD COLUMN m int", settings=settings
        )
        main_node.query_with_retry(
            "ALTER TABLE recover.rmt1 ADD COLUMN m int", settings=settings
        )
        main_node.query_with_retry(
            "RENAME TABLE recover.rmt3 TO recover.rmt4", settings=settings
        )
        main_node.query_with_retry("DROP TABLE recover.rmt5", settings=settings)
        main_node.query_with_retry("DROP DICTIONARY recover.d2", settings=settings)
        main_node.query_with_retry(
            "CREATE DICTIONARY recover.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
            "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());",
            settings=settings,
        )

        inner_table = (
            ".inner_id."
            + dummy_node.query_with_retry(
                "SELECT uuid FROM system.tables WHERE database='recover' AND name='mv1'"
            ).strip()
        )
        main_node.query_with_retry(
            "ALTER TABLE recover.`{}` MODIFY COLUMN n int DEFAULT 42".format(
                inner_table
            ),
            settings=settings,
        )
        main_node.query_with_retry(
            "ALTER TABLE recover.mv1 MODIFY QUERY SELECT m FROM recover.rmt1".format(
                inner_table
            ),
            settings=settings,
        )
        main_node.query_with_retry(
            "RENAME TABLE recover.mv2 TO recover.mv3".format(inner_table),
            settings=settings,
        )

        main_node.query_with_retry(
            "CREATE TABLE recover.tmp AS recover.m1", settings=settings
        )
        main_node.query_with_retry("DROP TABLE recover.tmp", settings=settings)
        main_node.query_with_retry(
            "CREATE TABLE recover.tmp AS recover.m1", settings=settings
        )
        main_node.query_with_retry("DROP TABLE recover.tmp", settings=settings)
        main_node.query_with_retry(
            "CREATE TABLE recover.tmp AS recover.m1", settings=settings
        )

    assert (
        main_node.query(
            "SELECT name FROM system.tables WHERE database='recover' AND name NOT LIKE '.inner_id.%' ORDER BY name"
        )
        == "d1\nd2\nm1\nmt1\nmt2\nmv1\nmv3\nrmt1\nrmt2\nrmt4\nt2\ntmp\n"
    )
    query = (
        "SELECT name, uuid, create_table_query FROM system.tables WHERE database='recover' AND name NOT LIKE '.inner_id.%' "
        "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
    )
    expected = main_node.query(query)
    assert_eq_with_retry(dummy_node, query, expected)
    assert (
        main_node.query(
            "SELECT count() FROM system.tables WHERE database='recover' AND name LIKE '.inner_id.%'"
        )
        == "2\n"
    )
    assert (
        dummy_node.query(
            "SELECT count() FROM system.tables WHERE database='recover' AND name LIKE '.inner_id.%'"
        )
        == "2\n"
    )

    for table in [
        "m1",
        "t2",
        "mt1",
        "mt2",
        "rmt1",
        "rmt2",
        "rmt4",
        "d1",
        "d2",
        "mv1",
        "mv3",
    ]:
        assert main_node.query("SELECT (*,).1 FROM recover.{}".format(table)) == "42\n"
    for table in ["t2", "rmt1", "rmt2", "rmt4", "d1", "d2", "mt2", "mv1", "mv3"]:
        assert dummy_node.query("SELECT (*,).1 FROM recover.{}".format(table)) == "42\n"
    for table in ["m1", "mt1"]:
        assert dummy_node.query("SELECT count() FROM recover.{}".format(table)) == "0\n"
    global test_recover_staled_replica_run
    assert (
        dummy_node.query(
            "SELECT count() FROM system.tables WHERE database='recover_broken_tables'"
        )
        == f"{test_recover_staled_replica_run}\n"
    )
    assert (
        dummy_node.query(
            "SELECT count() FROM system.tables WHERE database='recover_broken_replicated_tables'"
        )
        == f"{test_recover_staled_replica_run}\n"
    )
    test_recover_staled_replica_run += 1
    table = dummy_node.query(
        "SHOW TABLES FROM recover_broken_tables LIKE 'mt1_29_%' LIMIT 1"
    ).strip()
    assert (
        dummy_node.query("SELECT (*,).1 FROM recover_broken_tables.{}".format(table))
        == "42\n"
    )
    table = dummy_node.query(
        "SHOW TABLES FROM recover_broken_replicated_tables LIKE 'rmt5_29_%' LIMIT 1"
    ).strip()
    assert (
        dummy_node.query(
            "SELECT (*,).1 FROM recover_broken_replicated_tables.{}".format(table)
        )
        == "42\n"
    )

    expected = "Cleaned 6 outdated objects: dropped 1 dictionaries and 3 tables, moved 2 tables"
    assert_logs_contain(dummy_node, expected)

    dummy_node.query("DROP TABLE recover.tmp")
    assert_eq_with_retry(
        main_node,
        "SELECT count() FROM system.tables WHERE database='recover' AND name='tmp'",
        "0\n",
    )
    main_node.query("DROP DATABASE recover SYNC")
    dummy_node.query("DROP DATABASE recover SYNC")


def test_startup_without_zk(started_cluster):
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(main_node)
        err = main_node.query_and_get_error(
            "CREATE DATABASE startup ENGINE = Replicated('/clickhouse/databases/startup', 'shard1', 'replica1');"
        )
        assert "ZooKeeper" in err
    main_node.query(
        "CREATE DATABASE startup ENGINE = Replicated('/clickhouse/databases/startup', 'shard1', 'replica1');"
    )
    # main_node.query("CREATE TABLE startup.rmt (n int) ENGINE=ReplicatedMergeTree order by n")
    main_node.query("CREATE TABLE startup.rmt (n int) ENGINE=MergeTree order by n")
    main_node.query("INSERT INTO startup.rmt VALUES (42)")
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(main_node)
        main_node.restart_clickhouse(stop_start_wait_sec=30)
        assert main_node.query("SELECT (*,).1 FROM startup.rmt") == "42\n"

    for _ in range(10):
        try:
            main_node.query("CREATE TABLE startup.m (n int) ENGINE=Memory")
            break
        except:
            time.sleep(1)

    main_node.query("EXCHANGE TABLES startup.rmt AND startup.m")
    assert main_node.query("SELECT (*,).1 FROM startup.m") == "42\n"
    main_node.query("DROP DATABASE startup SYNC")


def test_server_uuid(started_cluster):
    uuid1 = main_node.query("select serverUUID()")
    uuid2 = dummy_node.query("select serverUUID()")
    assert uuid1 != uuid2
    main_node.restart_clickhouse()
    uuid1_after_restart = main_node.query("select serverUUID()")
    assert uuid1 == uuid1_after_restart


def test_sync_replica(started_cluster):
    main_node.query(
        "CREATE DATABASE test_sync_database ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE test_sync_database ENGINE = Replicated('/clickhouse/databases/test1', 'shard1', 'replica2');"
    )

    number_of_tables = 1000

    settings = {"distributed_ddl_task_timeout": 0}

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(dummy_node)

        for i in range(number_of_tables):
            main_node.query(
                "CREATE TABLE test_sync_database.table_{} (n int) ENGINE=MergeTree order by n".format(
                    i
                ),
                settings=settings,
            )

    # wait for host to reconnect
    dummy_node.query_with_retry("SELECT * FROM system.zookeeper WHERE path='/'")

    dummy_node.query("SYSTEM SYNC DATABASE REPLICA test_sync_database")

    assert dummy_node.query(
        "SELECT count() FROM system.tables where database='test_sync_database'"
    ).strip() == str(number_of_tables)

    assert main_node.query(
        "SELECT count() FROM system.tables where database='test_sync_database'"
    ).strip() == str(number_of_tables)

    engine_settings = {"default_table_engine": "ReplicatedMergeTree"}
    dummy_node.query(
        "CREATE TABLE test_sync_database.table (n int, primary key n) partition by n",
        settings=engine_settings,
    )
    main_node.query("INSERT INTO test_sync_database.table SELECT * FROM numbers(10)")
    dummy_node.query("TRUNCATE TABLE test_sync_database.table", settings=settings)
    dummy_node.query(
        "ALTER TABLE test_sync_database.table ADD COLUMN m int", settings=settings
    )

    main_node.query(
        "SYSTEM SYNC DATABASE REPLICA ON CLUSTER test_sync_database test_sync_database"
    )

    lp1 = main_node.query(
        "select value from system.zookeeper where path='/clickhouse/databases/test1/replicas/shard1|replica1' and name='log_ptr'"
    )
    lp2 = main_node.query(
        "select value from system.zookeeper where path='/clickhouse/databases/test1/replicas/shard1|replica2' and name='log_ptr'"
    )
    max_lp = main_node.query(
        "select value from system.zookeeper where path='/clickhouse/databases/test1/' and name='max_log_ptr'"
    )
    assert lp1 == max_lp
    assert lp2 == max_lp


def test_force_synchronous_settings(started_cluster):
    main_node.query(
        "CREATE DATABASE test_force_synchronous_settings ENGINE = Replicated('/clickhouse/databases/test2', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE test_force_synchronous_settings ENGINE = Replicated('/clickhouse/databases/test2', 'shard1', 'replica2');"
    )
    snapshotting_node.query(
        "CREATE DATABASE test_force_synchronous_settings ENGINE = Replicated('/clickhouse/databases/test2', 'shard2', 'replica1');"
    )
    main_node.query(
        "CREATE TABLE test_force_synchronous_settings.t (n int) ENGINE=ReplicatedMergeTree('/test/same/path/{shard}', '{replica}') ORDER BY tuple()"
    )
    main_node.query(
        "INSERT INTO test_force_synchronous_settings.t SELECT * FROM numbers(10)"
    )
    snapshotting_node.query(
        "INSERT INTO test_force_synchronous_settings.t SELECT * FROM numbers(10)"
    )
    snapshotting_node.query(
        "SYSTEM SYNC DATABASE REPLICA test_force_synchronous_settings"
    )
    dummy_node.query("SYSTEM SYNC DATABASE REPLICA test_force_synchronous_settings")

    snapshotting_node.query("SYSTEM STOP MERGES test_force_synchronous_settings.t")

    def start_merges_func():
        time.sleep(5)
        snapshotting_node.query("SYSTEM START MERGES test_force_synchronous_settings.t")

    start_merges_thread = threading.Thread(target=start_merges_func)
    start_merges_thread.start()

    settings = {
        "mutations_sync": 2,
        "database_replicated_enforce_synchronous_settings": 1,
    }
    main_node.query(
        "ALTER TABLE test_force_synchronous_settings.t UPDATE n = n * 10 WHERE 1",
        settings=settings,
    )
    assert "10\t450\n" == snapshotting_node.query(
        "SELECT count(), sum(n) FROM test_force_synchronous_settings.t"
    )
    start_merges_thread.join()

    def select_func():
        dummy_node.query(
            "SELECT sleepEachRow(1) FROM test_force_synchronous_settings.t"
        )

    select_thread = threading.Thread(target=select_func)
    select_thread.start()

    settings = {"database_replicated_enforce_synchronous_settings": 1}
    snapshotting_node.query(
        "DROP TABLE test_force_synchronous_settings.t SYNC", settings=settings
    )
    main_node.query(
        "CREATE TABLE test_force_synchronous_settings.t (n String) ENGINE=ReplicatedMergeTree('/test/same/path/{shard}', '{replica}') ORDER BY tuple()"
    )
    select_thread.join()


def test_recover_digest_mismatch(started_cluster):
    main_node.query(
        "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE recover_digest_mismatch ENGINE = Replicated('/clickhouse/databases/recover_digest_mismatch', 'shard1', 'replica2');"
    )

    create_some_tables("recover_digest_mismatch")

    ways_to_corrupt_metadata = [
        f"mv /var/lib/clickhouse/metadata/recover_digest_mismatch/t1.sql /var/lib/clickhouse/metadata/recover_digest_mismatch/m1.sql",
        f"sed --follow-symlinks -i 's/Int32/String/' /var/lib/clickhouse/metadata/recover_digest_mismatch/mv1.sql",
        f"rm -f /var/lib/clickhouse/metadata/recover_digest_mismatch/d1.sql",
        # f"rm -rf /var/lib/clickhouse/metadata/recover_digest_mismatch/", # Directory already exists
        f"rm -rf /var/lib/clickhouse/store",
    ]

    for command in ways_to_corrupt_metadata:
        need_remove_is_active_node = "rm -rf" in command
        dummy_node.stop_clickhouse(kill=not need_remove_is_active_node)
        dummy_node.exec_in_container(["bash", "-c", command])
        dummy_node.start_clickhouse()

        query = (
            "SELECT name, uuid, create_table_query FROM system.tables WHERE database='recover_digest_mismatch' AND name NOT LIKE '.inner_id.%' "
            "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
        )
        expected = main_node.query(query)

        if "rm -rf" in command:
            # NOTE Otherwise it fails to recreate ReplicatedMergeTree table due to "Replica already exists"
            main_node.query(
                "SYSTEM DROP REPLICA '2' FROM DATABASE recover_digest_mismatch"
            )

        assert_eq_with_retry(dummy_node, query, expected)

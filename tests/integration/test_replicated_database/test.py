import logging
import os
import re
import shutil
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
    # Disable `with_remote_database_disk` as in `test_startup_without_zk`, Keeper rejects `main_node` connections before restarting
    with_remote_database_disk=False,
)
dummy_node = cluster.add_instance(
    "dummy_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings2.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
competing_node = cluster.add_instance(
    "competing_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
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

bad_settings_node = cluster.add_instance(
    "bad_settings_node",
    main_configs=["configs/config2.xml"],
    user_configs=["configs/inconsistent_settings.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 4},
)

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


def test_flatten_nested(started_cluster):
    main_node.query(
        "CREATE DATABASE flatten_nested ENGINE = Replicated('/test/flatten_nested', 'shard1', 'replica' || '1');"
    )
    dummy_node.query(
        "CREATE DATABASE flatten_nested ENGINE = Replicated('/test/flatten_nested', 'shard1', 'replica2');"
    )

    main_node.query(
        "CREATE TABLE flatten_nested.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
    )

    main_node.query(
        "CREATE MATERIALIZED VIEW flatten_nested.mv ENGINE=ReplicatedMergeTree ORDER BY tuple() AS select d, cast([(k, toString(i32))] as Nested(a UInt64, b String)) from flatten_nested.replicated_table"
    )

    main_node.query(
        "CREATE TABLE flatten_nested.no_flatten (n Nested(a UInt64, b String)) ENGINE=ReplicatedMergeTree ORDER BY tuple();",
        settings={"flatten_nested": 0},
    )

    snapshot_recovering_node.query(
        "CREATE DATABASE flatten_nested ENGINE = Replicated('/test/flatten_nested', 'shard1', 'replica3');"
    )
    snapshot_recovering_node.query(
        "SYSTEM SYNC DATABASE REPLICA flatten_nested"
    )

    for node in [dummy_node, snapshot_recovering_node]:
        for table in ["replicated_table", "mv", "no_flatten"]:
            assert main_node.query(
                f"show create flatten_nested.{table}"
            ) == node.query(f"show create flatten_nested.{table}")

    main_node.query("DROP DATABASE flatten_nested SYNC")
    dummy_node.query("DROP DATABASE flatten_nested SYNC")
    snapshot_recovering_node.query("DROP DATABASE flatten_nested SYNC")


def test_create_replicated_table(started_cluster):
    main_node.query(
        "CREATE DATABASE create_replicated_table ENGINE = Replicated('/test/create_replicated_table', 'shard1', 'replica' || '1');"
    )
    dummy_node.query(
        "CREATE DATABASE create_replicated_table ENGINE = Replicated('/test/create_replicated_table', 'shard1', 'replica2');"
    )
    assert (
        "Explicit zookeeper_path and replica_name are specified"
        in main_node.query_and_get_error(
            "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) "
            "ENGINE=ReplicatedMergeTree('/test/tmp', 'r') ORDER BY k PARTITION BY toYYYYMM(d);"
        )
    )

    assert (
        "Explicit zookeeper_path and replica_name are specified"
        in main_node.query_and_get_error(
            "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) "
            "ENGINE=ReplicatedMergeTree('/test/tmp', 'r') ORDER BY k PARTITION BY toYYYYMM(d);"
        )
    )

    assert (
        "This syntax for *MergeTree engine is deprecated"
        in main_node.query_and_get_error(
            "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) "
            "ENGINE=ReplicatedMergeTree('/test/tmp/{shard}', '{replica}', d, k, 8192);"
        )
    )

    main_node.query(
        "CREATE TABLE create_replicated_table.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
    )

    expected = (
        "CREATE TABLE create_replicated_table.replicated_table\\n(\\n    `d` Date,\\n    `k` UInt64,\\n    `i32` Int32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\n"
        "PARTITION BY toYYYYMM(d)\\nORDER BY k\\nSETTINGS index_granularity = 8192"
    )
    assert_create_query(
        [main_node, dummy_node], "create_replicated_table.replicated_table", expected
    )
    # assert without replacing uuid
    assert main_node.query(
        "show create create_replicated_table.replicated_table"
    ) == dummy_node.query("show create create_replicated_table.replicated_table")
    main_node.query("DROP DATABASE create_replicated_table SYNC")
    dummy_node.query("DROP DATABASE create_replicated_table SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_simple_alter_table(started_cluster, engine):
    database = f"test_simple_alter_table_{engine}"
    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
    )
    # test_simple_alter_table
    name = f"{database}.alter_test"
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
        f"CREATE DATABASE IF NOT EXISTS {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica3');"
    )

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
    main_node.query(f"DROP DATABASE {database} SYNC")
    dummy_node.query(f"DROP DATABASE {database} SYNC")
    competing_node.query(f"DROP DATABASE {database} SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_delete_from_table(started_cluster, engine):
    database = f"delete_from_table_{engine}"

    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard2', 'replica1');"
    )

    name = f"{database}.delete_test"
    main_node.query(
        "CREATE TABLE {} "
        "(id UInt64, value String) "
        "ENGINE = {} PARTITION BY id%2 ORDER BY (id);".format(name, engine)
    )
    main_node.query("INSERT INTO TABLE {} VALUES(1, 'aaaa');".format(name))
    main_node.query("INSERT INTO TABLE {} VALUES(2, 'aaaa');".format(name))
    dummy_node.query("INSERT INTO TABLE {} VALUES(1, 'bbbb');".format(name))
    dummy_node.query("INSERT INTO TABLE {} VALUES(2, 'bbbb');".format(name))

    main_node.query("DELETE FROM {} WHERE id=2;".format(name))

    expected = "1\taaaa\n1\tbbbb"

    table_for_select = name
    if not "Replicated" in engine:
        table_for_select = f"cluster('{database}', {name})"
    for node in [main_node, dummy_node]:
        assert_eq_with_retry(
            node,
            "SELECT * FROM {} ORDER BY id, value;".format(table_for_select),
            expected,
        )

    main_node.query(f"DROP DATABASE {database} SYNC")
    dummy_node.query(f"DROP DATABASE {database} SYNC")


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
    database = f"alter_attach_{engine}"
    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
    )

    main_node.query(
        f"CREATE TABLE {database}.alter_attach_test (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    table_uuid = get_table_uuid(database, "alter_attach_test")
    # Provide and attach a part to the main node
    shutil.copytree(
        attachable_part,
        os.path.join(
            main_node.path,
            f"database/store/{table_uuid[:3]}/{table_uuid}/detached/all_1_1_0",
        ),
    )
    main_node.query(f"ALTER TABLE {database}.alter_attach_test ATTACH PART 'all_1_1_0'")
    # On the main node, data is attached
    assert (
        main_node.query(f"SELECT CounterID FROM {database}.alter_attach_test")
        == "123\n"
    )

    # On the other node, data is replicated only if using a Replicated table engine
    if engine == "ReplicatedMergeTree":
        dummy_node.query(
            f"SYSTEM SYNC REPLICA {database}.alter_attach_test LIGHTWEIGHT"
        )
        assert (
            dummy_node.query(f"SELECT CounterID FROM {database}.alter_attach_test")
            == "123\n"
        )
    else:
        assert (
            dummy_node.query(f"SELECT CounterID FROM {database}.alter_attach_test")
            == ""
        )
    main_node.query(f"DROP DATABASE {database} SYNC")
    dummy_node.query(f"DROP DATABASE {database} SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_drop_part(started_cluster, engine):
    database = f"alter_drop_part_{engine}"
    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
    )

    part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
    main_node.query(
        f"CREATE TABLE {database}.alter_drop_part (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO {database}.alter_drop_part VALUES (123)")
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO {database}.alter_drop_part VALUES (456)")
    else:
        main_node.query(f"SYSTEM SYNC REPLICA {database}.alter_drop_part PULL")
    main_node.query(f"ALTER TABLE {database}.alter_drop_part DROP PART '{part_name}'")
    assert main_node.query(f"SELECT CounterID FROM {database}.alter_drop_part") == ""
    if engine == "ReplicatedMergeTree":
        # The DROP operation is still replicated at the table engine level
        assert (
            dummy_node.query(f"SELECT CounterID FROM {database}.alter_drop_part") == ""
        )
    else:
        assert (
            dummy_node.query(f"SELECT CounterID FROM {database}.alter_drop_part")
            == "456\n"
        )
    main_node.query(f"DROP DATABASE {database} SYNC")
    dummy_node.query(f"DROP DATABASE {database} SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_detach_part(started_cluster, engine):
    database = f"alter_detach_part_{engine}"
    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
    )

    part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
    main_node.query(
        f"CREATE TABLE {database}.alter_detach (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO {database}.alter_detach VALUES (123)")
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO {database}.alter_detach VALUES (456)")
    else:
        main_node.query(f"SYSTEM SYNC REPLICA {database}.alter_detach PULL")
    main_node.query(f"ALTER TABLE {database}.alter_detach DETACH PART '{part_name}'")
    detached_parts_query = f"SELECT name FROM system.detached_parts WHERE database='{database}' AND table='alter_detach'"
    assert main_node.query(detached_parts_query) == f"{part_name}\n"
    if engine == "ReplicatedMergeTree":
        # The detach operation is still replicated at the table engine level
        assert dummy_node.query(detached_parts_query) == f"{part_name}\n"
    else:
        assert dummy_node.query(detached_parts_query) == ""
    main_node.query(f"DROP DATABASE {database} SYNC")
    dummy_node.query(f"DROP DATABASE {database} SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_drop_detached_part(started_cluster, engine):
    database = f"alter_drop_detached_part_{engine}"
    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
    )

    part_name = "all_0_0_0" if engine == "ReplicatedMergeTree" else "all_1_1_0"
    main_node.query(
        f"CREATE TABLE {database}.alter_drop_detached (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO {database}.alter_drop_detached VALUES (123)")
    main_node.query(
        f"ALTER TABLE {database}.alter_drop_detached DETACH PART '{part_name}'"
    )
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO {database}.alter_drop_detached VALUES (456)")
        dummy_node.query(
            f"ALTER TABLE {database}.alter_drop_detached DETACH PART '{part_name}'"
        )
    main_node.query(
        f"ALTER TABLE {database}.alter_drop_detached DROP DETACHED PART '{part_name}'"
    )
    detached_parts_query = f"SELECT name FROM system.detached_parts WHERE database='{database}' AND table='alter_drop_detached'"
    assert main_node.query(detached_parts_query) == ""
    assert dummy_node.query(detached_parts_query) == f"{part_name}\n"

    main_node.query(f"DROP DATABASE {database} SYNC")
    dummy_node.query(f"DROP DATABASE {database} SYNC")


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_alter_drop_partition(started_cluster, engine):
    database = f"alter_drop_partition_{engine}"
    main_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica1');"
    )
    dummy_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard1', 'replica2');"
    )
    snapshotting_node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard2', 'replica1');"
    )

    main_node.query(
        f"CREATE TABLE {database}.alter_drop (CounterID UInt32) ENGINE = {engine} ORDER BY (CounterID)"
    )
    main_node.query(f"INSERT INTO {database}.alter_drop VALUES (123)")
    if engine == "MergeTree":
        dummy_node.query(f"INSERT INTO {database}.alter_drop VALUES (456)")
    snapshotting_node.query(f"INSERT INTO {database}.alter_drop VALUES (789)")
    main_node.query(
        f"ALTER TABLE {database}.alter_drop ON CLUSTER {database} DROP PARTITION ID 'all'",
        settings={"replication_alter_partitions_sync": 2},
    )
    assert (
        main_node.query(
            f"SELECT CounterID FROM clusterAllReplicas('{database}', {database}.alter_drop)"
        )
        == ""
    )
    assert dummy_node.query(f"SELECT CounterID FROM {database}.alter_drop") == ""
    main_node.query(f"DROP DATABASE {database}")
    dummy_node.query(f"DROP DATABASE {database}")
    snapshotting_node.query(f"DROP DATABASE {database}")


def test_alter_fetch(started_cluster):
    main_node.query(
        "CREATE DATABASE alter_fetch ENGINE = Replicated('/test/alter_fetch', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE alter_fetch ENGINE = Replicated('/test/alter_fetch', 'shard1', 'replica2');"
    )

    main_node.query(
        "CREATE TABLE alter_fetch.fetch_source (CounterID UInt32) ENGINE = ReplicatedMergeTree ORDER BY (CounterID)"
    )
    main_node.query(
        "CREATE TABLE alter_fetch.fetch_target (CounterID UInt32) ENGINE = ReplicatedMergeTree ORDER BY (CounterID)"
    )
    main_node.query("INSERT INTO alter_fetch.fetch_source VALUES (123)")
    table_uuid = get_table_uuid("alter_fetch", "fetch_source")
    main_node.query(
        f"ALTER TABLE alter_fetch.fetch_target FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/{table_uuid}/{{shard}}' "
    )
    detached_parts_query = "SELECT name FROM system.detached_parts WHERE database='alter_fetch' AND table='fetch_target'"
    assert main_node.query(detached_parts_query) == "all_0_0_0\n"
    assert dummy_node.query(detached_parts_query) == ""

    main_node.query("DROP DATABASE alter_fetch SYNC")
    dummy_node.query("DROP DATABASE alter_fetch SYNC")


def test_alters_from_different_replicas(started_cluster):
    main_node.query(
        "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica2');"
    )

    # test_alters_from_different_replicas
    competing_node.query(
        "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica3');"
    )

    main_node.query(
        "CREATE TABLE alters_from_different_replicas.concurrent_test "
        "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
        "ENGINE = MergeTree PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);"
    )

    main_node.query(
        "CREATE TABLE alters_from_different_replicas.dist AS alters_from_different_replicas.concurrent_test ENGINE = Distributed(alters_from_different_replicas, alters_from_different_replicas, concurrent_test, CounterID)"
    )

    dummy_node.stop_clickhouse(kill=True)

    settings = {"distributed_ddl_task_timeout": 5}
    assert "is not finished on 1 of 3 hosts" in competing_node.query_and_get_error(
        "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN Added0 UInt32;",
        settings=settings,
    )
    settings = {
        "distributed_ddl_task_timeout": 5,
        "distributed_ddl_output_mode": "null_status_on_timeout",
    }
    assert "shard1\treplica2\tQUEUED\t" in main_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN Added2 UInt32;",
        settings=settings,
    )
    settings = {
        "distributed_ddl_task_timeout": 5,
        "distributed_ddl_output_mode": "never_throw",
    }
    assert "shard1\treplica2\tQUEUED\t" in competing_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN Added1 UInt32 AFTER Added0;",
        settings=settings,
    )
    dummy_node.start_clickhouse()
    main_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;"
    )
    competing_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;"
    )
    main_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;"
    )

    expected = (
        "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32,\\n"
        "    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n    `AddedNested1.A` Array(UInt32),\\n"
        "    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n    `AddedNested2.A` Array(UInt32),\\n"
        "    `AddedNested2.B` Array(UInt64)\\n)\\n"
        "ENGINE = MergeTree\\nPARTITION BY toYYYYMM(StartDate)\\nORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)\\nSETTINGS index_granularity = 8192"
    )

    assert_create_query(
        [main_node, competing_node],
        "alters_from_different_replicas.concurrent_test",
        expected,
    )

    # test_create_replica_after_delay
    main_node.query("DROP TABLE alters_from_different_replicas.concurrent_test SYNC")
    main_node.query(
        "CREATE TABLE alters_from_different_replicas.concurrent_test "
        "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
        "ENGINE = ReplicatedMergeTree ORDER BY CounterID;"
    )

    expected = (
        "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
    )

    assert_create_query(
        [main_node, competing_node],
        "alters_from_different_replicas.concurrent_test",
        expected,
    )

    main_node.query(
        "INSERT INTO alters_from_different_replicas.dist (CounterID, StartDate, UserID) SELECT number, addDays(toDate('2020-02-02'), number), intHash32(number) FROM numbers(10)"
    )

    # test_replica_restart
    main_node.restart_clickhouse()

    expected = (
        "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
    )

    # test_snapshot_and_snapshot_recover
    snapshotting_node.query(
        "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard2', 'replica1');"
    )
    snapshot_recovering_node.query(
        "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard2', 'replica2');"
    )
    assert_create_query(
        all_nodes, "alters_from_different_replicas.concurrent_test", expected
    )

    main_node.query("SYSTEM FLUSH DISTRIBUTED alters_from_different_replicas.dist")
    main_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test UPDATE StartDate = addYears(StartDate, 1) WHERE 1"
    )
    res = main_node.query(
        "ALTER TABLE alters_from_different_replicas.concurrent_test DELETE WHERE UserID % 2"
    )
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
            "SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster='alters_from_different_replicas'"
        )
        == expected
    )

    # test_drop_and_create_replica
    main_node.query("DROP DATABASE alters_from_different_replicas SYNC")
    main_node.query(
        "CREATE DATABASE alters_from_different_replicas ENGINE = Replicated('/test/alters_from_different_replicas', 'shard1', 'replica1');"
    )

    expected = (
        "CREATE TABLE alters_from_different_replicas.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n"
        "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n"
        "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY CounterID\\nSETTINGS index_granularity = 8192"
    )

    assert_create_query(
        [main_node, competing_node],
        "alters_from_different_replicas.concurrent_test",
        expected,
    )
    assert_create_query(
        all_nodes, "alters_from_different_replicas.concurrent_test", expected
    )

    for node in all_nodes:
        node.query("SYSTEM SYNC REPLICA alters_from_different_replicas.concurrent_test")

    expected = (
        "0\t2021-02-02\t4249604106\n"
        "1\t2021-02-03\t1343103100\n"
        "4\t2021-02-06\t3902320246\n"
        "7\t2021-02-09\t3844986530\n"
        "9\t2021-02-11\t1241149650\n"
    )

    assert_eq_with_retry(
        dummy_node,
        "SELECT CounterID, StartDate, UserID FROM alters_from_different_replicas.dist ORDER BY CounterID",
        expected,
    )
    main_node.query("DROP DATABASE alters_from_different_replicas SYNC")
    dummy_node.query("DROP DATABASE alters_from_different_replicas SYNC")
    competing_node.query("DROP DATABASE alters_from_different_replicas SYNC")
    snapshotting_node.query("DROP DATABASE alters_from_different_replicas SYNC")
    snapshot_recovering_node.query("DROP DATABASE alters_from_different_replicas SYNC")


def create_some_tables(db):
    settings = {
        "distributed_ddl_task_timeout": 0,
        "allow_experimental_object_type": 1,
        "allow_suspicious_codecs": 1,
    }
    main_node.query(f"CREATE TABLE {db}.t1 (n int) ENGINE=Memory", settings=settings)
    dummy_node.query(
        f"CREATE TABLE {db}.t2 (s String) ENGINE=Memory", settings=settings
    )
    main_node.query(
        f"CREATE TABLE {db}.mt1 (n int) ENGINE=MergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.mt2 (n int) ENGINE=MergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE TABLE {db}.rmt1 (n int) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.rmt2 (n int CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12))) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE TABLE {db}.rmt3 (n int, json Object('json') materialized '') ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE TABLE {db}.rmt5 (n int) ENGINE=ReplicatedMergeTree order by n",
        settings=settings,
    )
    main_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv1 (n int) ENGINE=ReplicatedMergeTree order by n AS SELECT n FROM {db}.rmt1",
        settings=settings,
    )
    dummy_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv2 (n int) ENGINE=ReplicatedMergeTree order by n  AS SELECT n FROM {db}.rmt2",
        settings=settings,
    )
    main_node.query(
        f"CREATE DICTIONARY {db}.d1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
    )
    dummy_node.query(
        f"CREATE DICTIONARY {db}.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt2' PASSWORD '' DB 'recover')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())"
    )


# These tables are used to check that DatabaseReplicated correctly renames all the tables in case when it restores from the lost state
def create_table_for_exchanges(db):
    settings = {"distributed_ddl_task_timeout": 0}
    for table in ["a1", "a2", "a3", "a4", "a5", "a6"]:
        main_node.query(
            f"CREATE TABLE {db}.{table} (s String) ENGINE=ReplicatedMergeTree order by s",
            settings=settings,
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
    create_table_for_exchanges("recover")

    for table in ["t1", "t2", "mt1", "mt2", "rmt1", "rmt2", "rmt3", "rmt5"]:
        main_node.query(f"INSERT INTO recover.{table} VALUES (42)")
    for table in ["t1", "t2", "mt1", "mt2"]:
        dummy_node.query(f"INSERT INTO recover.{table} VALUES (42)")

    for i, table in enumerate(["a1", "a2", "a3", "a4", "a5", "a6"]):
        main_node.query(f"INSERT INTO recover.{table} VALUES ('{str(i + 1) * 10}')")

    for table in ["rmt1", "rmt2", "rmt3", "rmt5"]:
        main_node.query(f"SYSTEM SYNC REPLICA recover.{table}")
    for table in ["a1", "a2", "a3", "a4", "a5", "a6"]:
        main_node.query(f"SYSTEM SYNC REPLICA recover.{table}")

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
            f"ALTER TABLE recover.`{inner_table}` MODIFY COLUMN n int DEFAULT 42",
            settings=settings,
        )
        main_node.query_with_retry(
            "ALTER TABLE recover.mv1 MODIFY QUERY SELECT m as n FROM recover.rmt1",
            settings=settings,
        )
        main_node.query_with_retry(
            "RENAME TABLE recover.mv2 TO recover.mv3",
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

        main_node.query("EXCHANGE TABLES recover.a1 AND recover.a2", settings=settings)
        main_node.query("EXCHANGE TABLES recover.a3 AND recover.a4", settings=settings)
        main_node.query("EXCHANGE TABLES recover.a5 AND recover.a4", settings=settings)
        main_node.query("EXCHANGE TABLES recover.a6 AND recover.a3", settings=settings)
        main_node.query("RENAME TABLE recover.a6 TO recover.a7", settings=settings)
        main_node.query("RENAME TABLE recover.a1 TO recover.a8", settings=settings)

    assert (
        main_node.query(
            "SELECT name FROM system.tables WHERE database='recover' AND name NOT LIKE '.inner_id.%' ORDER BY name"
        )
        == "a2\na3\na4\na5\na7\na8\nd1\nd2\nm1\nmt1\nmt2\nmv1\nmv3\nrmt1\nrmt2\nrmt4\nt2\ntmp\n"
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

    # Check that Database Replicated renamed all the tables correctly
    for i, table in enumerate(["a2", "a8", "a5", "a7", "a4", "a3"]):
        assert (
            dummy_node.query(f"SELECT * FROM recover.{table}") == f"{str(i + 1) * 10}\n"
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
        assert main_node.query(f"SELECT (*,).1 FROM recover.{table}") == "42\n"
    for table in ["t2", "rmt1", "rmt2", "rmt4", "d1", "d2", "mt2", "mv1", "mv3"]:
        assert (
            dummy_node.query(f"SELECT '{table}', (*,).1 FROM recover.{table}")
            == f"{table}\t42\n"
        )
    for table in ["m1", "mt1"]:
        assert dummy_node.query(f"SELECT count() FROM recover.{table}") == "0\n"

    logging.debug("Result: %s", dummy_node.query("SHOW DATABASES"))
    logging.debug("Result: %s", dummy_node.query("SHOW TABLES FROM recover_broken_tables"))
    logging.debug("Result: %s", dummy_node.query("SHOW TABLES FROM recover_broken_replicated_tables"))

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
        "SHOW TABLES FROM recover_broken_tables LIKE 'mt1_41_%' LIMIT 1"
    ).strip()
    assert (
        dummy_node.query(f"SELECT (*,).1 FROM recover_broken_tables.{table}") == "42\n"
    )
    table = dummy_node.query(
        "SHOW TABLES FROM recover_broken_replicated_tables LIKE 'rmt5_41_%' LIMIT 1"
    ).strip()
    assert (
        dummy_node.query(f"SELECT (*,).1 FROM recover_broken_replicated_tables.{table}")
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


def test_recover_staled_replica_many_mvs(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS recover_mvs SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS recover_mvs SYNC")

    main_node.query_with_retry(
        "CREATE DATABASE IF NOT EXISTS recover_mvs ENGINE = Replicated('/clickhouse/databases/recover_mvs', 'shard1', 'replica1');"
    )
    started_cluster.get_kazoo_client("zoo1").set(
        "/clickhouse/databases/recover_mvs/logs_to_keep", b"10"
    )
    dummy_node.query_with_retry(
        "CREATE DATABASE IF NOT EXISTS recover_mvs ENGINE = Replicated('/clickhouse/databases/recover_mvs', 'shard1', 'replica2');"
    )

    settings = {"distributed_ddl_task_timeout": 0}

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(dummy_node)
        dummy_node.query_and_get_error("RENAME TABLE recover_mvs.t1 TO recover_mvs.m1")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query(
                f"CREATE TABLE recover_mvs.rmt{identifier} (n int) ENGINE=ReplicatedMergeTree ORDER BY n",
                settings=settings,
            )

        print("Created tables")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query(
                f"CREATE TABLE recover_mvs.mv_inner{identifier} (n int) ENGINE=ReplicatedMergeTree ORDER BY n",
                settings=settings,
            )

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE MATERIALIZED VIEW recover_mvs.mv{identifier}
                    TO recover_mvs.mv_inner{identifier}
                    AS SELECT * FROM recover_mvs.rmt{identifier}""",
                settings=settings,
            )

        print("Created MVs")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE VIEW recover_mvs.view_from_mv{identifier}
                    AS SELECT * FROM recover_mvs.mv{identifier}""",
                settings=settings,
            )

        print("Created Views on top of MVs")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE MATERIALIZED VIEW recover_mvs.cascade_mv{identifier}
                    ENGINE=MergeTree() ORDER BY tuple()
                    POPULATE AS SELECT * FROM recover_mvs.mv_inner{identifier};""",
                settings=settings,
            )

        print("Created cascade MVs")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE VIEW recover_mvs.view_from_cascade_mv{identifier}
                    AS SELECT * FROM recover_mvs.cascade_mv{identifier}""",
                settings=settings,
            )

        print("Created Views on top of cascade MVs")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE MATERIALIZED VIEW recover_mvs.double_cascade_mv{identifier}
                    ENGINE=MergeTree() ORDER BY tuple()
                    POPULATE AS SELECT * FROM recover_mvs.`.inner_id.{get_table_uuid("recover_mvs", f"cascade_mv{identifier}")}`""",
                settings=settings,
            )

        print("Created double cascade MVs")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE VIEW recover_mvs.view_from_double_cascade_mv{identifier}
                    AS SELECT * FROM recover_mvs.double_cascade_mv{identifier}""",
                settings=settings,
            )

        print("Created Views on top of double cascade MVs")

        # This weird table name is actually makes sence because it starts with letter `a` and may break some internal sorting
        main_node.query_with_retry(
            """
            CREATE VIEW recover_mvs.anime
            AS
            SELECT n
            FROM
            (
                SELECT *
                FROM
                (
                    SELECT *
                    FROM
                    (
                        SELECT *
                        FROM recover_mvs.mv_inner1 AS q1
                        INNER JOIN recover_mvs.mv_inner2 AS q2 ON q1.n = q2.n
                    ) AS new_table_1
                    INNER JOIN recover_mvs.mv_inner3 AS q3 ON new_table_1.n = q3.n
                ) AS new_table_2
                INNER JOIN recover_mvs.mv_inner4 AS q4 ON new_table_2.n = q4.n
            )
            """,
            settings=settings,
        )

        print("Created final boss")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE DICTIONARY recover_mvs.`11111d{identifier}` (n UInt64)
                PRIMARY KEY n
                SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'double_cascade_mv{identifier}' DB 'recover_mvs'))
                LAYOUT(FLAT()) LIFETIME(1)""",
                settings=settings,
            )

        print("Created dictionaries")

        for identifier in ["1", "2", "3", "4"]:
            main_node.query_with_retry(
                f"""CREATE VIEW recover_mvs.`00000vd{identifier}`
                AS SELECT * FROM recover_mvs.`11111d{identifier}`""",
                settings=settings,
            )

        print("Created Views on top of dictionaries")

    dummy_node.query("SYSTEM SYNC DATABASE REPLICA recover_mvs")
    query = "SELECT name FROM system.tables WHERE database='recover_mvs' ORDER BY name"
    assert main_node.query(query) == dummy_node.query(query)

    main_node.query("DROP DATABASE IF EXISTS recover_mvs SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS recover_mvs SYNC")


def test_startup_without_zk(started_cluster):
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(main_node)
        err = main_node.query_and_get_error(
            "CREATE DATABASE startup ENGINE = Replicated('/clickhouse/databases/startup', 'shard1', 'replica1');"
        )
        assert "ZooKeeper" in err or "Coordination::Exception" in err
    main_node.query(
        "CREATE DATABASE startup ENGINE = Replicated('/clickhouse/databases/startup', 'shard1', 'replica1');"
    )
    main_node.query(
        "CREATE TABLE startup.rmt (n int) ENGINE=ReplicatedMergeTree order by n"
    )

    main_node.query("INSERT INTO startup.rmt VALUES (42)")
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(
            main_node, action="REJECT --reject-with tcp-reset"
        )
        main_node.restart_clickhouse(stop_start_wait_sec=120)
        assert main_node.query("SELECT (*,).1 FROM startup.rmt") == "42\n"

    # we need to wait until the table is not readonly
    main_node.query_with_retry("INSERT INTO startup.rmt VALUES(42)")

    main_node.query_with_retry("CREATE TABLE startup.m (n int) ENGINE=Memory")

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
        "CREATE DATABASE test_sync_database ENGINE = Replicated('/test/sync_replica', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE test_sync_database ENGINE = Replicated('/test/sync_replica', 'shard1', 'replica2');"
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

    assert "2\n" == main_node.query(
        "SELECT sum(is_active) FROM system.clusters WHERE cluster='test_sync_database'"
    )

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
        "select value from system.zookeeper where path='/test/sync_replica/replicas/shard1|replica1' and name='log_ptr'"
    )
    lp2 = main_node.query(
        "select value from system.zookeeper where path='/test/sync_replica/replicas/shard1|replica2' and name='log_ptr'"
    )
    max_lp = main_node.query(
        "select value from system.zookeeper where path='/test/sync_replica/' and name='max_log_ptr'"
    )
    assert lp1 == max_lp
    assert lp2 == max_lp

    main_node.query("DROP DATABASE test_sync_database SYNC")
    dummy_node.query("DROP DATABASE test_sync_database SYNC")


def test_force_synchronous_settings(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS test_force_synchronous_settings SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS test_force_synchronous_settings SYNC")
    snapshotting_node.query(
        "DROP DATABASE IF EXISTS test_force_synchronous_settings SYNC"
    )
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
            "SELECT sleepEachRow(1) FROM test_force_synchronous_settings.t SETTINGS function_sleep_max_microseconds_per_block = 0"
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

    main_node.query("DROP DATABASE test_force_synchronous_settings SYNC")
    dummy_node.query("DROP DATABASE test_force_synchronous_settings SYNC")
    snapshotting_node.query(
        "DROP DATABASE test_force_synchronous_settings SYNC"
    )


def test_replicated_table_structure_alter(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS table_structure SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS table_structure SYNC")
    competing_node.query("DROP DATABASE IF EXISTS table_structure SYNC")

    main_node.query(
        "CREATE DATABASE table_structure ENGINE = Replicated('/clickhouse/databases/table_structure', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE table_structure ENGINE = Replicated('/clickhouse/databases/table_structure', 'shard1', 'replica2');"
    )
    competing_node.query(
        "CREATE DATABASE table_structure ENGINE = Replicated('/clickhouse/databases/table_structure', 'shard1', 'replica3');"
    )

    competing_node.query("CREATE TABLE table_structure.mem (n int) ENGINE=Memory")
    dummy_node.query("DETACH DATABASE table_structure")

    settings = {"distributed_ddl_task_timeout": 0}
    main_node.query(
        "CREATE TABLE table_structure.rmt (n int, v UInt64) ENGINE=ReplicatedReplacingMergeTree(v) ORDER BY n",
        settings=settings,
    )

    competing_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
    competing_node.query("DETACH DATABASE table_structure")

    main_node.query(
        "ALTER TABLE table_structure.rmt ADD COLUMN m int", settings=settings
    )
    main_node.query(
        "ALTER TABLE table_structure.rmt COMMENT COLUMN v 'version'", settings=settings
    )
    main_node.query("INSERT INTO table_structure.rmt VALUES (1, 2, 3)")

    metadata_path = competing_node.query(
        f"SELECT metadata_path FROM system.tables WHERE database='table_structure' AND name='mem'"
    ).strip()
    db_disk_name = get_database_disk_name(competing_node)
    competing_node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "-C",
            "/etc/clickhouse-server/config.xml",
            "--disk",
            f"{db_disk_name}",
            "--save-logs",
            "--query",
            f"remove {metadata_path}",
        ],
        user="root",
    )
    competing_node.restart_clickhouse(kill=True)

    dummy_node.query("ATTACH DATABASE table_structure")
    dummy_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
    dummy_node.query("SYSTEM SYNC REPLICA table_structure.rmt")
    assert "1\t2\t3\n" == dummy_node.query("SELECT * FROM table_structure.rmt")

    competing_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
    competing_node.query("SYSTEM SYNC REPLICA table_structure.rmt")
    # time.sleep(600)
    assert "mem" in competing_node.query("SHOW TABLES FROM table_structure")
    assert "1\t2\t3\n" == competing_node.query("SELECT * FROM table_structure.rmt")

    main_node.query("ALTER TABLE table_structure.rmt ADD COLUMN k int")
    main_node.query("INSERT INTO table_structure.rmt VALUES (1, 2, 3, 4)")
    dummy_node.query("SYSTEM SYNC DATABASE REPLICA table_structure")
    dummy_node.query("SYSTEM SYNC REPLICA table_structure.rmt")
    assert "1\t2\t3\t0\n1\t2\t3\t4\n" == dummy_node.query(
        "SELECT * FROM table_structure.rmt ORDER BY k"
    )


def test_modify_comment(started_cluster):
    main_node.query(
        "CREATE DATABASE modify_comment_db ENGINE = Replicated('/test/modify_comment', 'shard1', 'replica' || '1');"
    )

    dummy_node.query(
        "CREATE DATABASE modify_comment_db ENGINE = Replicated('/test/modify_comment', 'shard1', 'replica' || '2');"
    )

    main_node.query(
        "CREATE TABLE modify_comment_db.modify_comment_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
    )

    def restart_verify_not_readonly():
        main_node.restart_clickhouse()
        assert (
            main_node.query(
                "SELECT is_readonly FROM system.replicas WHERE table = 'modify_comment_table'"
            )
            == "0\n"
        )
        dummy_node.restart_clickhouse()
        assert (
            dummy_node.query(
                "SELECT is_readonly FROM system.replicas WHERE table = 'modify_comment_table'"
            )
            == "0\n"
        )

    main_node.query(
        "ALTER TABLE modify_comment_db.modify_comment_table COMMENT COLUMN d 'Some comment'"
    )

    restart_verify_not_readonly()

    main_node.query(
        "ALTER TABLE modify_comment_db.modify_comment_table MODIFY COMMENT 'Some error comment'"
    )

    restart_verify_not_readonly()

    main_node.query("DROP DATABASE modify_comment_db SYNC")
    dummy_node.query("DROP DATABASE modify_comment_db SYNC")


def test_table_metadata_corruption(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS table_metadata_corruption SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS table_metadata_corruption SYNC")

    main_node.query(
        "CREATE DATABASE table_metadata_corruption ENGINE = Replicated('/clickhouse/databases/table_metadata_corruption', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE table_metadata_corruption ENGINE = Replicated('/clickhouse/databases/table_metadata_corruption', 'shard1', 'replica2');"
    )

    create_some_tables("table_metadata_corruption")

    main_node.query("SYSTEM SYNC DATABASE REPLICA table_metadata_corruption")
    dummy_node.query("SYSTEM SYNC DATABASE REPLICA table_metadata_corruption")

    metadata_path = dummy_node.query(
        f"SELECT metadata_path FROM system.tables WHERE database='table_metadata_corruption' AND name='rmt1'"
    ).strip()
    # Server should handle this by throwing an exception during table loading, which should lead to server shutdown

    print(f"Corrupting metadata {metadata_path}")
    dummy_node.stop_clickhouse(kill=True)
    replace_text_in_metadata(
        dummy_node, metadata_path, "ReplicatedMergeTree", "CorruptedMergeTree"
    )
    query = (
        "SELECT name, uuid, create_table_query FROM system.tables WHERE database='table_metadata_corruption' AND name NOT LIKE '.inner_id.%' "
        "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
    )
    expected = main_node.query(query)

    # We expect clickhouse server to shutdown without LOGICAL_ERRORs or deadlocks
    dummy_node.start_clickhouse(expected_to_fail=True)

    assert not dummy_node.contains_in_log("LOGICAL_ERROR")

    print(f"Fix corrupted metadata")
    replace_text_in_metadata(
        dummy_node, metadata_path, "CorruptedMergeTree", "ReplicatedMergeTree"
    )

    dummy_node.start_clickhouse()
    assert_eq_with_retry(dummy_node, query, expected)

    main_node.query("DROP DATABASE IF EXISTS table_metadata_corruption SYNC")
    dummy_node.query("DROP DATABASE IF EXISTS table_metadata_corruption SYNC")


def test_auto_recovery(started_cluster):
    dummy_node.query("DROP DATABASE IF EXISTS auto_recovery SYNC")
    bad_settings_node.query(
        "DROP DATABASE IF EXISTS auto_recovery",
        settings={
            "implicit_transaction": 0,
        },
    )

    dummy_node.query(
        "CREATE DATABASE auto_recovery ENGINE = Replicated('/clickhouse/databases/auto_recovery', 'shard1', 'replica1');"
    )
    bad_settings_node.query(
        "CREATE DATABASE auto_recovery ENGINE = Replicated('/clickhouse/databases/auto_recovery', 'shard1', 'replica2') SETTINGS max_retries_before_automatic_recovery=3;"
    )

    dummy_node.query(
        "CREATE TABLE auto_recovery.t1 (n int) ENGINE=ReplicatedMergeTree ORDER BY n"
    )
    dummy_node.query("INSERT INTO auto_recovery.t1 SELECT 42")
    # dummy_node has <throw_on_unsupported_query_inside_transaction>0</throw_on_unsupported_query_inside_transaction> (default is 1),
    # so it will consider that the setting is changed, and will write it to the DDL entry
    # bad_settings_node has implicit_transaction=1, so it will fail and recover from snapshot
    dummy_node.query(
        "CREATE TABLE auto_recovery.t2 (n int) ENGINE=ReplicatedMergeTree ORDER BY tuple()",
        settings={
            "throw_on_unsupported_query_inside_transaction": 1,
            "distributed_ddl_task_timeout": 0,
        },
    )
    dummy_node.query("INSERT INTO auto_recovery.t2 SELECT 137")
    dummy_node.query(
        "EXCHANGE TABLES auto_recovery.t1 AND auto_recovery.t2",
        settings={"distributed_ddl_task_timeout": 0},
    )

    bad_settings_node.query(
        "SYSTEM SYNC DATABASE REPLICA auto_recovery", settings={"receive_timeout": 60}
    )
    assert bad_settings_node.contains_in_log(
        "Unexpected error (3 times in a row), will try to restart main thread"
    )
    assert bad_settings_node.contains_in_log("Cannot begin an implicit transaction")
    bad_settings_node.query("SYSTEM SYNC REPLICA auto_recovery.t1")
    bad_settings_node.query("SYSTEM SYNC REPLICA auto_recovery.t2")

    assert "42\n" == dummy_node.query("SELECT * FROM auto_recovery.t2")
    assert "137\n" == dummy_node.query("SELECT * FROM auto_recovery.t1")

    assert "42\n" == bad_settings_node.query("SELECT * FROM auto_recovery.t2")
    assert "137\n" == bad_settings_node.query("SELECT * FROM auto_recovery.t1")


def test_all_groups_cluster(started_cluster):
    dummy_node.query("DROP DATABASE IF EXISTS db_cluster SYNC")
    bad_settings_node.query("DROP DATABASE IF EXISTS db_cluster SYNC")
    dummy_node.query(
        "CREATE DATABASE db_cluster ENGINE = Replicated('/clickhouse/databases/all_groups_cluster', 'shard1', 'replica1');"
    )
    bad_settings_node.query(
        "CREATE DATABASE db_cluster ENGINE = Replicated('/clickhouse/databases/all_groups_cluster', 'shard1', 'replica2');"
    )

    assert "dummy_node\n" == dummy_node.query(
        "select host_name from system.clusters where name='db_cluster' order by host_name"
    )
    assert "bad_settings_node\n" == bad_settings_node.query(
        "select host_name from system.clusters where name='db_cluster' order by host_name"
    )
    assert "bad_settings_node\ndummy_node\n" == bad_settings_node.query(
        "select host_name from system.clusters where name='all_groups.db_cluster' order by host_name"
    )


def test_detach_attach_table(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS detach_attach_db SYNC")
    main_node.query(
        "CREATE DATABASE detach_attach_db ENGINE = Replicated('/clickhouse/databases/detach_attach_db');"
    )
    main_node.query(
        "CREATE TABLE detach_attach_db.detach_attach_table (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k;"
    )
    main_node.query("INSERT INTO detach_attach_db.detach_attach_table VALUES (1);")
    main_node.query("DETACH TABLE detach_attach_db.detach_attach_table PERMANENTLY;")
    main_node.query("ATTACH TABLE detach_attach_db.detach_attach_table;")
    assert (
        main_node.query("SELECT * FROM detach_attach_db.detach_attach_table;") == "1\n"
    )


def test_alter_rename(started_cluster):
    settings = {
        "distributed_ddl_output_mode": "none",
        "alter_sync": 0,
    }
    res = main_node.query(
        """
        DROP DATABASE IF EXISTS bug SYNC;
        CREATE DATABASE bug ENGINE = Replicated('/clickhouse/databases/bug');
        CREATE TABLE bug.table (`date` DateTime, `id` String) ENGINE = ReplicatedReplacingMergeTree(date) ORDER BY id SETTINGS deduplicate_merge_projection_mode = 'drop';
        ALTER TABLE bug.table ADD PROJECTION max_date (SELECT max(date));
        RENAME TABLE bug.table TO bug.table2;
        SELECT value from system.zookeeper WHERE path = '/clickhouse/databases/bug/metadata';
        """,
        settings=settings,
    )
    assert "PROJECTION" in res


@pytest.mark.parametrize("engine", ["ReplicatedMergeTree"])
def test_create_alter_sleeping(started_cluster, engine):
    competing_node.query("DROP DATABASE IF EXISTS create_alter_sleeping")
    dummy_node.query("DROP DATABASE IF EXISTS create_alter_sleeping")

    competing_node.query(
        "CREATE DATABASE create_alter_sleeping ENGINE = Replicated('/clickhouse/databases/create_alter_sleeping', 'shard1', 'replica1');"
    )
    dummy_node.query(
        "CREATE DATABASE create_alter_sleeping ENGINE = Replicated('/clickhouse/databases/create_alter_sleeping', 'shard1', 'replica2');"
    )

    dummy_node.stop_clickhouse()
    competing_node.query(
        f"""
        CREATE TABLE create_alter_sleeping.t (n int) ENGINE={engine} ORDER BY n;
        ALTER TABLE create_alter_sleeping.t ADD INDEX n_idx n TYPE minmax GRANULARITY 10;
        """,
        settings={"distributed_ddl_task_timeout": 0},
    )

    dummy_node.start_clickhouse()
    assert "n_idx" in dummy_node.query(
        """
        SYSTEM SYNC DATABASE REPLICA create_alter_sleeping;
        -- In case DDL task has been executed on another replica we need to sync the table
        SYSTEM SYNC REPLICA create_alter_sleeping.t;
        SHOW CREATE TABLE create_alter_sleeping.t;
        """, timeout=10
    )

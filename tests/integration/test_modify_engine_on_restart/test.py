import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from test_modify_engine_on_restart.common import (
    check_flags_deleted,
    clear_failpoint,
    get_table_path,
    kill_paused,
    pause_at,
    prepare_versioned_table,
    set_convert_flags,
)

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node2"},
)

database_name = "modify_engine"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def create_tables():
    # MergeTree table that will be converted
    q(
        ch1,
        "CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    q(ch1, "INSERT INTO rmt SELECT number, today(), '' FROM numbers(1e6);")
    q(ch1, "INSERT INTO rmt SELECT number, today()-60, '' FROM numbers(1e5);")

    # ReplacingMergeTree table that will be converted to check unusual engine kinds
    q(
        ch1,
        "CREATE TABLE replacing ( A Int64, D Date, S String ) ENGINE ReplacingMergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    q(ch1, "INSERT INTO replacing SELECT number, today(), '' FROM numbers(1e6);")
    q(ch1, "INSERT INTO replacing SELECT number, today()-60, '' FROM numbers(1e5);")

    # MergeTree table that will not be converted
    q(
        ch1,
        "CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    # Not MergeTree table
    q(ch1, "CREATE TABLE log ( A Int64, D Date, S String ) ENGINE Log;")


def check_tables(converted):
    engine_prefix = ""
    if converted:
        engine_prefix = "Replicated"

    # Check tables exists
    assert (
        q(
            ch1,
            "SHOW TABLES",
        ).strip()
        == "log\nmt\nreplacing\nrmt"
    )

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name != 'log' AND name != 'mt')",
        ).strip()
        == f"replacing\t{engine_prefix}ReplacingMergeTree\nrmt\t{engine_prefix}MergeTree"
    )
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'log' OR name = 'mt')",
        ).strip()
        == "log\tLog\nmt\tMergeTree"
    )

    # Check values
    for table in ["rmt", "replacing"]:
        assert (
            q(
                ch1,
                f"SELECT count() FROM {table}",
            ).strip()
            == "1100000"
        )


def check_replica_added():
    # Add replica to check if zookeeper path is correct and consistent with table uuid

    uuid = q(
        ch1,
        f"SELECT uuid FROM system.tables WHERE table = 'rmt' AND database = '{database_name}'",
    ).strip()

    q(
        ch2,
        f"CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{uuid}/{{shard}}', '{{replica}}') PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    ch2.query(database=database_name, sql="SYSTEM SYNC REPLICA rmt", timeout=20)

    # Check values
    assert (
        q(
            ch2,
            "SELECT count() FROM rmt",
        ).strip()
        == "1100000"
    )


def test_modify_engine_on_restart(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} ON CLUSTER cluster SYNC")
    ch1.query(f"CREATE DATABASE {database_name} ON CLUSTER cluster")

    create_tables()

    check_tables(False)

    ch1.restart_clickhouse()

    check_tables(False)

    set_convert_flags(ch1, database_name, ["rmt", "replacing", "log"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["rmt", "replacing"])

    check_tables(True)

    check_replica_added()

    ch1.restart_clickhouse()

    check_tables(True)

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} ON CLUSTER cluster SYNC")


JOURNAL = "convert_to_replicated_parts.txt"


def ls(node, path):
    return node.exec_in_container(["bash", "-c", f"ls {path}"]).split()


def active_parts(database):
    return ch1.query(
        f"SELECT name FROM system.parts WHERE database = '{database}' AND table = 'mt' AND active ORDER BY name"
    ).split()


def check_converted(database, uuid, zk, rows):
    assert (
        ch1.query(
            f"SELECT engine FROM system.tables WHERE database = '{database}' AND name = 'mt'"
        ).strip()
        == "ReplicatedMergeTree"
    )
    assert ch1.query(f"SELECT count() FROM {database}.mt").strip() == str(rows)
    ch1.query(f"INSERT INTO {database}.mt SELECT number, today() FROM numbers(100)")
    assert ch1.query(f"SELECT count() FROM {database}.mt").strip() == str(rows + 100)

    check_flags_deleted(ch1, database, ["mt"])
    assert (
        ch1.query(
            f"SELECT count() FROM system.detached_parts WHERE database = '{database}' AND table = 'mt'"
        ).strip()
        == "0"
    )

    zk_path = f"/clickhouse/tables/{uuid}/01"
    assert zk.exists(f"{zk_path}/metadata").version == 2
    assert zk.get(f"{zk_path}/replicas/node1/metadata_version")[0] == b"2"


def test_modify_engine_on_restart_resumes_after_kill(started_cluster):
    database = "modify_engine_resume"
    zk = cluster.get_kazoo_client("zoo1")
    uuid = prepare_versioned_table(ch1, zk, database)
    table_path = get_table_path(ch1, "mt", database)

    # The server dies after the parts are moved to detached/, before the table is created in ZooKeeper.
    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_parts")
    kill_paused(ch1)

    # Only the journal is left, so a server that does not know it does not act on this state.
    files = ls(ch1, table_path)
    assert JOURNAL in files
    assert "convert_to_replicated" not in files

    # Without ZooKeeper the table is not served while its parts are in detached/, and the journal is kept.
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(ch1)
        ch1.start_clickhouse(start_wait_sec=150)
        error = ch1.query_and_get_error(f"SELECT count() FROM {database}.mt")
        assert "NO_ZOOKEEPER" in error
        assert JOURNAL in error
        ch1.stop_clickhouse(kill=True)
    assert JOURNAL in ls(ch1, table_path)

    ch1.start_clickhouse()
    check_converted(database, uuid, zk, 200)

    ch1.query(f"DROP DATABASE {database} SYNC")


def test_modify_engine_on_restart_resumes_after_kill_with_force_restore_data(
    started_cluster,
):
    database = "modify_engine_resume_force_restore"
    zk = cluster.get_kazoo_client("zoo1")
    uuid = prepare_versioned_table(ch1, zk, database)
    table_path = get_table_path(ch1, "mt", database)

    # With the global flag, a table without metadata in ZooKeeper restores it when it is attached.
    # The server dies after the first of the two parts is moved to detached/, and the flag is consumed.
    ch1.exec_in_container(["touch", "/var/lib/clickhouse/flags/force_restore_data"])
    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_active_part")
    kill_paused(ch1)
    assert "force_restore_data" not in ls(ch1, "/var/lib/clickhouse/flags")
    assert len(ls(ch1, f"{table_path}detached")) == 1

    ch1.start_clickhouse()
    check_converted(database, uuid, zk, 200)

    ch1.query(f"DROP DATABASE {database} SYNC")


def test_modify_engine_on_restart_resumes_after_kill_before_merge_sources_are_moved(
    started_cluster,
):
    database = "modify_engine_resume_merged"
    ch1.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    ch1.query(f"CREATE DATABASE {database}")
    ch1.query(
        f"CREATE TABLE {database}.mt ( A Int64 ) ENGINE = MergeTree ORDER BY A SETTINGS old_parts_lifetime = 3600"
    )
    ch1.query(f"INSERT INTO {database}.mt SELECT number FROM numbers(100)")
    ch1.query(f"INSERT INTO {database}.mt SELECT number FROM numbers(100)")
    ch1.query(f"OPTIMIZE TABLE {database}.mt FINAL")
    set_convert_flags(ch1, database, ["mt"])
    table_path = get_table_path(ch1, "mt", database)

    # The server dies after the merged part is moved to detached/, while its source all_2_2_0 is still in place,
    # so all_2_2_0 is active again on the next start.
    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_active_part")
    kill_paused(ch1)
    assert "all_2_2_0" in ls(ch1, table_path)
    assert sorted(ls(ch1, f"{table_path}detached")) == ["all_1_2_1", "ignored_all_1_1_0"]

    ch1.start_clickhouse()
    assert (
        ch1.query(
            f"SELECT engine FROM system.tables WHERE database = '{database}' AND name = 'mt'"
        ).strip()
        == "ReplicatedMergeTree"
    )
    assert ch1.query(f"SELECT count() FROM {database}.mt").strip() == "200"
    ch1.query(f"INSERT INTO {database}.mt SELECT number FROM numbers(100)")
    assert ch1.query(f"SELECT count() FROM {database}.mt").strip() == "300"
    check_flags_deleted(ch1, database, ["mt"])

    ch1.query(f"DROP DATABASE {database} SYNC")


def test_modify_engine_on_restart_refuses_after_table_created(started_cluster):
    database = "modify_engine_refuse"
    zk = cluster.get_kazoo_client("zoo1")
    uuid = prepare_versioned_table(ch1, zk, database)
    table_path = get_table_path(ch1, "mt", database)
    parts = active_parts(database)
    assert len(parts) == 2

    # The server dies after the table is created in ZooKeeper, before the parts are attached.
    pause_at(ch1, "rmt_restore_replica_pause_after_creating_table")
    kill_paused(ch1)
    ch1.start_clickhouse()

    error = ch1.query_and_get_error(f"SELECT count() FROM {database}.mt")
    assert "INCORRECT_DATA" in error
    assert JOURNAL in error
    assert JOURNAL in ls(ch1, table_path)
    assert sorted(ls(ch1, f"{table_path}detached")) == parts

    # The recovery the error describes: remove the table from ZooKeeper and restart.
    ch1.stop_clickhouse()
    zk.delete(f"/clickhouse/tables/{uuid}/01", recursive=True)
    ch1.start_clickhouse()
    check_converted(database, uuid, zk, 200)

    ch1.query(f"DROP DATABASE {database} SYNC")


def test_modify_engine_on_restart_keeps_journal_when_part_is_missing(started_cluster):
    database = "modify_engine_missing_part"
    zk = cluster.get_kazoo_client("zoo1")
    uuid = prepare_versioned_table(ch1, zk, database)
    table_path = get_table_path(ch1, "mt", database)
    parts = active_parts(database)
    assert len(parts) == 2

    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_parts")
    kill_paused(ch1)
    ch1.exec_in_container(["rm", "-r", f"{table_path}detached/{parts[0]}"])
    ch1.start_clickhouse()

    error = ch1.query_and_get_error(f"SELECT count() FROM {database}.mt")
    assert "INCORRECT_DATA" in error
    assert parts[0] in error
    assert JOURNAL in ls(ch1, table_path)
    assert zk.exists(f"/clickhouse/tables/{uuid}/01") is None

    ch1.exec_in_container(["rm", f"{table_path}{JOURNAL}"])
    ch1.restart_clickhouse()
    ch1.query(f"DROP DATABASE {database} SYNC")


def test_modify_engine_on_restart_refuses_foreign_detached_part(started_cluster):
    database = "modify_engine_foreign_detached"
    zk = cluster.get_kazoo_client("zoo1")
    uuid = prepare_versioned_table(ch1, zk, database)
    table_path = get_table_path(ch1, "mt", database)
    parts = active_parts(database)
    assert len(parts) == 2

    # A detached part with the name of an active one, which the move would sidestep with a `_try1` suffix.
    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {table_path}detached && cp -r {table_path}{parts[0]} {table_path}detached/{parts[0]}",
        ]
    )
    ch1.restart_clickhouse()

    error = ch1.query_and_get_error(f"SELECT count() FROM {database}.mt")
    assert "INCORRECT_DATA" in error
    assert parts[0] in error
    files = ls(ch1, table_path)
    assert "convert_to_replicated" in files
    assert JOURNAL not in files
    assert parts[0] in files and parts[1] in files
    assert ls(ch1, f"{table_path}detached") == [parts[0]]
    assert zk.exists(f"/clickhouse/tables/{uuid}/01") is None

    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"rm -r {table_path}detached/{parts[0]} {table_path}convert_to_replicated",
        ]
    )
    ch1.restart_clickhouse()
    ch1.query(f"DROP DATABASE {database} SYNC")


def test_modify_engine_on_restart_refuses_table_created_concurrently(started_cluster):
    database = "modify_engine_concurrent"
    zk = cluster.get_kazoo_client("zoo1")
    uuid = prepare_versioned_table(ch1, zk, database)
    table_path = get_table_path(ch1, "mt", database)
    ch2.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    ch2.query(f"CREATE DATABASE {database}")

    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_parts")
    clear_failpoint(ch1)
    ch2.query(
        f"CREATE TABLE {database}.mt ( A Int64, D Date ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{{shard}}', '{{replica}}') PARTITION BY toYYYYMM(D) ORDER BY A"
    )
    ch1.query("SYSTEM DISABLE FAILPOINT rmt_restore_replica_pause_after_detaching_parts")

    error = ch1.query_and_get_error(f"SELECT count() FROM {database}.mt")
    assert "UNEXPECTED_NODE_IN_ZOOKEEPER" in error
    assert JOURNAL in ls(ch1, table_path)
    assert zk.exists(f"/clickhouse/tables/{uuid}/01/replicas/node1") is None

    # Once the path is free again, the next start completes the conversion.
    ch2.query(f"DROP DATABASE {database} SYNC")
    ch1.restart_clickhouse()
    assert ch1.query(f"SELECT count() FROM {database}.mt").strip() == "200"
    check_flags_deleted(ch1, database, ["mt"])

    ch1.query(f"DROP DATABASE {database} SYNC")

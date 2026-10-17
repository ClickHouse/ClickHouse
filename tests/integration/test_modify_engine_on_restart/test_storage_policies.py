import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import (
    check_flags_deleted,
    get_table_path,
    kill_paused,
    pause_at,
    set_convert_flags,
)

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
        "configs/config.d/storage_policies.xml",
    ],
    with_zookeeper=True,
    with_minio=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_storage_policies"


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
    # Implicit jbod (set default in config)
    q(
        ch1,
        "CREATE TABLE jbod_imp ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    # Explicit jbod
    q(
        ch1,
        """
        CREATE TABLE jbod_exp ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A
        SETTINGS storage_policy='jbod';
        """,
    )

    # s3
    q(
        ch1,
        """
        CREATE TABLE s3 ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A
        SETTINGS storage_policy='s3';
        """,
    )

    # Default
    q(
        ch1,
        """
        CREATE TABLE default ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A
        SETTINGS storage_policy='default';
        """,
    )


def check_tables(converted):
    engine_prefix = ""
    if converted:
        engine_prefix = "Replicated"

    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}'",
        ).strip()
        == f"default\t{engine_prefix}MergeTree\njbod_exp\t{engine_prefix}MergeTree\njbod_imp\t{engine_prefix}MergeTree\ns3\t{engine_prefix}MergeTree"
    )


def test_modify_engine_on_restart(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    create_tables()

    check_tables(False)

    ch1.restart_clickhouse()

    check_tables(False)

    set_convert_flags(ch1, database_name, ["default", "jbod_exp", "jbod_imp", "s3"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["default", "jbod_exp", "jbod_imp", "s3"])

    check_tables(True)

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_modify_engine_on_restart_keeps_journal_when_retry_is_interrupted(started_cluster):
    database = "modify_engine_s3_retry"
    ch1.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    ch1.query(f"CREATE DATABASE {database}")
    ch1.query(
        f"CREATE TABLE {database}.mt ( A Int64 ) ENGINE = MergeTree ORDER BY A SETTINGS storage_policy = 's3'"
    )
    ch1.query(f"INSERT INTO {database}.mt SELECT number FROM numbers(100)")
    ch1.query(f"INSERT INTO {database}.mt SELECT number FROM numbers(100)")
    set_convert_flags(ch1, database, ["mt"])
    journal = get_table_path(ch1, "mt", database) + "convert_to_replicated_parts.txt"

    def journal_inode():
        return ch1.exec_in_container(["stat", "-c", "%i", journal]).strip()

    # Replacing a file is not atomic on an object storage disk, so a retry must not replace the journal:
    # killed in between, it would leave no journal while the parts are in detached/.
    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_parts")
    kill_paused(ch1)
    inode = journal_inode()
    pause_at(ch1, "rmt_restore_replica_pause_after_detaching_parts")
    kill_paused(ch1)
    assert journal_inode() == inode

    ch1.start_clickhouse()
    assert (
        ch1.query(
            f"SELECT engine FROM system.tables WHERE database = '{database}' AND name = 'mt'"
        ).strip()
        == "ReplicatedMergeTree"
    )
    assert ch1.query(f"SELECT count() FROM {database}.mt").strip() == "200"
    check_flags_deleted(ch1, database, ["mt"])

    ch1.query(f"DROP DATABASE {database} SYNC")

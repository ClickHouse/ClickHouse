import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, move_file
from test_modify_engine_on_restart.common import (
    check_flags_deleted,
    get_table_path,
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

database_name = "modify_engine_metadata_file_name"

# A metadata file name for `t1` that ClickHouse itself cannot write: `escapeForFileName` never emits
# an escape for a word character, so it produces `t1.sql`, while unescaping this name yields `t1`
# too. Any name whose escape round trip is not the identity puts the file somewhere the by-name
# lookup does not look.
MISMATCHED_FILE_NAME = "%74%31.sql"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return ch1.query(database=database_name, sql=query)


def metadata_dir_listing(directory):
    """Names in the database's metadata directory, read through the database disk.

    Read through `clickhouse-disks` rather than with `ls`, because the database disk is remote in
    some configurations and the directory is then not on the container filesystem at all.
    """
    disk = get_database_disk_name(ch1)
    return ch1.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml"
            f" --disk {disk} --save-logs --query 'list --path {directory}'",
        ]
    ).split()


def engine_of(table):
    return q(
        "SELECT engine FROM system.tables"
        f" WHERE database = '{database_name}' AND name = '{table}'"
    ).strip()


def test_convert_flag_with_mismatched_metadata_file_name(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q("CREATE TABLE t1 ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q("INSERT INTO t1 VALUES (1), (2)")
    # `t2` keeps its canonical file name and acts as the control below.
    q("CREATE TABLE t2 ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q("INSERT INTO t2 VALUES (3)")

    set_convert_flags(ch1, database_name, ["t1", "t2"])
    # Arming assertion: without a flag the startup path returns before converting anything, and the
    # engine assertions below then hold whether or not the conversion ran.
    for table in ["t1", "t2"]:
        assert "convert_to_replicated" in ch1.exec_in_container(
            ["bash", "-c", f"ls {get_table_path(ch1, table, database_name)}"]
        )

    # Read while the server is up: every helper here answers from a query, and between the stop and
    # the start below there is provably no server to answer one.
    metadata_dir = (
        ch1.query(
            f"SELECT metadata_path FROM system.databases WHERE name = '{database_name}'"
        )
        .strip()
        .rstrip("/")
    )
    t1_metadata_path = q(
        "SELECT metadata_path FROM system.tables"
        f" WHERE database = '{database_name}' AND name = 't1'"
    ).strip()

    ch1.stop_clickhouse()
    move_file(ch1, t1_metadata_path, f"{metadata_dir}/{MISMATCHED_FILE_NAME}")

    # Arming assertions, both required: the name-derived lookup has to miss. Without the second one
    # the file it derives is still in place, and the scenario passes on unfixed code.
    listing = metadata_dir_listing(metadata_dir)
    assert MISMATCHED_FILE_NAME in listing
    assert "t1.sql" not in listing

    # The line that reddens on unfixed code: startup dereferences a null create query and aborts, so
    # the server never comes up.
    ch1.start_clickhouse()

    assert engine_of("t1") == "ReplicatedMergeTree"
    assert q("SELECT count() FROM t1").strip() == "2"
    # Control: the same server, restart and configuration convert a canonically named table, so the
    # scenario is about the file name and not about the conversion route.
    assert engine_of("t2") == "ReplicatedMergeTree"
    assert q("SELECT count() FROM t2").strip() == "1"

    check_flags_deleted(ch1, database_name, ["t1", "t2"])

    # The conversion rewrote the metadata file that startup was iterating, not the name-derived path,
    # which also keeps the arming assertions above honest across the restart.
    listing = metadata_dir_listing(metadata_dir)
    assert MISMATCHED_FILE_NAME in listing
    assert "t1.sql" not in listing

    # `DROP` resolves a table's metadata file from its name as well, so the canonical name has to be
    # back before the teardown below, and before a repeated run reaches the leading `DROP` again.
    ch1.stop_clickhouse()
    move_file(ch1, f"{metadata_dir}/{MISMATCHED_FILE_NAME}", t1_metadata_path)
    ch1.start_clickhouse()

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")

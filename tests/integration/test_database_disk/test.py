import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/database_disk.xml"],
    with_remote_database_disk=False,
)

# Database metadata on a `plain_rewritable` disk, where moving a file is a copy followed by a removal.
node_pr = cluster.add_instance(
    "node_pr",
    with_remote_database_disk=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_rename_database_with_database_disk(started_cluster):
    db_disk_path = node1.query(
        "SELECT path FROM system.disks WHERE name='custom_db_disk'"
    ).strip()

    def list_file_in_metadata_dir():
        return node1.exec_in_container(
            [
                "bash",
                "-c",
                f'ls "{db_disk_path}/metadata/"',
            ],
            privileged=True,
            user="root",
        )

    node1.query("DROP DATABASE IF EXISTS test SYNC")
    node1.query("CREATE DATABASE test ENGINE=Atomic")

    node1.query("RENAME DATABASE test TO test_rename")
    metadata_files = list_file_in_metadata_dir()
    assert "test.sql" not in metadata_files
    assert "test_rename.sql" in metadata_files

    node1.query("RENAME DATABASE test_rename TO test")
    metadata_files = list_file_in_metadata_dir()
    assert "test.sql" in metadata_files
    assert "test_rename.sql" not in metadata_files

    node1.query("DROP DATABASE IF EXISTS test SYNC")


def run_disks_query(query):
    return node_pr.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml "
            f"--disk {get_database_disk_name(node_pr)} --save-logs --query '{query}'",
        ]
    )


def kill_before_publishing_rename(db, table="t", replace_existing=False):
    """Kills the server inside `CREATE OR REPLACE TABLE {db}.{table}`, after the temporary table is created
    and filled and before it replaces `{table}`. Returns the temporary table's name, UUID and metadata path.
    """
    node_pr.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    node_pr.query(f"CREATE DATABASE {db} ENGINE = Atomic")
    if replace_existing:
        node_pr.query(
            f"CREATE TABLE {db}.{table} (x UInt64) ENGINE = MergeTree ORDER BY x"
        )
    # Creates the `system.all_*` union tables, so that the kill does not interrupt their creation.
    node_pr.query("SYSTEM FLUSH LOGS")
    node_pr.query("SYSTEM ENABLE FAILPOINT create_or_replace_before_rename")
    request = node_pr.get_query_request(
        f"CREATE OR REPLACE TABLE {db}.{table} (x UInt64) ENGINE = MergeTree ORDER BY x "
        "AS SELECT number FROM numbers(100)"
    )
    node_pr.query("SYSTEM WAIT FAILPOINT create_or_replace_before_rename PAUSE")
    name, uuid, metadata_path = (
        node_pr.query(
            "SELECT name, uuid, metadata_path FROM system.tables "
            f"WHERE database = '{db}' AND startsWith(name, '_tmp_replace_') AND name != '{table}'"
        )
        .strip()
        .split("\t")
    )
    node_pr.stop_clickhouse(kill=True)
    request.get_answer_and_error()
    return name, uuid, metadata_path


def copy_metadata_file(path_from, path_to):
    """Performs the first step of a move of a metadata file on the `plain_rewritable` disk:
    a kill between the copy and the removal of the source leaves both files."""
    run_disks_query(f"copy {path_from} {path_to}")
    content = run_disks_query(f"read {path_from}")
    assert content.startswith("ATTACH TABLE _ UUID")
    assert run_disks_query(f"read {path_to}") == content


@pytest.mark.parametrize(
    "db, table",
    [
        ("db_interrupted_rename", "t"),
        ("db_interrupted_rename_tmp_name", "_tmp_replace_abc_def"),
    ],
)
def test_interrupted_rename_of_temporary_table(started_cluster, db, table):
    tmp_name, tmp_uuid, tmp_path = kill_before_publishing_rename(db, table)
    db_dir = os.path.dirname(tmp_path)
    copy_metadata_file(tmp_path, f"{db_dir}/{table}.sql")

    node_pr.start_clickhouse()

    assert node_pr.query(f"SELECT count() FROM {db}.{table}") == "100\n"
    assert (
        node_pr.query(f"SELECT name, uuid FROM system.tables WHERE database = '{db}'")
        == f"{table}\t{tmp_uuid}\n"
    )
    assert tmp_name not in run_disks_query(f"ls {db_dir}")
    assert node_pr.contains_in_log(
        f"Removing {tmp_path}: it is a copy of {db_dir}/{table}.sql"
    )
    node_pr.query(f"DROP DATABASE {db} SYNC")


def test_interrupted_drop_of_temporary_table(started_cluster):
    db = "db_interrupted_drop"
    tmp_name, tmp_uuid, tmp_path = kill_before_publishing_rename(db)
    dropped_path = f"metadata_dropped/{db}.{tmp_name}.{tmp_uuid}.sql"
    run_disks_query("mkdir --parents metadata_dropped")
    copy_metadata_file(tmp_path, dropped_path)

    node_pr.start_clickhouse()

    assert (
        node_pr.query(f"SELECT count() FROM system.tables WHERE database = '{db}'")
        == "0\n"
    )
    assert tmp_name not in run_disks_query(f"ls {os.path.dirname(tmp_path)}")
    assert node_pr.contains_in_log(
        f"Removing {tmp_path}: it is a copy of {dropped_path}"
    )
    node_pr.query(f"DROP DATABASE {db} SYNC")


def test_copy_of_temporary_table_under_unrelated_name(started_cluster):
    db = "db_unrelated_copy"
    # The temporary table is named after `t`, but `t` is another table, so `t.sql` is not its copy.
    tmp_name, tmp_uuid, tmp_path = kill_before_publishing_rename(
        db, replace_existing=True
    )
    db_dir = os.path.dirname(tmp_path)
    copy_metadata_file(tmp_path, f"{db_dir}/u.sql")

    # Not a leftover of a move of the temporary table, so it is still a UUID collision.
    node_pr.start_clickhouse(expected_to_fail=True)
    assert node_pr.contains_in_log(
        f"Mapping for table with UUID={tmp_uuid} already exists",
        filename="clickhouse-server.err.log",
    )

    run_disks_query(f"remove {db_dir}/u.sql")
    node_pr.start_clickhouse()

    assert (
        node_pr.query(
            f"SELECT name FROM system.tables WHERE database = '{db}' ORDER BY name"
        )
        == f"{tmp_name}\nt\n"
    )
    node_pr.query(f"DROP DATABASE {db} SYNC")

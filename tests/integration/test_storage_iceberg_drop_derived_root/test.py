"""
`DROP` must not delete data when the Iceberg table root had to be derived from a metadata document
deeper than the queried path: the queried path is then an ancestor of the table directory and covers
whatever else lives under it, so deleting everything below it would take other tables with it.

Covers the skip added by https://github.com/ClickHouse/ClickHouse/pull/117037. It cannot be a
stateless test, because `StorageObjectStorage::drop` deliberately reads `iceberg_delete_data_on_drop`
from the global context (the drop runs in the background), so the setting has to come from a profile.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

USER_FILES = "/var/lib/clickhouse/user_files"


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node1",
        user_configs=["configs/users.d/iceberg.xml"],
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def list_files(instance, path):
    return instance.exec_in_container(
        ["bash", "-c", f"find {path} -type f 2>/dev/null | sort"], user="root"
    ).strip()


def test_drop_keeps_data_when_table_root_is_derived(started_cluster):
    instance = started_cluster.instances["node1"]
    prefix = f"{USER_FILES}/iceberg_drop_derived_{uuid.uuid4().hex}"
    table = "t_derived_root"

    # The real table lives one directory below the path the query will name.
    instance.query(
        f"CREATE TABLE t_real (x UInt32) ENGINE = IcebergLocal('{prefix}/tbl/sub/', 'Parquet')"
    )
    instance.query("INSERT INTO t_real VALUES (1), (2), (3)")
    # Detached rather than dropped: dropping it would delete the very data this test is about.
    instance.query("DETACH TABLE t_real")

    # A neighbour of the table: reachable from the queried path, not part of the table.
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {prefix}/tbl/sibling && echo keep > {prefix}/tbl/sibling/keep.txt",
        ],
        user="root",
    )

    before = list_files(instance, f"{prefix}/tbl")
    assert f"{prefix}/tbl/sibling/keep.txt" in before

    instance.query(
        f"""
        CREATE TABLE {table} ENGINE = IcebergLocal('{prefix}/tbl/', 'Parquet')
        SETTINGS iceberg_metadata_file_path = 'sub/metadata/v2.metadata.json'
        """
    )

    # Also proves the root really was derived: rooted at the queried path, every data key would
    # lose its `sub/` component and the read would fail with a missing file.
    assert instance.query(f"SELECT sum(x) FROM {table}").strip() == "6"

    instance.query(f"DROP TABLE {table} SYNC")

    assert list_files(instance, f"{prefix}/tbl") == before
    assert instance.contains_in_log("Keeping the data of the Iceberg table at")


def test_drop_deletes_data_when_table_root_is_the_queried_path(started_cluster):
    """Control: the deletion the test above asserts against is live for an ordinary table."""
    instance = started_cluster.instances["node1"]
    prefix = f"{USER_FILES}/iceberg_drop_plain_{uuid.uuid4().hex}"

    instance.query(
        f"CREATE TABLE t_plain (x UInt32) ENGINE = IcebergLocal('{prefix}/tbl/', 'Parquet')"
    )
    instance.query("INSERT INTO t_plain VALUES (1), (2), (3)")
    assert instance.query("SELECT sum(x) FROM t_plain").strip() == "6"
    assert list_files(instance, f"{prefix}/tbl") != ""

    instance.query("DROP TABLE t_plain SYNC")

    assert list_files(instance, f"{prefix}/tbl") == ""

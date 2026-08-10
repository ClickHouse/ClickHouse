import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

BUFFER_ARGS = "'db', 'dst', 1, 10, 100, 10000, 1000000, 10000000, 100000000"
METADATA = "/var/lib/clickhouse/metadata/db/b.sql"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_buffer_with_omitted_columns_loads_from_metadata_without_column_list(
    started_cluster,
):
    """A Buffer whose stored definition has no column list resolves the destination's columns on load.

    Metadata written before 22.x stores no column list for engines that infer their structure, so the
    server must still be able to attach such a table. Newer versions persist the inferred list, hence
    the list is stripped here to produce the older shape.
    """
    node.query("DROP DATABASE IF EXISTS db SYNC")
    node.query("CREATE DATABASE db")
    node.query("CREATE TABLE db.dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
    node.query(f"CREATE TABLE db.b ENGINE = Buffer({BUFFER_ARGS})")

    columns_query = (
        "SELECT name, type FROM system.columns "
        "WHERE database = 'db' AND table = 'b' ORDER BY position"
    )
    expected = TSV("a\tUInt64\nb\tString\n")
    assert TSV(node.query(columns_query)) == expected

    node.query("DETACH TABLE db.b")
    node.exec_in_container(
        ["bash", "-c", f"grep -v '^(\\|^)\\|^    `' {METADATA} > {METADATA}.new"]
    )
    node.exec_in_container(["bash", "-c", f"mv {METADATA}.new {METADATA}"])
    stored = node.exec_in_container(["bash", "-c", f"cat {METADATA}"])
    assert "UInt64" not in stored, f"the column list was not stripped: {stored}"
    assert "ENGINE = Buffer" in stored, f"the engine definition was lost: {stored}"

    node.restart_clickhouse()

    assert TSV(node.query(columns_query)) == expected

    node.query("INSERT INTO db.b VALUES (7, 'x')")
    assert TSV(node.query("SELECT a, b FROM db.b")) == TSV("7\tx\n")

    node.query("DROP DATABASE db SYNC")

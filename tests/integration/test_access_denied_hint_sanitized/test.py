import pytest

import re

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def extract_access_denied_hint(err: str) -> str:
    match = re.search(r"Not enough privileges\.[\s\S]*", err)
    if not match:
        return err
    hint = match.group(0)
    for marker in ("Stack trace:", " (query:", "\nQuery:"):
        pos = hint.find(marker)
        if pos != -1:
            hint = hint[:pos]
    return hint

def test_hint_filters_columns_by_show_privilege():
    node.query("DROP TABLE IF EXISTS t_cols")
    node.query(
        "CREATE TABLE t_cols(col_alpha UInt32, col_beta UInt32) ENGINE = Memory"
    )
    node.query("CREATE USER OR REPLACE u_cols")
    node.query("GRANT SHOW TABLES ON default.t_cols TO u_cols")
    node.query("GRANT SHOW COLUMNS(col_alpha) ON default.t_cols TO u_cols")
    node.query("GRANT SELECT(col_alpha) ON default.t_cols TO u_cols")

    err = node.query_and_get_error(
        "SELECT col_alpha, col_beta FROM t_cols", user="u_cols"
    )
    hint = extract_access_denied_hint(err)
    assert "necessary to have the grant SELECT" in hint
    assert "Missing permissions" in hint
    assert "t_cols" in hint
    assert "col_alpha" not in hint
    assert "col_beta" not in hint


def test_hint_keeps_table_name_from_query():
    node.query("DROP DATABASE IF EXISTS db_hint")
    node.query("CREATE DATABASE db_hint")
    node.query("DROP TABLE IF EXISTS db_hint.t_hidden")
    node.query("CREATE TABLE db_hint.t_hidden(x UInt32) ENGINE = Memory")
    node.query("CREATE USER OR REPLACE u_table")

    err = node.query_and_get_error(
        "SELECT * FROM db_hint.t_hidden", user="u_table"
    )
    hint = extract_access_denied_hint(err)
    assert "necessary to have the grant SELECT" in hint
    assert "db_hint" in hint
    assert "t_hidden" in hint


def test_hint_keeps_database_name_from_query():
    node.query("CREATE USER OR REPLACE u_db")

    err = node.query_and_get_error("CREATE DATABASE db_no_show", user="u_db")
    hint = extract_access_denied_hint(err)
    assert "necessary to have the grant CREATE DATABASE" in hint
    assert "db_no_show" in hint


def test_hint_hides_system_clusters_columns_without_show():
    node.query("CREATE USER OR REPLACE u_system")

    err = node.query_and_get_error("SELECT * FROM system.clusters", user="u_system")
    hint = extract_access_denied_hint(err)
    assert "necessary to have the grant SELECT" in hint
    assert "system.clusters" in hint
    assert "shard_num" not in hint

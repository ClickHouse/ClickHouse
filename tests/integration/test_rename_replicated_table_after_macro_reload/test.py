import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", with_zookeeper=True, stay_alive=True, macros={"prefix": "/clickhouse/tables/db1"}
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_macros(macros):
    # The harness writes its macros, `{instance}` among them, into conf.d/macros.xml; rewrite that same file.
    full = {"instance": node.name}
    full.update(macros)
    node.replace_config(
        "/etc/clickhouse-server/conf.d/macros.xml",
        "<clickhouse><macros>"
        + "".join(f"<{k}>{v}</{k}>" for k, v in full.items())
        + "</macros></clickhouse>",
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_rename_after_macro_reload(started_cluster):
    node.query("CREATE DATABASE db1 ENGINE = Atomic")
    node.query(
        "CREATE TABLE db1.t (x UInt64) ENGINE = ReplicatedMergeTree('{prefix}/t', 'r1') ORDER BY x"
    )
    node.query("INSERT INTO db1.t VALUES (1)")

    # The macro now binds the database name. The table was loaded with the literal, so its path is unchanged,
    # but the next load would expand the new name.
    set_macros({"prefix": "/clickhouse/tables/{database}"})
    assert "NOT_IMPLEMENTED" in node.query_and_get_error("RENAME DATABASE db1 TO db2")
    assert "NOT_IMPLEMENTED" in node.query_and_get_error("RENAME TABLE db1.t TO db1.t2")
    assert node.query("SELECT count() FROM db1.t") == "1\n"

    # With the literal back, both renames go through and the table keeps its path across a restart.
    set_macros({"prefix": "/clickhouse/tables/db1"})
    node.query("RENAME TABLE db1.t TO db1.t2")
    node.query("RENAME DATABASE db1 TO db2")
    node.restart_clickhouse()
    assert node.query("SELECT count() FROM db2.t2") == "1\n"
    assert (
        node.query("SELECT is_readonly FROM system.replicas WHERE database = 'db2' AND table = 't2'")
        == "0\n"
    )

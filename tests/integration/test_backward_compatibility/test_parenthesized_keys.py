"""Tables whose keys and other definition expressions are written with redundant parentheses
(`PARTITION BY (a)`, `ORDER BY (b, c)`, `DEFAULT (a + 1)`, ...) must stay interchangeable with
older versions, which always stored the canonical form without the parentheses
(`PARTITION BY a`). https://github.com/ClickHouse/ClickHouse/pull/92340 started to preserve the
parentheses, so metadata comparisons against tables created by older versions failed with
exceptions like "Tables have different partition key" or METADATA_MISMATCH.

With normalization on AST read into the storage descriptions, the stored and compared metadata
is canonical, while `SHOW CREATE` keeps the query as written (with the parentheses).
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_old = cluster.add_instance(
    "node_old",
    image="clickhouse/clickhouse-server",
    tag="26.4",
    stay_alive=True,
    with_zookeeper=True,
    with_installed_binary=True,
)
node_new = cluster.add_instance(
    "node_new",
    with_zookeeper=True,
    stay_alive=True,
)

CREATE_TEMPLATE = """
    CREATE TABLE {table} (
        a UInt32,
        b UInt32,
        c UInt32,
        d DateTime,
        i UInt32 DEFAULT (a + 1),
        INDEX ix (b * c) TYPE minmax GRANULARITY 1,
        CONSTRAINT cc CHECK (a > 0),
        PROJECTION pr (SELECT (b), (c + 1) ORDER BY (c)),
        PROJECTION pri INDEX (b) TYPE basic
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}')
    PARTITION BY (a)
    PRIMARY KEY (b)
    ORDER BY (b, c)
    TTL (d + INTERVAL 10 YEAR)
"""


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_replication(table):
    node_old.query(
        f"INSERT INTO {table} (a, b, c, d) VALUES (1, 1, 1, now()), (2, 2, 2, now())"
    )
    node_new.query(f"SYSTEM SYNC REPLICA {table}", timeout=30)
    assert node_new.query(f"SELECT a, b, c FROM {table} ORDER BY a") == "1\t1\t1\n2\t2\t2\n"

    node_new.query(f"INSERT INTO {table} (a, b, c, d) VALUES (3, 3, 3, now())")
    node_old.query(f"SYSTEM SYNC REPLICA {table}", timeout=30)
    assert node_old.query(f"SELECT count() FROM {table}") == "3\n"


def test_new_replica_joins_old_table(start_cluster):
    """The table is created by the old version (canonical metadata in ZooKeeper), then the
    current version joins with the same parenthesized DDL. Without normalization the new
    replica fails to create with METADATA_MISMATCH."""
    node_old.query(CREATE_TEMPLATE.format(table="t_parens_old_first", replica="r1"))
    node_new.query(CREATE_TEMPLATE.format(table="t_parens_old_first", replica="r2"))

    check_replication("t_parens_old_first")

    # The stored and compared metadata is canonical, while the query text is kept as written.
    assert (
        node_new.query(
            "SELECT partition_key, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_parens_old_first'"
        )
        == "a\tb, c\n"
    )
    assert "PARTITION BY (a)" in node_new.query("SHOW CREATE TABLE t_parens_old_first")


def test_old_replica_joins_new_table(start_cluster):
    """The table is created by the current version first: the metadata written to ZooKeeper
    must keep the canonical form so that the old version can join."""
    node_new.query(CREATE_TEMPLATE.format(table="t_parens_new_first", replica="r1"))
    node_old.query(CREATE_TEMPLATE.format(table="t_parens_new_first", replica="r2"))

    check_replication("t_parens_new_first")


def test_upgrade_and_attach_partition_from(start_cluster):
    """A table created by the old version must load after an upgrade and be compatible with a
    freshly created parenthesized table in `ATTACH PARTITION FROM`. Must be the last test in
    the module: it replaces the old binary on `node_old` with the current one."""
    node_old.query(
        """
        CREATE TABLE t_parens_src (a UInt32, b UInt32)
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (b)
        """
    )
    node_old.query("INSERT INTO t_parens_src VALUES (1, 1), (1, 2), (2, 1)")

    node_old.restart_with_latest_version()

    node_old.query(
        """
        CREATE TABLE t_parens_dst (a UInt32, b UInt32)
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (b)
        """
    )
    node_old.query("ALTER TABLE t_parens_dst ATTACH PARTITION 1 FROM t_parens_src")
    assert node_old.query("SELECT a, b FROM t_parens_dst ORDER BY a, b") == "1\t1\n1\t2\n"

    # The replicated table created by the old version must still work after the upgrade.
    node_old.query("SYSTEM SYNC REPLICA t_parens_old_first", timeout=30)
    assert node_old.query("SELECT count() FROM t_parens_old_first") == "3\n"

"""Tables whose keys and other definition expressions are written with redundant parentheses
(`PARTITION BY (a)`, `ORDER BY (b, c)`, `DEFAULT (a + 1)`, ...) must stay interchangeable with
older versions, which always stored the canonical form without the parentheses
(`PARTITION BY a`). https://github.com/ClickHouse/ClickHouse/pull/92340 started to preserve the
parentheses in stored table metadata, so metadata comparisons against tables created by older
versions failed with exceptions like "Tables have different partition key" or METADATA_MISMATCH.
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
    current version joins with the same parenthesized DDL. Without canonicalization the new
    replica fails to create with METADATA_MISMATCH."""
    node_old.query(CREATE_TEMPLATE.format(table="t_parens_old_first", replica="r1"))
    node_new.query(CREATE_TEMPLATE.format(table="t_parens_old_first", replica="r2"))

    check_replication("t_parens_old_first")

    create = node_new.query("SHOW CREATE TABLE t_parens_old_first")
    assert "PARTITION BY a" in create
    assert "PARTITION BY (a)" not in create
    assert "PRIMARY KEY b" in create
    assert "ORDER BY (b, c)" in create


def test_old_replica_joins_new_table(start_cluster):
    """The table is created by the current version first: the metadata written to ZooKeeper
    must keep the canonical form so that the old version can join."""
    node_new.query(CREATE_TEMPLATE.format(table="t_parens_new_first", replica="r1"))
    node_old.query(CREATE_TEMPLATE.format(table="t_parens_new_first", replica="r2"))

    check_replication("t_parens_new_first")


def test_alter_writes_canonical_metadata_to_zookeeper(start_cluster):
    """A replicated ALTER must write the same backward-compatible canonical metadata to ZooKeeper
    that CREATE writes, so the stored form stays comparable with older versions. The ALTER write
    path used the raw serializer and persisted the parenthesized form that #92340 preserves
    (`SAMPLE BY (a)` -> `sampling expression: (a)`, `CHECK (a > 0)` -> `constraints: cc CHECK (a > 0)`),
    while CREATE and the old version store the canonical form (`a`, `cc CHECK a > 0`)."""

    def zk_metadata(node, table):
        return node.query(
            "SELECT value FROM system.zookeeper "
            f"WHERE path = '/clickhouse/tables/{table}' AND name = 'metadata'"
        )

    # Reach the schema two ways: everything via CREATE, or a minimal table plus ALTERs.
    node_new.query(
        """
        CREATE TABLE t_parens_created (a UInt32, b UInt32, c UInt32, d DateTime,
            CONSTRAINT cc CHECK (a > 0))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_parens_created', 'r1')
        ORDER BY (b) SAMPLE BY (b) TTL (d + INTERVAL 10 YEAR)
        """
    )
    node_new.query(
        """
        CREATE TABLE t_parens_altered (a UInt32, b UInt32, c UInt32, d DateTime)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_parens_altered', 'r1')
        ORDER BY (b)
        """
    )
    node_new.query("ALTER TABLE t_parens_altered MODIFY SAMPLE BY (b)")
    node_new.query("ALTER TABLE t_parens_altered MODIFY TTL (d + INTERVAL 10 YEAR)")
    node_new.query("ALTER TABLE t_parens_altered ADD CONSTRAINT cc CHECK (a > 0)")

    # The ALTER path must store the same canonical metadata as CREATE (no redundant parentheses).
    assert zk_metadata(node_new, "t_parens_altered") == zk_metadata(
        node_new, "t_parens_created"
    )

    # The old version joins the table whose metadata was written entirely by ALTER.
    node_old.query(
        """
        CREATE TABLE t_parens_altered (a UInt32, b UInt32, c UInt32, d DateTime,
            CONSTRAINT cc CHECK (a > 0))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_parens_altered', 'r2')
        ORDER BY (b) SAMPLE BY (b) TTL (d + INTERVAL 10 YEAR)
        """
    )
    check_replication("t_parens_altered")


def test_upgrade_and_attach_partition_from(start_cluster):
    """A table created by the old version must load after an upgrade, keep the canonical
    formatting, and be compatible with a freshly created parenthesized table in
    `ATTACH PARTITION FROM`. Must be the last test in the module: it replaces the old binary
    on `node_old` with the current one."""
    node_old.query(
        """
        CREATE TABLE t_parens_src (a UInt32, b UInt32)
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (b)
        """
    )
    node_old.query("INSERT INTO t_parens_src VALUES (1, 1), (1, 2), (2, 1)")

    # A table with a secondary index and a projection created by the OLD version: it stores the
    # canonical form without the redundant parentheses (`INDEX ix b * c`, `PROJECTION p (SELECT b ...)`).
    node_old.query(
        """
        CREATE TABLE t_parens_defs_src (a UInt32, b UInt32, c UInt32,
            INDEX ix (b * c) TYPE minmax GRANULARITY 1,
            PROJECTION p (SELECT (b), sum(c) GROUP BY (b)))
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (a)
        """
    )
    node_old.query("INSERT INTO t_parens_defs_src VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3)")

    node_old.restart_with_latest_version()

    create = node_old.query("SHOW CREATE TABLE t_parens_src")
    assert "PARTITION BY a" in create
    assert "ORDER BY b" in create

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

    # The index/projection stored by the old version (canonical, no redundant parentheses) must be
    # interchangeable in `ATTACH PARTITION FROM` with a table freshly created by the current version,
    # whose `INDEX ix (b * c)` / `PROJECTION p (SELECT (b) ...)` keeps the parentheses #92340 preserves.
    # The structure gate must compare them by their backward-compatible canonical form.
    node_old.query(
        """
        CREATE TABLE t_parens_defs_dst (a UInt32, b UInt32, c UInt32,
            INDEX ix (b * c) TYPE minmax GRANULARITY 1,
            PROJECTION p (SELECT (b), sum(c) GROUP BY (b)))
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (a)
        """
    )
    node_old.query("ALTER TABLE t_parens_defs_dst ATTACH PARTITION 1 FROM t_parens_defs_src")
    assert (
        node_old.query("SELECT a, b, c FROM t_parens_defs_dst ORDER BY a, b, c")
        == "1\t1\t1\n1\t2\t2\n"
    )

    # The replicated table created by the old version must still work after the upgrade.
    node_old.query("SYSTEM SYNC REPLICA t_parens_old_first", timeout=30)
    assert node_old.query("SELECT count() FROM t_parens_old_first") == "3\n"

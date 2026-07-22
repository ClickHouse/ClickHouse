"""Tables whose keys and other definition expressions are written with redundant parentheses
(`PARTITION BY (a)`, `ORDER BY (b, c)`, `DEFAULT (a + 1)`, ...) must stay interchangeable with
older versions, which always stored the canonical form without the parentheses
(`PARTITION BY a`). https://github.com/ClickHouse/ClickHouse/pull/92340 started to preserve the
parentheses in stored table metadata, so metadata comparisons against tables created by older
versions failed with exceptions like "Tables have different partition key" or METADATA_MISMATCH.
The comparisons are performed on the ASTs (`getTreeHash`), not on the formatted text, so any
stored form of the same definition compares equal.
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


def check_replication(table, start=1):
    node_old.query(
        f"INSERT INTO {table} (a, b, c, d) VALUES ({start}, {start}, {start}, now()), ({start + 1}, {start + 1}, {start + 1}, now())"
    )
    node_new.query(f"SYSTEM SYNC REPLICA {table}", timeout=30)
    assert node_new.query(f"SELECT count() FROM {table} WHERE a >= {start}") == "2\n"

    node_new.query(
        f"INSERT INTO {table} (a, b, c, d) VALUES ({start + 2}, {start + 2}, {start + 2}, now())"
    )
    node_old.query(f"SYSTEM SYNC REPLICA {table}", timeout=30)
    assert node_old.query(f"SELECT count() FROM {table} WHERE a >= {start}") == "3\n"


def test_new_replica_joins_old_table(start_cluster):
    """The table is created by the old version (canonical metadata in ZooKeeper), then the
    current version joins with the same parenthesized DDL. Comparing the formatted text instead
    of the ASTs made the new replica fail to create with METADATA_MISMATCH."""
    node_old.query(CREATE_TEMPLATE.format(table="t_parens_old_first", replica="r1"))
    node_new.query(CREATE_TEMPLATE.format(table="t_parens_old_first", replica="r2"))

    check_replication("t_parens_old_first")


def test_old_replica_joins_new_table(start_cluster):
    """The table is created by the current version first: the old version must be able to join
    the metadata it wrote to ZooKeeper."""
    node_new.query(CREATE_TEMPLATE.format(table="t_parens_new_first", replica="r1"))
    node_old.query(CREATE_TEMPLATE.format(table="t_parens_new_first", replica="r2"))

    check_replication("t_parens_new_first")


def test_alter_metadata_across_versions(start_cluster):
    """A replicated `ALTER` on the current version serializes the changed definitions to
    ZooKeeper in whatever form the user wrote them (`SAMPLE BY (b)`, `CHECK (a > 0)`). An old
    replica attached to the same table must apply such `ALTER` log entries, and both replicas
    must still pass the metadata comparison against ZooKeeper on restart."""
    node_new.query("""
        CREATE TABLE t_parens_altered (a UInt32, b UInt32, c UInt32, d DateTime)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_parens_altered', 'r1')
        ORDER BY (b)
        """)
    node_old.query("""
        CREATE TABLE t_parens_altered (a UInt32, b UInt32, c UInt32, d DateTime)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_parens_altered', 'r2')
        ORDER BY (b)
        """)

    node_new.query("ALTER TABLE t_parens_altered MODIFY SAMPLE BY (b)")
    node_new.query("ALTER TABLE t_parens_altered MODIFY TTL (d + INTERVAL 10 YEAR)")
    node_new.query("ALTER TABLE t_parens_altered ADD CONSTRAINT cc CHECK (a > 0)")

    check_replication("t_parens_altered")

    # On restart both replicas compare their local metadata against the ALTER-written
    # ZooKeeper metadata: the parenthesized stored form must compare equal.
    node_new.restart_clickhouse()
    node_old.restart_clickhouse()

    check_replication("t_parens_altered", start=10)


def test_upgrade_and_attach_partition_from(start_cluster):
    """A table created by the old version must load after an upgrade and be compatible with a
    freshly created parenthesized table in `ATTACH PARTITION FROM`. Must be the last test in the
    module: it replaces the old binary on `node_old` with the current one."""
    node_old.query("""
        CREATE TABLE t_parens_src (a UInt32, b UInt32)
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (b)
        """)
    node_old.query("INSERT INTO t_parens_src VALUES (1, 1), (1, 2), (2, 1)")

    # A table with a secondary index and a projection created by the OLD version: it stores the
    # canonical form without the redundant parentheses (`INDEX ix b * c`, `PROJECTION p (SELECT b ...)`).
    node_old.query("""
        CREATE TABLE t_parens_defs_src (a UInt32, b UInt32, c UInt32,
            INDEX ix (b * c) TYPE minmax GRANULARITY 1,
            PROJECTION p (SELECT (b), sum(c) GROUP BY (b)))
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (a)
        """)
    node_old.query(
        "INSERT INTO t_parens_defs_src VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3)"
    )

    node_old.restart_with_latest_version()

    # The upgraded server loads the canonical `.sql` written by the old version.
    node_old.query("SELECT count() FROM t_parens_src")

    node_old.query("""
        CREATE TABLE t_parens_dst (a UInt32, b UInt32)
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (b)
        """)
    node_old.query("ALTER TABLE t_parens_dst ATTACH PARTITION 1 FROM t_parens_src")
    assert (
        node_old.query("SELECT a, b FROM t_parens_dst ORDER BY a, b") == "1\t1\n1\t2\n"
    )

    # The index/projection stored by the old version (canonical, no redundant parentheses) must be
    # interchangeable in `ATTACH PARTITION FROM` with a table freshly created by the current version,
    # whose `INDEX ix (b * c)` / `PROJECTION p (SELECT (b) ...)` keeps the parentheses #92340 preserves.
    node_old.query("""
        CREATE TABLE t_parens_defs_dst (a UInt32, b UInt32, c UInt32,
            INDEX ix (b * c) TYPE minmax GRANULARITY 1,
            PROJECTION p (SELECT (b), sum(c) GROUP BY (b)))
        ENGINE = MergeTree
        PARTITION BY (a)
        ORDER BY (a)
        """)
    node_old.query(
        "ALTER TABLE t_parens_defs_dst ATTACH PARTITION 1 FROM t_parens_defs_src"
    )
    assert (
        node_old.query("SELECT a, b, c FROM t_parens_defs_dst ORDER BY a, b, c")
        == "1\t1\t1\n1\t2\t2\n"
    )

    # The replicated table created by the old version must still work after the upgrade.
    node_old.query("SYSTEM SYNC REPLICA t_parens_old_first", timeout=30)
    assert node_old.query("SELECT count() FROM t_parens_old_first") == "3\n"

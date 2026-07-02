"""
Integration tests for skip_empty_columns_on_insert + missing_columns in
ReplicatedMergeTree. Tests replication consistency, merge propagation,
mixed-version compat, restart durability, and REPLACE PARTITION.
"""

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True, macros={"replica": "node1"})
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True, macros={"replica": "node2"})
old_node = cluster.add_instance(
    "old_node",
    with_zookeeper=True,
    macros={"replica": "old_node"},
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

# Common table settings for tests using the new feature.
SETTINGS = ", ".join(
    [
        "min_bytes_for_wide_part = 0",
        "min_rows_for_wide_part = 0",
        "ratio_of_defaults_for_sparse_serialization = 1.0",
        "skip_empty_columns_on_insert = 1",
        "serialization_info_version = 'with_missing_columns'",
        "enable_block_number_column = 0",
        "enable_block_offset_column = 0",
    ]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def q(node, query, **kwargs):
    """Shorthand: run a query, strip trailing whitespace."""
    return node.query(query, **kwargs).strip()


def sync(node, db, table):
    """Wait until a replica is fully synced."""
    node.query(f"SYSTEM SYNC REPLICA {db}.{table}", timeout=60)


# ---------------------------------------------------------------------------
# Test 1: Replicated table — both replicas see consistent data
# ---------------------------------------------------------------------------


def test_replicated_consistent_data(started_cluster):
    """
    Insert on node1 with some columns marked as missing (all type-default).
    Verify node2 syncs and sees the same data.
    After ALTER MODIFY COLUMN ... DEFAULT 999, both nodes must still
    return the original type-default (0), not 999.
    """
    db = "test_t1"
    table = f"{db}.t_repl"
    zk = "/clickhouse/test_t1/t_repl"

    for n in (node1, node2):
        n.query(f"CREATE DATABASE IF NOT EXISTS {db}")

    node1.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64, c String)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {SETTINGS}"""
    )
    node2.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64, c String)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {SETTINGS}"""
    )

    # b=0, c='' → both marked as missing
    node1.query(f"INSERT INTO {table} VALUES (1, 100, 0, '')")
    sync(node2, db, "t_repl")

    expected = "1\t100\t0\t"
    assert q(node1, f"SELECT * FROM {table}") == expected
    assert q(node2, f"SELECT * FROM {table}") == expected

    # ALTER DEFAULT on b — should NOT affect existing parts (frozen marker)
    for n in (node1, node2):
        n.query(
            f"ALTER TABLE {table} MODIFY COLUMN b UInt64 DEFAULT 999",
            settings={"mutations_sync": "2"},
        )

    assert q(node1, f"SELECT b FROM {table}") == "0"
    assert q(node2, f"SELECT b FROM {table}") == "0"

    for n in (node1, node2):
        n.query(f"DROP TABLE {table} SYNC")
        n.query(f"DROP DATABASE {db}")


# ---------------------------------------------------------------------------
# Test 2: Replicated merge preserves missing_columns marker
# ---------------------------------------------------------------------------


def test_replicated_merge_preserves_marker(started_cluster):
    """
    Insert two parts on node1 with column b=0 missing in both.
    OPTIMIZE FINAL → merge preserves marker.
    Node2 syncs the merged part.
    After ALTER DEFAULT, both nodes still read 0.
    """
    db = "test_t2"
    table = f"{db}.t_merge"
    zk = "/clickhouse/test_t2/t_merge"

    for n in (node1, node2):
        n.query(f"CREATE DATABASE IF NOT EXISTS {db}")

    node1.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {SETTINGS}"""
    )
    node2.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {SETTINGS}"""
    )

    # Two parts, b=0 in both → b marked as missing in both
    node1.query(f"INSERT INTO {table} VALUES (1, 100, 0)")
    node1.query(f"INSERT INTO {table} VALUES (2, 200, 0)")
    node1.query(f"OPTIMIZE TABLE {table} FINAL")

    sync(node2, db, "t_merge")

    expected = "0\n0"
    assert q(node1, f"SELECT b FROM {table} ORDER BY key") == expected
    assert q(node2, f"SELECT b FROM {table} ORDER BY key") == expected

    # ALTER DEFAULT — marker must shield old data
    for n in (node1, node2):
        n.query(
            f"ALTER TABLE {table} MODIFY COLUMN b UInt64 DEFAULT 999",
            settings={"mutations_sync": "2"},
        )

    assert q(node1, f"SELECT b FROM {table} ORDER BY key") == expected
    assert q(node2, f"SELECT b FROM {table} ORDER BY key") == expected

    for n in (node1, node2):
        n.query(f"DROP TABLE {table} SYNC")
        n.query(f"DROP DATABASE {db}")


# ---------------------------------------------------------------------------
# Test 3: Mixed version replicas — old replica reads parts from new node
# ---------------------------------------------------------------------------


def test_mixed_version_version_gate(started_cluster):
    """
    node1 (new binary) uses serialization_info_version='basic'
    (below with_missing_columns) so old_node can read the parts.
    No columns should actually be missing — the version gate prevents it.
    Both nodes see identical data.
    """
    db = "test_t3"
    table = f"{db}.t_mixed"
    zk = "/clickhouse/test_t3/t_mixed"

    # Settings with version gate set LOW so old node can read
    compat_settings = ", ".join(
        [
            "min_bytes_for_wide_part = 0",
            "min_rows_for_wide_part = 0",
            "ratio_of_defaults_for_sparse_serialization = 1.0",
            "skip_empty_columns_on_insert = 1",
            "serialization_info_version = 'basic'",
            "enable_block_number_column = 0",
            "enable_block_offset_column = 0",
        ]
    )

    for n in (node1, old_node):
        n.query(f"CREATE DATABASE IF NOT EXISTS {db}")

    node1.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {compat_settings}"""
    )

    # old_node: create same table but without skip_empty_columns_on_insert
    # (old version doesn't know the setting)
    old_node.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 ratio_of_defaults_for_sparse_serialization = 1.0"""
    )

    # Insert on new node — version gate means b is NOT marked as missing
    node1.query(f"INSERT INTO {table} VALUES (1, 100, 0)")
    sync(old_node, db, "t_mixed")

    # Both nodes see the data — b column present in part
    expected = "1\t100\t0"
    assert q(node1, f"SELECT * FROM {table}") == expected
    assert q(old_node, f"SELECT * FROM {table}") == expected

    # Verify b column IS in the part (not marked as missing)
    cols = q(
        node1,
        f"""SELECT column FROM system.parts_columns
        WHERE database = '{db}' AND table = 't_mixed' AND active
        ORDER BY column""",
    )
    assert "b" in cols.split("\n")

    for n in (node1, old_node):
        n.query(f"DROP TABLE {table} SYNC")
        n.query(f"DROP DATABASE {db}")


# ---------------------------------------------------------------------------
# Test 4: New version reads parts written by old version (backward compat)
# ---------------------------------------------------------------------------


def test_old_parts_use_current_default(started_cluster):
    """
    old_node writes a part (no missing_columns support).
    node1 (new binary) reads it via replication.
    ALTER ADD COLUMN + ALTER MODIFY DEFAULT.
    Old parts (without missing_columns marker) use the CURRENT default
    — this is the backward-compat fallback.
    """
    db = "test_t4"
    table = f"{db}.t_compat"
    zk = "/clickhouse/test_t4/t_compat"

    for n in (old_node, node1):
        n.query(f"CREATE DATABASE IF NOT EXISTS {db}")

    # Create on old_node first (it doesn't know skip_empty_columns_on_insert)
    old_node.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 ratio_of_defaults_for_sparse_serialization = 1.0"""
    )

    node1.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 ratio_of_defaults_for_sparse_serialization = 1.0,
                 enable_block_number_column = 0, enable_block_offset_column = 0"""
    )

    # Old node writes data
    old_node.query(f"INSERT INTO {table} VALUES (1, 100)")
    sync(node1, db, "t_compat")

    # ADD COLUMN with DEFAULT on new node — runs as replicated DDL
    node1.query(
        f"ALTER TABLE {table} ADD COLUMN b UInt64 DEFAULT 42",
        settings={"mutations_sync": "2"},
    )
    sync(old_node, db, "t_compat")

    # Old part has no missing_columns → fallback: use CURRENT default (42)
    assert q(node1, f"SELECT b FROM {table}") == "42"

    # Modify default to 999
    node1.query(
        f"ALTER TABLE {table} MODIFY COLUMN b UInt64 DEFAULT 999",
        settings={"mutations_sync": "2"},
    )

    # Backward compat: old parts follow the CURRENT default → now 999
    assert q(node1, f"SELECT b FROM {table}") == "999"

    for n in (old_node, node1):
        n.query(f"DROP TABLE {table} SYNC")
        n.query(f"DROP DATABASE {db}")


# ---------------------------------------------------------------------------
# Test 5: Fetched parts preserve missing_columns after restart
# ---------------------------------------------------------------------------


def test_fetched_parts_survive_restart(started_cluster):
    """
    Insert on node1 with missing columns.
    node2 fetches part via SYSTEM SYNC REPLICA.
    Restart node2. Verify data is still correct.
    ALTER DEFAULT. Verify marker still works.
    """
    db = "test_t5"
    table = f"{db}.t_restart"
    zk = "/clickhouse/test_t5/t_restart"

    for n in (node1, node2):
        n.query(f"CREATE DATABASE IF NOT EXISTS {db}")

    node1.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64, c String)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {SETTINGS}"""
    )
    node2.query(
        f"""CREATE TABLE {table}
        (key UInt64, a UInt64, b UInt64, c String)
        ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
        ORDER BY key
        SETTINGS {SETTINGS}"""
    )

    # b=0, c='' → marked as missing
    node1.query(f"INSERT INTO {table} VALUES (1, 100, 0, '')")
    node1.query(f"INSERT INTO {table} VALUES (2, 200, 0, '')")
    sync(node2, db, "t_restart")

    expected = "1\t100\t0\t\n2\t200\t0\t"
    assert q(node2, f"SELECT * FROM {table} ORDER BY key") == expected

    # Restart node2 — serialization.json is persisted on disk
    node2.restart_clickhouse()
    assert q(node2, f"SELECT * FROM {table} ORDER BY key") == expected

    # ALTER DEFAULT on both
    for n in (node1, node2):
        n.query(
            f"ALTER TABLE {table} MODIFY COLUMN b UInt64 DEFAULT 999",
            settings={"mutations_sync": "2"},
        )

    # Marker must survive the restart and still shield against new default
    assert q(node2, f"SELECT b FROM {table} ORDER BY key") == "0\n0"
    assert q(node1, f"SELECT b FROM {table} ORDER BY key") == "0\n0"

    for n in (node1, node2):
        n.query(f"DROP TABLE {table} SYNC")
        n.query(f"DROP DATABASE {db}")


# ---------------------------------------------------------------------------
# Test 6: REPLACE PARTITION preserves missing_columns marker
# ---------------------------------------------------------------------------


def test_replace_partition_preserves_marker(started_cluster):
    """
    Two replicated tables with different ZK paths.
    Insert into table1 with missing columns.
    ALTER TABLE table2 REPLACE PARTITION FROM table1.
    Verify table2 reads correctly.
    ALTER DEFAULT on table2 — marker must be preserved.
    """
    db = "test_t6"
    t1 = f"{db}.t_src"
    t2 = f"{db}.t_dst"
    zk1 = "/clickhouse/test_t6/t_src"
    zk2 = "/clickhouse/test_t6/t_dst"

    node1.query(f"CREATE DATABASE IF NOT EXISTS {db}")

    # Both tables use the same schema + partition key for REPLACE PARTITION
    for tbl, zk in ((t1, zk1), (t2, zk2)):
        node1.query(
            f"""CREATE TABLE {tbl}
            (key UInt64, a UInt64, b UInt64)
            ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}')
            PARTITION BY intDiv(key, 10)
            ORDER BY key
            SETTINGS {SETTINGS}"""
        )

    # Insert into source — b=0 → marked as missing
    node1.query(f"INSERT INTO {t1} VALUES (1, 100, 0)")
    node1.query(f"INSERT INTO {t1} VALUES (2, 200, 0)")

    assert q(node1, f"SELECT b FROM {t1} ORDER BY key") == "0\n0"

    # REPLACE PARTITION into destination
    node1.query(f"ALTER TABLE {t2} REPLACE PARTITION 0 FROM {t1}")

    assert q(node1, f"SELECT * FROM {t2} ORDER BY key") == "1\t100\t0\n2\t200\t0"

    # ALTER DEFAULT on destination — marker must be preserved from source part
    node1.query(
        f"ALTER TABLE {t2} MODIFY COLUMN b UInt64 DEFAULT 999",
        settings={"mutations_sync": "2"},
    )

    assert q(node1, f"SELECT b FROM {t2} ORDER BY key") == "0\n0"

    node1.query(f"DROP TABLE {t1} SYNC")
    node1.query(f"DROP TABLE {t2} SYNC")
    node1.query(f"DROP DATABASE {db}")

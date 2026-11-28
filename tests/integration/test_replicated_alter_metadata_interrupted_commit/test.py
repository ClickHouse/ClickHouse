import re
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/fail_points.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

FAILPOINT = "replicated_merge_tree_pause_after_alter_metadata_zk_commit"
FAILPOINT_CONFIG = "/etc/clickhouse-server/config.d/fail_points.xml"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def replica_path(table):
    return f"/clickhouse/{table}/replicas/r1"


def read_metadata_version(table):
    return int(
        node.query(
            f"""
            SELECT value FROM system.zookeeper
            WHERE path = '{replica_path(table)}' AND name = 'metadata_version'
            """
        ).strip()
    )


def count_queue_entries(table):
    return int(
        node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{replica_path(table)}/queue'"
        ).strip()
    )


def interrupt_alter(table, alter):
    """Run `alter` and leave it parked right after it committed the metadata to Keeper,
    i.e. in the window in which the local metadata file has not been written yet."""
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

    # alter_sync = 0 so the query returns without waiting for this replica to apply the entry;
    # the applying happens on a background queue thread, which is what parks at the failpoint.
    node.query(f"{alter} SETTINGS alter_sync = 0")

    node.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE")


def readonly_failure_lines(table, error="Code: 122"):
    """Log lines in which the attach thread refused to start this table with this error. Both parts have
    to be on the same line: the log covers every table of the module, so testing for them separately
    could be satisfied by two unrelated lines. The table name is matched on word boundaries because one
    table name in this module is a prefix of another."""
    return [
        line
        for line in node.grep_in_log(
            "Initialization failed, table will remain readonly"
        ).splitlines()
        if re.search(rf"\b{re.escape(table)}\b", line) and error in line
    ]


def wait_readonly_failure_lines(table, error="Code: 122", retries=30, sleep_time=0.5):
    """is_readonly is already 1 while the attach thread is still running, so the readonly assertions are
    satisfied before the thread has thrown. Wait for the log line that proves it did."""
    for _ in range(retries):
        lines = readonly_failure_lines(table, error)
        if lines:
            return lines
        time.sleep(sleep_time)
    return []


def test_columns_delta_recovers(start_cluster):
    table = "interrupted_add_column"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        INSERT INTO {table} VALUES (1);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} ADD COLUMN y UInt32")

    # The state under test really is the torn one: Keeper has the new metadata and still owes the
    # entry, while the local table does not know the new column yet.
    assert read_metadata_version(table) == 1
    assert count_queue_entries(table) >= 1
    assert (
        node.query(
            f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'y'"
        ).strip()
        == "0"
    )

    # Enabled from the config for this restart, the failpoint parks the queue's replay of the entry after
    # its Keeper commit and before it writes the local metadata file. A new column visible there can only
    # have been published by the attach.
    node.replace_in_config(
        FAILPOINT_CONFIG, f"<{FAILPOINT}>false<", f"<{FAILPOINT}>true<"
    )
    try:
        node.restart_clickhouse(kill=True)
        node.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=120)
        assert count_queue_entries(table) >= 1
        assert (
            node.query(
                f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'y'"
            ).strip()
            == "1"
        )
    finally:
        node.replace_in_config(
            FAILPOINT_CONFIG, f"<{FAILPOINT}>true<", f"<{FAILPOINT}>false<"
        )
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "0\n"
    )
    assert node.query(f"SELECT y FROM {table}").strip() == "0"
    assert read_metadata_version(table) == 1

    # The queue, not the attach, is what persisted the metadata: the entry is only acknowledged
    # after executeMetadataAlter has written the local metadata file.
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.zookeeper WHERE path = '{replica_path(table)}/queue'",
        "0\n",
    )

    # A restart with nothing left in the queue must start clean, which it can only do if the
    # metadata file on disk was really updated rather than only the in-memory copy.
    node.restart_clickhouse()
    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "0\n"
    )
    assert node.query(f"SELECT y FROM {table}").strip() == "0"


def test_new_column_in_sorting_key_recovers(start_cluster):
    """The Keeper metadata may reference a column the local table does not have yet, so it can only be
    parsed against the columns from the same source. Guards the parse order of the recovery path."""
    table = "interrupted_add_column_reorder"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32, y UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY (x, y);
        INSERT INTO {table} VALUES (1, 2);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} ADD COLUMN z UInt32 AFTER y, MODIFY ORDER BY (x, y, -z)")

    assert read_metadata_version(table) == 1
    assert (
        node.query(
            f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'z'"
        ).strip()
        == "0"
    )

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "0\n"
    )
    assert node.query(f"SELECT z FROM {table}").strip() == "0"
    assert "-z" in node.query(f"SELECT sorting_key FROM system.tables WHERE name = '{table}'")

    # Every recovering arm must leave its queue empty: the failpoint is process-global and one-shot, so an
    # entry still pending here would consume the next arm's.
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.zookeeper WHERE path = '{replica_path(table)}/queue'",
        "0\n",
    )


def test_projections_delta_recovers(start_cluster):
    table = "interrupted_drop_projection"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32, PROJECTION p (SELECT x ORDER BY x))
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        INSERT INTO {table} VALUES (1);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} DROP PROJECTION p")

    assert read_metadata_version(table) == 1
    assert count_queue_entries(table) >= 1
    assert (
        node.query(
            f"SELECT count() FROM system.projections WHERE table = '{table}' AND name = 'p'"
        ).strip()
        == "1"
    )

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "0\n"
    )
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.projections WHERE table = '{table}' AND name = 'p'",
        "0\n",
    )
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.zookeeper WHERE path = '{replica_path(table)}/queue'",
        "0\n",
    )

    node.restart_clickhouse()
    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "0\n"
    )


def test_no_pending_entry_still_refuses(start_cluster):
    """Without a queued ALTER_METADATA entry there is nothing that authorises adopting the Keeper
    metadata, so the replica must refuse exactly as it did before."""
    table = "no_pending_entry"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        INSERT INTO {table} VALUES (1);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} ADD COLUMN y UInt32")

    assert read_metadata_version(table) == 1

    zk = cluster.get_kazoo_client("zoo1")
    queue_path = f"{replica_path(table)}/queue"
    entries = zk.get_children(queue_path)
    assert len(entries) >= 1
    for entry in entries:
        zk.delete(f"{queue_path}/{entry}")

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "1\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'y'"
        ).strip()
        == "0"
    )
    assert wait_readonly_failure_lines(
        table
    ), "expected a readonly initialization failure with Code: 122 for this table"


def test_entry_version_mismatch_still_refuses(start_cluster):
    """The pending entry has to account for the metadata_version this replica committed. If it does not,
    the entry says nothing about the metadata that is actually in Keeper, so the replica must refuse even
    though an ALTER_METADATA entry is still queued."""
    table = "entry_version_mismatch"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        INSERT INTO {table} VALUES (1);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} ADD COLUMN y UInt32")

    assert read_metadata_version(table) == 1

    zk = cluster.get_kazoo_client("zoo1")
    # Only metadata_version moves, to a value no entry carries. The queue and the columns/metadata
    # znodes stay exactly as the ALTER left them, so the version comparison is the only thing that can
    # make the replica refuse here.
    zk.set(f"{replica_path(table)}/metadata_version", b"7")
    assert count_queue_entries(table) >= 1

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "1\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'y'"
        ).strip()
        == "0"
    )
    assert wait_readonly_failure_lines(
        table
    ), "expected a readonly initialization failure with Code: 122 for this table"


def test_entry_payload_mismatch_still_refuses(start_cluster):
    """The pending entry has to carry byte-for-byte the columns this replica committed. With the
    committed columns rewritten behind the entry's back, even to an equal structure, the entry is not the
    one that committed them, so the replica must refuse and must not publish the entry's columns."""
    table = "entry_payload_mismatch"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        INSERT INTO {table} VALUES (1);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} ADD COLUMN y UInt32")

    assert read_metadata_version(table) == 1

    zk = cluster.get_kazoo_client("zoo1")
    columns_path = f"{replica_path(table)}/columns"
    old_columns = zk.get(columns_path)[0]
    # The same columns spelled differently (`INT UNSIGNED` is an alias of `UInt32`), so the structure check
    # would accept the entry's columns and only the byte comparison against the entry can make the replica
    # refuse. The queue and metadata_version stay untouched.
    new_columns = old_columns.replace(b"`y` UInt32", b"`y` INT UNSIGNED")
    assert new_columns != old_columns, "the columns znode was not rewritten"
    zk.set(columns_path, new_columns)
    assert count_queue_entries(table) >= 1

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "1\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'y'"
        ).strip()
        == "0"
    )
    assert wait_readonly_failure_lines(
        table
    ), "expected a readonly initialization failure with Code: 122 for this table"


def test_entry_metadata_mismatch_still_refuses(start_cluster):
    """The pending entry has to carry byte-for-byte the metadata this replica committed, not only the
    columns. With the committed metadata rewritten behind the entry's back, even to an equal structure,
    the entry is not the one that committed it, so the replica must refuse and must not publish the
    entry's columns."""
    table = "entry_metadata_mismatch"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        INSERT INTO {table} VALUES (1);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} ADD COLUMN y UInt32")

    assert read_metadata_version(table) == 1

    zk = cluster.get_kazoo_client("zoo1")
    metadata_path = f"{replica_path(table)}/metadata"
    old_metadata = zk.get(metadata_path)[0]
    # The same primary key written differently, so the structure check would accept the entry's metadata
    # and only the metadata byte comparison against the entry can make the replica refuse. The queue,
    # columns and metadata_version stay untouched.
    new_metadata, n = re.subn(rb"^primary key: x$", b"primary key: (x)", old_metadata, flags=re.M)
    assert n == 1, f"expected one primary key line, got {n}: {old_metadata!r}"
    zk.set(metadata_path, new_metadata)
    assert count_queue_entries(table) >= 1

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "1\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = 'y'"
        ).strip()
        == "0"
    )
    assert wait_readonly_failure_lines(
        table
    ), "expected a readonly initialization failure with Code: 122 for this table"


def test_later_attach_failure_keeps_local_metadata(start_cluster):
    """An attach that fails after the structure check passed leaves the table readonly with its queue never
    loaded, so nothing applies the queued ALTER's conversions to the parts. The adopted metadata must not be
    published in that case: the parts are still read with the local metadata they were written with."""
    table = "attach_failure_after_adoption"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table} SYNC;
        CREATE TABLE {table} (x UInt32, y UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY x;
        SYSTEM STOP MERGES {table};
        INSERT INTO {table} VALUES (1, 10);
        INSERT INTO {table} VALUES (2, 20), (3, 30);
        """
    )

    interrupt_alter(table, f"ALTER TABLE {table} RENAME COLUMN y TO z")

    assert read_metadata_version(table) == 1
    assert count_queue_entries(table) >= 1
    assert (
        node.query(
            f"SELECT name FROM system.columns WHERE table = '{table}' ORDER BY position"
        )
        == "x\ny\n"
    )

    # The 2-row part becomes unexpected for this replica. That is more than half of its rows, so checkParts
    # refuses to start the table, after the structure check has already accepted the adopted metadata.
    unexpected_part = node.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' AND active AND rows = 2"
    ).strip()
    assert unexpected_part, "expected the 2-row part to be active"
    zk = cluster.get_kazoo_client("zoo1")
    zk.delete(f"{replica_path(table)}/parts/{unexpected_part}", recursive=True)

    node.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        node, f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'", "1\n"
    )
    assert wait_readonly_failure_lines(
        table, error="Code: 231"
    ), "expected a readonly initialization failure with Code: 231 for this table"
    assert (
        node.query(
            f"SELECT name FROM system.columns WHERE table = '{table}' ORDER BY position"
        )
        == "x\ny\n"
    )
    # With the adopted metadata published, this part would be read as if it already had the column `z`,
    # which it does not, and every value would come back as the default 0.
    assert node.query(f"SELECT x, y FROM {table} ORDER BY x") == "1\t10\n"

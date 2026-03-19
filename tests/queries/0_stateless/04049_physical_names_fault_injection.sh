#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_physical_column_names=1"

# Test 1: INSERT concurrent with metadata-only RENAME
# Uses the physical_names_pause_after_metadata_alter failpoint to pause
# ALTER RENAME after both the physical name mapping and metadata are committed
# but before serialization hints are reset. An INSERT should work correctly
# because both the mapping and metadata are already consistent.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_concurrent;

    CREATE TABLE t_fp_concurrent
    (
        a UInt64,
        b String
    )
    ENGINE = MergeTree
    ORDER BY a
    SETTINGS
        min_bytes_for_wide_part = 0,
        serialization_info_version = 'with_physical_names',
        activate_physical_names_for_existing_tables = 1;
"

echo "INSERT INTO t_fp_concurrent VALUES (1, 'before_rename')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT physical_names_pause_after_metadata_alter"

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_concurrent RENAME COLUMN b TO d" &
ALTER_PID=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT physical_names_pause_after_metadata_alter PAUSE"

# While ALTER is paused (mapping + metadata both committed, only
# serialization hint reset pending), insert a row using the new schema.
echo "INSERT INTO t_fp_concurrent (a, d) VALUES (2, 'during_rename')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT physical_names_pause_after_metadata_alter"
wait $ALTER_PID

# After ALTER completes, verify all data is accessible
$CLICKHOUSE_CLIENT --query "SELECT a, d FROM t_fp_concurrent ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_concurrent SYNC"

# Test 2: Exception before mapping persist — verify ALTER fails but table remains usable
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_crash;

    CREATE TABLE t_fp_crash
    (
        a UInt64,
        b String
    )
    ENGINE = MergeTree
    ORDER BY a
    SETTINGS
        min_bytes_for_wide_part = 0,
        serialization_info_version = 'with_physical_names',
        activate_physical_names_for_existing_tables = 1;
"

echo "INSERT INTO t_fp_crash VALUES (1, 'safe')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT physical_names_throw_before_mapping_persist"

# This ALTER should fail because of the injected exception
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_crash RENAME COLUMN b TO d" 2>&1 | grep -o -m1 'FAULT_INJECTED'

# Table MUST still have original column name — the exception fires before
# both the mapping persist and the metadata commit, so nothing changed.
$CLICKHOUSE_CLIENT --query "SELECT a, b FROM t_fp_crash ORDER BY a"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT physical_names_throw_before_mapping_persist"

# Retry RENAME — must succeed because no partial state was committed.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_crash RENAME COLUMN b TO d"

echo "INSERT INTO t_fp_crash (a, d) VALUES (2, 'recovered')" | $CLICKHOUSE_CLIENT
$CLICKHOUSE_CLIENT --query "SELECT a, d FROM t_fp_crash ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_crash SYNC"

# Test 3: Merge scheduling paused while RENAME happens
# Verifies that a merge kicked off AFTER a rename correctly
# reads pre-rename parts using physical names.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_merge;

    CREATE TABLE t_fp_merge
    (
        a UInt64,
        b String
    )
    ENGINE = MergeTree
    ORDER BY a
    SETTINGS
        min_bytes_for_wide_part = 0,
        serialization_info_version = 'with_physical_names',
        activate_physical_names_for_existing_tables = 1;

    SYSTEM STOP MERGES t_fp_merge;
"

echo "INSERT INTO t_fp_merge VALUES (1, 'one')" | $CLICKHOUSE_CLIENT
echo "INSERT INTO t_fp_merge VALUES (2, 'two')" | $CLICKHOUSE_CLIENT
echo "INSERT INTO t_fp_merge VALUES (3, 'three')" | $CLICKHOUSE_CLIENT

# Rename while merges are stopped (3 separate parts with old column name)
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_merge RENAME COLUMN b TO d"

# Verify we have multiple parts
PARTS=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_fp_merge' AND active")
echo "parts_before_merge: $([ $PARTS -gt 1 ] && echo 'multiple' || echo 'single')"

# Resume merges and force merge
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_fp_merge"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_fp_merge FINAL"

# Verify merged data with renamed column
$CLICKHOUSE_CLIENT --query "SELECT a, d FROM t_fp_merge ORDER BY a"

# Verify single merged part
PARTS_AFTER=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_fp_merge' AND active")
echo "parts_after_merge: $PARTS_AFTER"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_merge SYNC"

# Test 4: Two-phase DROP crash safety — exception after mapping persist but
# before metadata commit.  With the two-phase drop fix, the mapping must
# NOT have removed the dropped column yet, so the exception rolls back cleanly
# and column b remains readable.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_drop;

    CREATE TABLE t_fp_drop
    (
        a UInt64,
        b String
    )
    ENGINE = MergeTree
    ORDER BY a
    SETTINGS
        min_bytes_for_wide_part = 0,
        serialization_info_version = 'with_physical_names',
        activate_physical_names_for_existing_tables = 1;
"

echo "INSERT INTO t_fp_drop VALUES (1, 'keep_me')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT physical_names_throw_after_mapping_persist"

# This ALTER should fail because of the injected exception
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_drop DROP COLUMN b" 2>&1 | grep -o -m1 'FAULT_INJECTED'

# Column b must still be readable — the two-phase drop kept b's mapping
# entry in the persisted mapping (deferred removal to post-commit), and the
# exception handler reverted the in-memory mapping to the old state.
$CLICKHOUSE_CLIENT --query "SELECT a, b FROM t_fp_drop ORDER BY a"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT physical_names_throw_after_mapping_persist"

# Retry DROP — must succeed because no partial state was committed.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_drop DROP COLUMN b"
$CLICKHOUSE_CLIENT --query "SELECT a FROM t_fp_drop ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_drop SYNC"

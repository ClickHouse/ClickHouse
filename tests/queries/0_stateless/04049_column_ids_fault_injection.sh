#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# why: failpoint-driven checks that concurrent or failing column-ID ALTERs leave the
# mapping, metadata and per-part serialization files consistent.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_pause_after_metadata_alter" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_before_mapping_persist" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_after_mapping_persist" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_finalize_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mutate_task_finalize_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT insert_write_temp_part_pause" 2>/dev/null
}
trap cleanup EXIT

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

# Section 1: INSERT while a RENAME is paused after the mapping and metadata are both
# committed (only the serialization-hint reset is pending) must use the new schema.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_concurrent;

    CREATE TABLE t_fp_concurrent (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
"
echo "INSERT INTO t_fp_concurrent VALUES (1, 'before_rename')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT column_ids_pause_after_metadata_alter"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_concurrent RENAME COLUMN b TO d" &
ALTER_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT column_ids_pause_after_metadata_alter PAUSE"
echo "INSERT INTO t_fp_concurrent (a, d) VALUES (2, 'during_rename')" | $CLICKHOUSE_CLIENT
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_pause_after_metadata_alter"
wait $ALTER_PID

$CLICKHOUSE_CLIENT --query "SELECT a, d FROM t_fp_concurrent ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_concurrent SYNC"

# Section 2: an exception before the mapping persist must roll the RENAME back
# cleanly -- nothing was committed, and a retry succeeds.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_crash;

    CREATE TABLE t_fp_crash (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
"
echo "INSERT INTO t_fp_crash VALUES (1, 'safe')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT column_ids_throw_before_mapping_persist"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_crash RENAME COLUMN b TO d" 2>&1 | grep -o -m1 'FAULT_INJECTED'
$CLICKHOUSE_CLIENT --query "SELECT a, b FROM t_fp_crash ORDER BY a"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_before_mapping_persist"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_crash RENAME COLUMN b TO d"
echo "INSERT INTO t_fp_crash (a, d) VALUES (2, 'recovered')" | $CLICKHOUSE_CLIENT
$CLICKHOUSE_CLIENT --query "SELECT a, d FROM t_fp_crash ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_crash SYNC"

# Section 3: a merge kicked off after a RENAME must read pre-rename parts by ID.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_merge;

    CREATE TABLE t_fp_merge (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';

    SYSTEM STOP MERGES t_fp_merge;
"
echo "INSERT INTO t_fp_merge VALUES (1, 'one')" | $CLICKHOUSE_CLIENT
echo "INSERT INTO t_fp_merge VALUES (2, 'two')" | $CLICKHOUSE_CLIENT
echo "INSERT INTO t_fp_merge VALUES (3, 'three')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_merge RENAME COLUMN b TO d"
PARTS=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_fp_merge' AND active" | tr -d '[:space:]')
echo "parts_before_merge: $([ "$PARTS" -gt 1 ] && echo 'multiple' || echo 'single')"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_fp_merge"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_fp_merge FINAL"

$CLICKHOUSE_CLIENT --query "SELECT a, d FROM t_fp_merge ORDER BY a"
PARTS_AFTER=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_fp_merge' AND active" | tr -d '[:space:]')
echo "parts_after_merge: $PARTS_AFTER"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_merge SYNC"

# Section 4: two-phase DROP -- an exception after the mapping persist but before the
# metadata commit must leave the dropped column readable (removal is deferred to
# post-commit) and a retry succeeds.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_fp_drop;

    CREATE TABLE t_fp_drop (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
"
echo "INSERT INTO t_fp_drop VALUES (1, 'keep_me')" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT column_ids_throw_after_mapping_persist"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_drop DROP COLUMN b" 2>&1 | grep -o -m1 'FAULT_INJECTED'
$CLICKHOUSE_CLIENT --query "SELECT a, b FROM t_fp_drop ORDER BY a"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_after_mapping_persist"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_fp_drop DROP COLUMN b"
$CLICKHOUSE_CLIENT --query "SELECT a FROM t_fp_drop ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_fp_drop SYNC"

# Section 5: a metadata-only RENAME landing during a merge (paused between stamping
# the part's column IDs and writing serialization.json) must not desync
# serialization.json from columns.txt -- the sparse column survives reload.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ren_merge SYNC"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ren_merge (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.5;
"
# Make `b` non-identity (numeric ID via DROP + re-ADD) and sparse (all-empty), so its
# data files are only reachable through the mapping + serialization.json.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_merge DROP COLUMN b"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_merge ADD COLUMN b String"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ren_merge (a, c) SELECT number, number / 2 FROM numbers(1000)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ren_merge (a, c) SELECT number + 1000, number / 2 FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_finalize_pause"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ren_merge FINAL" &
opt_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_task_finalize_pause PAUSE"
# The merge has stamped the part's column IDs; flip the mapping now.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_merge RENAME COLUMN b TO d"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_finalize_pause"
wait "$opt_pid"

$CLICKHOUSE_CLIENT --query "DETACH TABLE t_ren_merge SYNC"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE t_ren_merge"
$CLICKHOUSE_CLIENT --query "
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ren_merge' AND active AND column = 'd'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ren_merge SYNC"

# Section 6: mutation-path sibling of section 5 -- `mutate_task_finalize_pause`
# pauses right before finalizeMutatedPart.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ren_mut SYNC"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ren_mut (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.5;
"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_mut DROP COLUMN b"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_mut ADD COLUMN b String"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ren_mut (a, c) SELECT number, number / 2 FROM numbers(1000)"

# Mutate an unrelated column so `b`'s serialization record is carried over.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mutate_task_finalize_pause"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_mut UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 1" &
mut_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT mutate_task_finalize_pause PAUSE"
# The mutation has stamped the new part's column IDs; flip the mapping now.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ren_mut RENAME COLUMN b TO d"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mutate_task_finalize_pause"
wait "$mut_pid"

$CLICKHOUSE_CLIENT --query "DETACH TABLE t_ren_mut SYNC"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE t_ren_mut"
$CLICKHOUSE_CLIENT --query "
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ren_mut' AND active AND column = 'd'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ren_mut SYNC"

# Section 7: INSERT/RENAME race (CB1) -- sink stamps from captured snapshot, not live mapping.
# An ALTER RENAME landing between the INSERT sink's snapshot capture and the temp-part write
# must not make the writer stamp a stale name against the moved live mapping; the inserted rows
# must stay readable under the new name instead of silently reading as defaults.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_ins_rename SYNC;

    CREATE TABLE t_ins_rename (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
"
# Make `b` a numeric-ID column (id != name) via DROP + re-ADD, so stamping the name as an
# identity ID would point at a different physical column than the mapping's `b`.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ins_rename DROP COLUMN b"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ins_rename ADD COLUMN b String"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT insert_write_temp_part_pause"
echo "INSERT INTO t_ins_rename (a, b) VALUES (1, 'inserted')" | $CLICKHOUSE_CLIENT &
INS_PID=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT insert_write_temp_part_pause PAUSE"
# The sink already captured its snapshot mapping; move the live mapping out from under it.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_ins_rename RENAME COLUMN b TO c"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT insert_write_temp_part_pause"
wait $INS_PID

# The row inserted into `b` must be readable under the new name `c` (not a silent default).
$CLICKHOUSE_CLIENT --query "SELECT a, c FROM t_ins_rename ORDER BY a"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ins_rename SYNC"

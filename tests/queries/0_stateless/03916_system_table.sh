#!/usr/bin/env bash
# Tags: no-parallel
# Other tests can enable failpoints and interfere with this test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DROP_DB="${CLICKHOUSE_DATABASE}_fp_drop"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT backup_add_empty_memory_table" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT drop_database_before_exclusive_ddl_lock" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DROP_DB}" 2>/dev/null || true
}
trap cleanup EXIT

# ──────────────────────────────────────────────────────────────────────────────
# Static checks (no concurrency needed)
# ──────────────────────────────────────────────────────────────────────────────

$CLICKHOUSE_CLIENT -nmq "
-- Basic: table exists and returns rows
SELECT count() > 0 FROM system.fail_points;

-- Schema check: verify columns and types
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'fail_points' ORDER BY position;

-- All failpoints are disabled by default
SELECT count() FROM system.fail_points WHERE enabled = 1;

-- All four types are present
SELECT type, count() > 0 FROM system.fail_points GROUP BY type ORDER BY type;

-- Filtering by type works
SELECT count() > 0 FROM system.fail_points WHERE type = 'pauseable';

-- Filtering by name with LIKE
SELECT count() > 0 FROM system.fail_points WHERE name LIKE '%smt_%';

-- Verify the specific failpoint we will test is present and disabled
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';

-- Enable a failpoint, verify it shows as enabled
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';

-- Disable it, verify it shows as disabled again
SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';

-- Regression test for issue #103403:
-- Querying system.fail_points must not consume 'once' and 'pauseable_once' failpoints.
-- The old code called fiu_fail() which marks FIU_ONETIME failpoints as already-fired.

-- 'once' type: enable, query three times, verify still enabled each time
SYSTEM ENABLE FAILPOINT backup_add_empty_memory_table;
SELECT name, enabled FROM system.fail_points WHERE name = 'backup_add_empty_memory_table';
SELECT name, enabled FROM system.fail_points WHERE name = 'backup_add_empty_memory_table';
SELECT name, enabled FROM system.fail_points WHERE name = 'backup_add_empty_memory_table';
SYSTEM DISABLE FAILPOINT backup_add_empty_memory_table;
SELECT name, enabled FROM system.fail_points WHERE name = 'backup_add_empty_memory_table';

-- 'pauseable_once' type: same check
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
"

# ──────────────────────────────────────────────────────────────────────────────
# Concurrent test: verify that a PAUSEABLE_ONCE failpoint auto-clears
# enabled = 0 in system.fail_points after natural firing, without requiring
# an explicit SYSTEM DISABLE FAILPOINT.
#
# We use drop_database_before_exclusive_ddl_lock (PAUSEABLE_ONCE), which fires
# inside DROP DATABASE after all tables are processed but before the exclusive
# DDL lock is acquired — a simple, self-contained trigger.
# ──────────────────────────────────────────────────────────────────────────────

FP="drop_database_before_exclusive_ddl_lock"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DROP_DB}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DROP_DB}.t (n UInt64) ENGINE = Memory"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"

# DROP DATABASE fires the failpoint and blocks; run it in the background so we
# can coordinate from this session.
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DROP_DB}" &
DROP_PID=$!

# Wait until the DROP thread has paused at the failpoint.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"

# While paused, the failpoint must be reported as enabled.
$CLICKHOUSE_CLIENT -q "SELECT name, enabled FROM system.fail_points WHERE name = '${FP}'"

# Resume the DROP thread.  notifyPauseAndWaitForResume (in the DROP thread) will
# erase the name from enabled_failpoints and fail_point_wait_channels before
# the operation continues.
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT ${FP}"

# Wait for DROP DATABASE to complete.  Because DROP is synchronous, this
# returns only after the DROP thread has finished — which is after the cleanup
# in notifyPauseAndWaitForResume has already run.
wait $DROP_PID

# After PAUSEABLE_ONCE fires and is resumed, enabled must be 0 automatically —
# no explicit SYSTEM DISABLE FAILPOINT required.
$CLICKHOUSE_CLIENT -q "SELECT name, enabled FROM system.fail_points WHERE name = '${FP}'"

# Leak check: no failpoints must be left enabled at the end of the test.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.fail_points WHERE enabled = 1"

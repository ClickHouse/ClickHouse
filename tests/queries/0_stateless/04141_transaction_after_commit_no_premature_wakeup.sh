#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-parallel
# Tag rationale: enables a server-wide failpoint.
#
# Verifies that a transactional `COMMIT` does not return to the client until every part
# touched by the transaction has its new `creation_csn` / `removal_csn` persisted.
#
# Reproduces the race exposed under `fault_probability_after_commit` on slow storage:
#   - `transaction_force_unknown_state_after_commit` deterministically takes the
#     "connection lost after commit" code path in `TransactionLog::commitTransaction`,
#     so finalization is deferred to `runUpdatingThread`.
#   - `transaction_after_commit_pause` pauses the background thread inside
#     `MergeTreeTransaction::afterCommit` at the boundary between the atomic CSN flip
#     and the per-part metadata writes.
#
# Correct ordering (`afterCommit` does writes before flipping the CSN): during the pause
# the foreground `COMMIT` is still running and `removal_csn` is already visible.
# Buggy ordering (CSN flipped first): during the pause the foreground `COMMIT` has
# already returned and `removal_csn` is still 0.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_after_commit;
    CREATE TABLE t_after_commit (n Int64)
        ENGINE = MergeTree ORDER BY n
        SETTINGS old_parts_lifetime = 3600;
"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_after_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_after_commit VALUES (1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_after_commit VALUES (2)"

# Defense-in-depth: always disable both failpoints on exit so that an early test
# failure (e.g. a `SYSTEM WAIT FAILPOINT ... PAUSE` timeout) cannot leave the
# server-wide failpoints active and disrupt later tests in the same run.
# `SYSTEM DISABLE FAILPOINT` on an already-disabled failpoint is a no-op, so this
# is safe even when the normal cleanup at the end of the script already ran.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_after_commit_pause" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_after_commit_pause"

# Run COMMIT in the background. With the fix, the foreground COMMIT will block
# on `waitStateChange` until the background `afterCommit` releases the pause.
tx_async 1 "BEGIN TRANSACTION"
tx_async 1 "SET throw_on_unsupported_query_inside_transaction = 0"
tx_async 1 "ALTER TABLE t_after_commit DROP PARTITION ID 'all'"
tx_async 1 "COMMIT"

# Deterministically wait until the background thread is paused inside `afterCommit`.
# `SYSTEM WAIT FAILPOINT ... PAUSE` returns immediately once a thread is paused at the
# failpoint, so the subsequent checks see a stable state regardless of host load.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_after_commit_pause PAUSE"

# Check 1: the foreground COMMIT must still be running (paused inside afterCommit).
$CLICKHOUSE_CLIENT -q "
    SELECT 'commit_still_running',
        count() > 0
    FROM system.processes
    WHERE current_database = currentDatabase()
        AND query LIKE '%COMMIT%'
        AND query NOT LIKE '%system.processes%'
"

# Check 2: per-part metadata is already persisted, even though COMMIT has not returned.
$CLICKHOUSE_CLIENT -q "
    SELECT 'removal_csn_visible_during_pause', count()
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_after_commit'
        AND removal_csn > 1
"

# Release the pause; the foreground COMMIT can now finish.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_after_commit_pause"
tx_wait 1

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit"

# Final state: still two parts with removal_csn > 1.
$CLICKHOUSE_CLIENT -q "
    SELECT 'removal_csn_visible_after_commit', count()
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_after_commit'
        AND removal_csn > 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_after_commit"

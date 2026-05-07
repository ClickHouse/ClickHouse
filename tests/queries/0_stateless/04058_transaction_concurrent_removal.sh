#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-replicated-database
# Test: two concurrent transactions that both attempt to remove the same part
# must produce a SERIALIZATION_ERROR for the second one.
# Also tests that after a rollback the removal_tid is reset, so a subsequent
# transaction can remove the same part successfully.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_concurrent_removal;
    CREATE TABLE t_concurrent_removal (n Int64)
        ENGINE = MergeTree ORDER BY n
        SETTINGS old_parts_lifetime=3600;
"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_concurrent_removal"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_concurrent_removal VALUES (1)"

# ------------------------------------------------------------------
# Case 1: two transactions both try to drop the same partition.
# The first one commits; the second one must get SERIALIZATION_ERROR.
# ------------------------------------------------------------------
tx 1 "BEGIN TRANSACTION"
tx 2 "BEGIN TRANSACTION"
tx 1 "ALTER TABLE t_concurrent_removal DROP PARTITION ID 'all'"
# tx2 tries to remove the same part — must fail with SERIALIZATION_ERROR
tx 2 "ALTER TABLE t_concurrent_removal DROP PARTITION ID 'all'" 2>&1 | grep -oE "SERIALIZATION_ERROR" | uniq
tx 1 "COMMIT"
tx 2 "ROLLBACK"

# The part is now removed; re-insert for the next case
$CLICKHOUSE_CLIENT -q "INSERT INTO t_concurrent_removal VALUES (2)"

# ------------------------------------------------------------------
# Case 2: a transaction removes a part and then rolls back; the part
# must be visible again with removal_tid = EmptyTID and removal_csn = 0,
# so a subsequent transaction can remove it successfully.
# ------------------------------------------------------------------
tx 3 "BEGIN TRANSACTION"
tx 3 "ALTER TABLE t_concurrent_removal DROP PARTITION ID 'all'"
tx 3 "ROLLBACK"

# After rollback the active part is restored
$CLICKHOUSE_CLIENT -q "
    SELECT 'restored_after_rollback',
        removal_tid = (0, 0, '00000000-0000-0000-0000-000000000000'),
        removal_csn = 0
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_concurrent_removal'
        AND active
"

# A new transaction can successfully remove the same part
tx 4 "BEGIN TRANSACTION"
tx 4 "ALTER TABLE t_concurrent_removal DROP PARTITION ID 'all'"
tx 4 "COMMIT"

$CLICKHOUSE_CLIENT -q "
    SELECT 'part_removed_by_second_txn', count()
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_concurrent_removal'
        AND active
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_concurrent_removal"

# ------------------------------------------------------------------
# Case 3: two transactions removing different (non-overlapping) parts.
# Both should commit without SERIALIZATION_ERROR because they operate
# on different data objects.
# ------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_noconflict_removal;
    CREATE TABLE t_noconflict_removal (n Int64)
        ENGINE = MergeTree ORDER BY n
        PARTITION BY n % 2
        SETTINGS old_parts_lifetime=3600;
"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_noconflict_removal"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_noconflict_removal VALUES (1)"   # partition 1
$CLICKHOUSE_CLIENT -q "INSERT INTO t_noconflict_removal VALUES (2)"   # partition 0

tx 5 "BEGIN TRANSACTION"
tx 6 "BEGIN TRANSACTION"
# Each transaction removes a distinct partition — no shared parts
tx 5 "ALTER TABLE t_noconflict_removal DROP PARTITION ID '1'"
tx 6 "ALTER TABLE t_noconflict_removal DROP PARTITION ID '0'"
tx 5 "COMMIT"
tx 6 "COMMIT"

$CLICKHOUSE_CLIENT -q "
    SELECT 'noconflict_active_count', count()
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_noconflict_removal'
        AND active
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_noconflict_removal"

#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

# A non-transactional TRUNCATE sees all active parts, including a part whose creating
# transaction has not committed yet. Such a part must NOT be removed: it belongs to a
# concurrent transaction that may still commit. TRUNCATE removes only committed parts.
#
# Each case seeds an already-committed row before TRUNCATE and asserts that it disappears
# while the in-flight row is skipped. This proves the advertised contract ("skip uncommitted
# parts") without weakening ordinary TRUNCATE semantics: the test would fail if the new
# transactions_enabled branch turned TRUNCATE into a no-op, because the committed row would
# then survive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_truncate_uncommitted"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_truncate_uncommitted (x UInt64) ENGINE = MergeTree ORDER BY x"

# Case 1: the creating transaction is rolled back after TRUNCATE.
# TRUNCATE removes the committed row (10) and skips the uncommitted part (1);
# the rollback then removes the uncommitted part -> empty table (0).
$CLICKHOUSE_CLIENT -q "INSERT INTO t_truncate_uncommitted VALUES (10)"
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_truncate_uncommitted SETTINGS async_insert = 0 VALUES (1)"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_uncommitted"
tx 1 "ROLLBACK"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_uncommitted"

# Case 2: the creating transaction is committed after TRUNCATE.
# TRUNCATE removes the committed row (20) and skips the uncommitted part (2);
# after the commit only the previously-uncommitted row survives -> one row, value 2.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_truncate_uncommitted VALUES (20)"
tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO t_truncate_uncommitted SETTINGS async_insert = 0 VALUES (2)"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_uncommitted"
tx 2 "COMMIT"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_uncommitted"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_truncate_uncommitted"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_truncate_uncommitted"

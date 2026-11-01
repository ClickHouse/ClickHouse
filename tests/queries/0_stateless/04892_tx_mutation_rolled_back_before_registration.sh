#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# A transactional mutation rolled back before registration on an empty table is undone once (no-parallel: fail point).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"' EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_tx_rollback_register SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_tx_rollback_register (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation"
tx 1 "BEGIN TRANSACTION" > /dev/null
tid=$(tx 1 "SELECT transactionID()" | cut -f2)
tx_async 1 "ALTER TABLE t_tx_rollback_register UPDATE v = 1 WHERE 1" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_register_mutation PAUSE"
$CLICKHOUSE_CLIENT -q "KILL TRANSACTION WHERE tid = $tid FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
tx_wait 1

$CLICKHOUSE_CLIENT -q "INSERT INTO t_tx_rollback_register VALUES (1, 0)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_tx_rollback_register UPDATE v = 2 WHERE 1 SETTINGS mutations_sync = 1"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_tx_rollback_register"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_tx_rollback_register' AND NOT is_done"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_tx_rollback_register SYNC"

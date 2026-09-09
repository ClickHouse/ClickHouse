#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
#
# Multi-part variant of 05161. A non-transactional `DROP PARTITION` covers two parts and only the
# second one has an in-flight transactional creator. A non-transactional removal is not undoable
# (`removal_csn` is set to `Tx::NonTransactionalCSN` right away and no rollback path clears it), so
# stamping the parts one by one would leave the first, committed part invisible for good even
# though the statement failed with a supposedly retryable `SERIALIZATION_ERROR`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_drop_partition_partial"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_drop_partition_partial (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_drop_partition_partial"

# First part: committed. Second part: created by a transaction that is still running.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_drop_partition_partial SETTINGS async_insert = 0 VALUES (1)"
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_drop_partition_partial SETTINGS async_insert = 0 VALUES (2)"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop_partition_partial DROP PARTITION ALL" 2>&1 | grep -c -F "SERIALIZATION_ERROR"

tx 1 "COMMIT"

# Both rows must still be there: the failed statement removed neither of the two parts.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_drop_partition_partial"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_drop_partition_partial ORDER BY x"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_drop_partition_partial"

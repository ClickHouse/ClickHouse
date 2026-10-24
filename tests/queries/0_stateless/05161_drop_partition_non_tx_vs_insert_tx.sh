#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
#
# A non-transactional remover sees every active part, including one whose creating transaction
# has not committed yet (`creation_csn` is still unset). Stamping such a part with
# `removal_tid = Tx::NonTransactionalTID` immediately sets `removal_csn = Tx::NonTransactionalCSN`,
# producing the `creation_csn = 0, removal_csn = 1` shape that `VersionMetadata::validateInfo`
# rejects as unrecoverable -- a logical error, and a server abort in debug / sanitizer builds.
#
# `TRUNCATE` skips such parts (see 04408), but `DROP PARTITION` and `DETACH PARTITION` do not,
# so the removal must be refused with a retryable `SERIALIZATION_ERROR` instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_drop_partition_non_tx"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_drop_partition_non_tx (x UInt64) ENGINE = MergeTree ORDER BY x"

# Case 1: DROP PARTITION over a part whose creating transaction is still running.
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_drop_partition_non_tx SETTINGS async_insert = 0 VALUES (1)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop_partition_non_tx DROP PARTITION ALL" 2>&1 | grep -o -F "SERIALIZATION_ERROR" | head -1
tx 1 "ROLLBACK"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_drop_partition_non_tx"

# Case 2: DETACH PARTITION goes through the same non-transactional removal path.
tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO t_drop_partition_non_tx SETTINGS async_insert = 0 VALUES (2)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop_partition_non_tx DETACH PARTITION ALL" 2>&1 | grep -o -F "SERIALIZATION_ERROR" | head -1
# The refused DETACH must not leave a copy in `detached/`: the removal is refused under the same parts
# lock that decides it, before anything is cloned, so retrying does not pile up `_tryN` directories.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_drop_partition_non_tx'"
tx 2 "COMMIT"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_drop_partition_non_tx"

# The error is retryable: with no transaction in flight the same statement succeeds.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop_partition_non_tx DROP PARTITION ALL"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_drop_partition_non_tx"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_drop_partition_non_tx"

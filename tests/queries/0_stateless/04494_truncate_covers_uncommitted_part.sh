#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

# A non-transactional TRUNCATE sees all active parts, including a part whose creating
# transaction is not committed yet. Removing such a part sets `removal_csn` to
# `NonTransactionalCSN` while `creation_csn` is still unknown, and this used to throw
# a logical error from `VersionMetadata::validateInfo`:
#   creation_csn is not set while removal_csn is set to 1

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_truncate_uncommitted"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_truncate_uncommitted (x UInt64) ENGINE = MergeTree ORDER BY x"

# Case 1: the creating transaction is rolled back after TRUNCATE.
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_truncate_uncommitted SETTINGS async_insert = 0 VALUES (1)"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_uncommitted"
tx 1 "ROLLBACK"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_uncommitted"

# Case 2: the creating transaction is committed after TRUNCATE.
# The part gets creation_csn > removal_csn == NonTransactionalCSN, which is allowed.
tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO t_truncate_uncommitted SETTINGS async_insert = 0 VALUES (2)"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_uncommitted"
tx 2 "COMMIT"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_uncommitted"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_truncate_uncommitted"

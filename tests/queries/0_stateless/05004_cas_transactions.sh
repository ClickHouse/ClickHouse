#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: cas is an object-storage metadata type; not available on the minimal
#              fasttest image.
# no-ordinary-database: transactions require DatabaseAtomic (or similar); they are not supported
#                       on DatabaseOrdinary.

# CA transactions oracle: proves that transactional INSERT/COMMIT/ROLLBACK works correctly on a
# content-addressed (CA) disk.  Three scenarios are verified:
#   1. A committed transaction's rows become visible after COMMIT.
#   2. A rolled-back transaction's rows are absent after ROLLBACK; prior data is intact.
#   3. Counts are deterministic: base=1, after commit=2, after rollback=2, rolled-back row absent.
#
# MERGES ARE STOPPED immediately after CREATE to prevent any background merge from firing on
# transactional parts during the test (transactional multi-part merges are not yet implemented on
# CA disks — B53 in the backlog).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_txn;"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_cas_txn (k UInt32, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05004',
    name = '05004_cas_transactions',
    path = '05004_cas_transactions_pool/');"

${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_cas_txn;"

# ── Step 1: base row (outside any transaction) ──────────────────────────────
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_txn VALUES (1, 'a');"
${CLICKHOUSE_CLIENT} --query "SELECT 'base', count() FROM t_cas_txn;"

# ── Step 2: committed transaction ───────────────────────────────────────────
# BEGIN … COMMIT must share one client connection (one --query / multiquery block).
${CLICKHOUSE_CLIENT} --query "
BEGIN TRANSACTION;
INSERT INTO t_cas_txn VALUES (2, 'b');
SELECT 'in_txn', count() FROM t_cas_txn;
COMMIT;"

${CLICKHOUSE_CLIENT} --query "SELECT 'after_commit', count() FROM t_cas_txn;"

# ── Step 3: rolled-back transaction ─────────────────────────────────────────
${CLICKHOUSE_CLIENT} --query "
BEGIN TRANSACTION;
INSERT INTO t_cas_txn VALUES (3, 'c');
SELECT 'in_txn2', count() FROM t_cas_txn;
ROLLBACK;"

${CLICKHOUSE_CLIENT} --query "SELECT 'after_rollback', count() FROM t_cas_txn;"
${CLICKHOUSE_CLIENT} --query "SELECT 'rolled_back_absent', count() FROM t_cas_txn WHERE k = 3;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_txn;"
${CLICKHOUSE_CLIENT} --query "SELECT 'done';"

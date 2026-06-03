#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-parallel
#
# A tx INSERT paused before its CSN znode is written (part `Active`, `creation_csn = 0`)
# is covered by a non-tx TRUNCATE. Must fail with `SERIALIZATION_ERROR` instead of
# crashing on `LOGICAL_ERROR`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_truncate_non_tx_vs_insert_tx"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_truncate_non_tx_vs_insert_tx (n Int64)
        ENGINE = MergeTree ORDER BY n
        SETTINGS old_parts_lifetime = 3600
"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_truncate_non_tx_vs_insert_tx"

# Defense-in-depth: clear the failpoint on exit so an early-failure does not leave
# every subsequent transactional commit stuck.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_before_csn_publish" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_before_csn_publish"

# Background: tx INSERT pauses inside `commitTransaction` before the CSN znode is
# written -- the new part is `Active` but `creation_csn == 0`.
$CLICKHOUSE_CLIENT --implicit_transaction 1 --async_insert 0 \
    -q "INSERT INTO t_truncate_non_tx_vs_insert_tx VALUES (1)" &
insert_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_before_csn_publish PAUSE"

# Foreground: non-tx TRUNCATE must fail with SERIALIZATION_ERROR (without the fix
# it would crash the server on a LOGICAL_ERROR instead).
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_non_tx_vs_insert_tx" 2>&1 \
    | grep -o "SERIALIZATION_ERROR" | head -1

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_before_csn_publish"
wait "$insert_pid"

# Sanity: INSERT succeeded; TRUNCATE did not run; one row remains.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_non_tx_vs_insert_tx"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_truncate_non_tx_vs_insert_tx"

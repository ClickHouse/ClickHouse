#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-parallel
#
# Multi-part variant of 04241. A non-tx TRUNCATE covers two parts; only the second
# has an in-flight tx creator. Without the preflight in `addNewPartAndRemoveCovered`,
# the first (committed) part is already stamped with `removal_tid = NonTransactionalTID`
# on disk by the time the second throws `SERIALIZATION_ERROR` -- silently invisible
# after a "retryable" failure.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_truncate_non_tx_partial_removal"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_truncate_non_tx_partial_removal (n Int64)
        ENGINE = MergeTree ORDER BY n
        SETTINGS old_parts_lifetime = 3600
"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_truncate_non_tx_partial_removal"

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_before_csn_publish" 2>/dev/null || true
' EXIT

# First part: fully committed before the failpoint is enabled.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_truncate_non_tx_partial_removal VALUES (1)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_before_csn_publish"

# Second part: tx INSERT pauses before its CSN znode is written.
$CLICKHOUSE_CLIENT --implicit_transaction 1 --async_insert 0 \
    -q "INSERT INTO t_truncate_non_tx_partial_removal VALUES (2)" &
insert_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_before_csn_publish PAUSE"

# TRUNCATE covers both parts. The non-tx remover walks them in order; the second
# triggers `SERIALIZATION_ERROR`. With the preflight, the first must NOT have been
# stamped invisible.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_non_tx_partial_removal" 2>&1 \
    | grep -o "SERIALIZATION_ERROR" | head -1

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_before_csn_publish"
wait "$insert_pid"

# Both committed rows must still be visible.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_non_tx_partial_removal"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_truncate_non_tx_partial_removal"

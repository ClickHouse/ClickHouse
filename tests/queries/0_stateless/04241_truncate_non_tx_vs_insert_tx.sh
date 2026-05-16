#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-parallel
# Tag rationale: uses transactions (no-ordinary-database), enables a server-wide
# failpoint that other parallel tests would observe (no-parallel), and exercises a
# path that the encrypted-disk randomization in the test runner does not currently
# support (no-encrypted-storage).
#
# Verifies the fix for a race between:
#   - a transactional INSERT whose storage `Transaction::commit` has flipped the new
#     part to `Active`, but whose CSN znode has not yet been created in Keeper (i.e.
#     `TransactionLog::getCSN(creation_tid)` still returns 0), and
#   - a non-transactional TRUNCATE that scans the same part and tries to stamp it
#     with `removal_tid = Tx::NonTransactionalTID` (which implies `removal_csn = 1`).
#
# Without the fix, the resulting on-disk shape (`creation_csn = 0, removal_csn = 1`)
# is rejected by `VersionMetadata::validateInfo` and the server aborts on the
# `LOGICAL_ERROR`. With the fix, `setAndStoreRemovalTID` detects the pending creator
# and refuses the write with `SERIALIZATION_ERROR` before validation can run.

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

# Always clear the failpoint on exit. A timed-out `SYSTEM WAIT FAILPOINT ... PAUSE`
# or any other early failure would otherwise leave every subsequent transactional
# commit stuck in `TransactionLog::commitTransaction` for the rest of the run.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_before_csn_publish" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_before_csn_publish"

# Background: implicit-transaction INSERT. Its storage `Transaction::commit` runs
# to completion (the new part is now `Active`), then the implicit `COMMIT` enters
# `TransactionLog::commitTransaction` and pauses at the failpoint -- BEFORE the
# CSN znode is written to Keeper. This is the bug window: the part is visible
# but `creation_csn == 0` and `getCSN(creation_tid)` cannot resolve it.
$CLICKHOUSE_CLIENT --implicit_transaction 1 --async_insert 0 \
    -q "INSERT INTO t_truncate_non_tx_vs_insert_tx VALUES (1)" &
insert_pid=$!

# Wait until the background INSERT has reached the pause.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_before_csn_publish PAUSE"

# Foreground: non-transactional TRUNCATE.
# Without the fix this crashes the server on a LOGICAL_ERROR.
# With the fix it must fail with `SERIALIZATION_ERROR`.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_truncate_non_tx_vs_insert_tx" 2>&1 \
    | grep -oE "SERIALIZATION_ERROR|LOGICAL_ERROR" | head -1

# Release the pause and let the INSERT finish.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_before_csn_publish"
wait "$insert_pid"

# Sanity: INSERT succeeded; TRUNCATE did not run; one row remains.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_truncate_non_tx_vs_insert_tx"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_truncate_non_tx_vs_insert_tx"

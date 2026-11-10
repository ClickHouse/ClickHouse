#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage, no-parallel, no-fasttest
# Tag rationale: enables server-wide failpoints; reads raw metadata files from the data directory.
# Replicated databases route the transactional ALTER UPDATE through replicated DDL, which is refused inside a transaction.
#
# A transient storage error in a metadata write made inside the noexcept commit/rollback callbacks of a
# transaction must be retried instead of terminating the server. Each scenario makes
# the first write of the callback fail exactly once with a ONCE failpoint and checks
# that the statement completed and the retry was logged once. Scenarios A and B also
# check the metadata on disk; scenario C (rollback) checks visibility only, because a
# rolled-back transaction re-derives the same state at load whether or not the write
# landed. Scenarios A-D fail before any I/O; E fails with an errno after the temporary
# file is written, F fails in the directory sync after the rename.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_metadata_store_fail" ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_mutation_csn_store_fail" ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_mutation_csn_store_fail_after_write" ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT local_directory_sync_fail" ||:
}
trap cleanup EXIT

# Prints how many objects were retried, how many were reported stored after a retry, whether the
# two lines name the same object, and the attempt count. The table is matched by its UUID, so a
# rerun in the same database cannot match lines of a previous run.
function report_retry_lines()
{
    local kind=$1
    local table=$2
    local uuid where
    uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '${table}'")
    if [ "$kind" = part ]; then
        where="of ${CLICKHOUSE_DATABASE}.${table} (${uuid})"
    else
        # The mutation file's path is relative to the disk: `store/<uuid prefix>/<uuid>/`.
        where="at store/${uuid:0:3}/${uuid}/"
    fi
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT -q "
        WITH
            (SELECT groupUniqArray(extract(message, 'for (${kind} .+), will retry')) FROM system.text_log
                WHERE event_date >= yesterday() AND message LIKE 'Cannot store transaction metadata for ${kind} % ${where}, will retry%') AS retried,
            (SELECT groupArray(extract(message, 'for (${kind} .+) after [0-9]+ attempts')) FROM system.text_log
                WHERE event_date >= yesterday() AND message LIKE 'Stored transaction metadata for ${kind} % ${where} after % attempts') AS stored,
            (SELECT groupUniqArray(toUInt64OrZero(extract(message, 'after ([0-9]+) attempts'))) FROM system.text_log
                WHERE event_date >= yesterday() AND message LIKE 'Stored transaction metadata for ${kind} % ${where} after % attempts') AS attempts
        SELECT 'retried objects', length(retried), 'stored after retry', length(stored), 'same object', retried = stored, 'attempts', attempts
        FORMAT TSV"
}

function commit_csn()
{
    local tid=$1
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS transactions_info_log"
    $CLICKHOUSE_CLIENT -q "SELECT csn FROM system.transactions_info_log WHERE type = 'Commit' AND tid = ${tid} ORDER BY event_time DESC LIMIT 1"
}

# ---------------------------------------------------------------------------
echo "--- A: commit, part metadata"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_meta_retry;
    CREATE TABLE t_meta_retry (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
    SYSTEM STOP MERGES t_meta_retry;
    INSERT INTO t_meta_retry VALUES (1);
    INSERT INTO t_meta_retry VALUES (2);
"
tx_sync 1 "BEGIN TRANSACTION"
tx_sync 1 "ALTER TABLE t_meta_retry DROP PARTITION ID 'all'"
tx_sync 1 "INSERT INTO t_meta_retry VALUES (3)"
tid=$(tx 1 "SELECT transactionID()" | cut -f2)

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_metadata_store_fail"
tx_sync 1 "COMMIT"

report_retry_lines part t_meta_retry
csn=$(commit_csn "$tid")
part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_meta_retry' AND active")
echo "creation_csn persisted: $(grep -c "^creation_csn: ${csn}$" "${part_path}txn_version.txt")"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after commit', count() FROM t_meta_retry"
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_meta_retry; ATTACH TABLE t_meta_retry"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after reattach', count() FROM t_meta_retry"

# ---------------------------------------------------------------------------
echo "--- B: commit, mutation CSN"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_meta_retry_mut;
    CREATE TABLE t_meta_retry_mut (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_meta_retry_mut VALUES (1, 1), (2, 2), (3, 3);
"
tx_sync 2 "BEGIN TRANSACTION"
tx_sync 2 "ALTER TABLE t_meta_retry_mut UPDATE v = v + 1 WHERE 1"
tid=$(tx 2 "SELECT transactionID()" | cut -f2)

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_mutation_csn_store_fail"
tx_sync 2 "COMMIT"

report_retry_lines mutation t_meta_retry_mut
csn=$(commit_csn "$tid")
data_path=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_meta_retry_mut'")
mutation_id=$($CLICKHOUSE_CLIENT -q "SELECT mutation_id FROM system.mutations WHERE database = currentDatabase() AND table = 't_meta_retry_mut'")
echo "csn lines in mutation file: $(grep -c '^csn: ' "${data_path}${mutation_id}")"
echo "last line is the csn: $([ "$(tail -n 1 "${data_path}${mutation_id}")" == "csn: ${csn}" ] && echo 1 || echo 0)"
$CLICKHOUSE_CLIENT -q "SELECT 'sum after commit', sum(v) FROM t_meta_retry_mut"
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_meta_retry_mut; ATTACH TABLE t_meta_retry_mut"
$CLICKHOUSE_CLIENT -q "SELECT 'sum after reattach', sum(v) FROM t_meta_retry_mut"
$CLICKHOUSE_CLIENT -q "SELECT 'mutations after reattach', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_meta_retry_mut'"

# ---------------------------------------------------------------------------
echo "--- C: rollback, part metadata"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_meta_retry_rb;
    CREATE TABLE t_meta_retry_rb (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
    SYSTEM STOP MERGES t_meta_retry_rb;
    INSERT INTO t_meta_retry_rb VALUES (1);
    INSERT INTO t_meta_retry_rb VALUES (2);
"
tx_sync 3 "BEGIN TRANSACTION"
tx_sync 3 "ALTER TABLE t_meta_retry_rb DROP PARTITION ID 'all'"
tx_sync 3 "INSERT INTO t_meta_retry_rb VALUES (3)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_metadata_store_fail"
tx_sync 3 "ROLLBACK"

report_retry_lines part t_meta_retry_rb
$CLICKHOUSE_CLIENT -q "SELECT 'rows after rollback', count() FROM t_meta_retry_rb"
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_meta_retry_rb; ATTACH TABLE t_meta_retry_rb"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after reattach', count() FROM t_meta_retry_rb"

# ---------------------------------------------------------------------------
echo "--- D: rollback, removal lock"
# The transaction only removes parts and creates none, so the first metadata write of the
# rollback is the clear of the removal lock and the ONCE failpoint fires there.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_meta_retry_lock;
    CREATE TABLE t_meta_retry_lock (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
    SYSTEM STOP MERGES t_meta_retry_lock;
    INSERT INTO t_meta_retry_lock VALUES (1);
    INSERT INTO t_meta_retry_lock VALUES (2);
"
tx_sync 4 "BEGIN TRANSACTION"
tx_sync 4 "ALTER TABLE t_meta_retry_lock DROP PARTITION ID 'all'"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_metadata_store_fail"
tx_sync 4 "ROLLBACK"

report_retry_lines part t_meta_retry_lock
$CLICKHOUSE_CLIENT -q "SELECT 'active parts after rollback', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_meta_retry_lock' AND active"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after rollback', count() FROM t_meta_retry_lock"
# Fails with a removal conflict if a part is still locked by the rolled-back transaction.
tx_sync 5 "BEGIN TRANSACTION"
tx_sync 5 "ALTER TABLE t_meta_retry_lock DROP PARTITION ID 'all'"
tx_sync 5 "COMMIT"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after later drop', count() FROM t_meta_retry_lock"

# ---------------------------------------------------------------------------
echo "--- E: commit, mutation CSN, ENOSPC after the write"
# The failure is an `ErrnoException` raised after the temporary file is complete: the retry
# must overwrite it, and the loader must still see exactly one `csn:` line.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_meta_retry_mut_errno;
    CREATE TABLE t_meta_retry_mut_errno (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_meta_retry_mut_errno VALUES (1, 1), (2, 2), (3, 3);
"
tx_sync 6 "BEGIN TRANSACTION"
tx_sync 6 "ALTER TABLE t_meta_retry_mut_errno UPDATE v = v + 1 WHERE 1"
tid=$(tx 6 "SELECT transactionID()" | cut -f2)

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_mutation_csn_store_fail_after_write"
tx_sync 6 "COMMIT"

report_retry_lines mutation t_meta_retry_mut_errno
csn=$(commit_csn "$tid")
data_path=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_meta_retry_mut_errno'")
mutation_id=$($CLICKHOUSE_CLIENT -q "SELECT mutation_id FROM system.mutations WHERE database = currentDatabase() AND table = 't_meta_retry_mut_errno'")
echo "csn lines in mutation file: $(grep -c '^csn: ' "${data_path}${mutation_id}")"
echo "last line is the csn: $([ "$(tail -n 1 "${data_path}${mutation_id}")" == "csn: ${csn}" ] && echo 1 || echo 0)"
echo "temporary files left: $(find "${data_path}" -maxdepth 1 -name 'tmp_mutation_*' | wc -l)"
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_meta_retry_mut_errno; ATTACH TABLE t_meta_retry_mut_errno"
$CLICKHOUSE_CLIENT -q "SELECT 'sum after reattach', sum(v) FROM t_meta_retry_mut_errno"
$CLICKHOUSE_CLIENT -q "SELECT 'mutations after reattach', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_meta_retry_mut_errno'"

# ---------------------------------------------------------------------------
echo "--- F: commit, part metadata, EIO on the directory sync"
# With `fsync_part_directory` the rename of `txn_version.txt` is followed by a sync of the part
# directory. The failure is injected inside the sync itself: it must reach the retry, and if it
# were left to the guard's destructor it would be logged, the write would pass, and no retry line
# would appear.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_meta_retry_fsync;
    CREATE TABLE t_meta_retry_fsync (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600, fsync_part_directory = 1;
    SYSTEM STOP MERGES t_meta_retry_fsync;
    INSERT INTO t_meta_retry_fsync VALUES (1);
    INSERT INTO t_meta_retry_fsync VALUES (2);
"
tx_sync 7 "BEGIN TRANSACTION"
tx_sync 7 "ALTER TABLE t_meta_retry_fsync DROP PARTITION ID 'all'"
tx_sync 7 "INSERT INTO t_meta_retry_fsync VALUES (3)"
tid=$(tx 7 "SELECT transactionID()" | cut -f2)

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT local_directory_sync_fail"
tx_sync 7 "COMMIT"

report_retry_lines part t_meta_retry_fsync
csn=$(commit_csn "$tid")
part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_meta_retry_fsync' AND active")
echo "creation_csn persisted: $(grep -c "^creation_csn: ${csn}$" "${part_path}txn_version.txt")"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after commit', count() FROM t_meta_retry_fsync"
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_meta_retry_fsync; ATTACH TABLE t_meta_retry_fsync"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after reattach', count() FROM t_meta_retry_fsync"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_meta_retry; DROP TABLE t_meta_retry_mut; DROP TABLE t_meta_retry_rb; DROP TABLE t_meta_retry_lock; DROP TABLE t_meta_retry_mut_errno; DROP TABLE t_meta_retry_fsync"

#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- the failpoint is server-wide and fires for every table, system logs included.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="claim_inject_stale_part_dir"
TABLE_PLAIN="t_merge_stale_plain"
TABLE_PACKED="t_merge_stale_packed"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_PLAIN SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_PACKED SYNC" 2>/dev/null
}
trap cleanup EXIT

# The failpoint injects a non-empty `tmp_merge_<part>` directory inside the claim, as an interrupted
# merge would leave behind. This used to fail the merge with `DIRECTORY_ALREADY_EXISTS` until
# `temporary_directories_lifetime` expired; now it is reclaimed. Arm the server-wide failpoint for
# this one query only; `send_logs_level=error` hides the expected reclaim warning.
function run_case()
{
    local table=$1

    # Stop merges so the two inserts deterministically leave two parts for the OPTIMIZE to merge.
    # The stop is table-scoped, so it dies with the table and needs no extra cleanup.
    $CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES $table"
    $CLICKHOUSE_CLIENT --query "INSERT INTO $table SELECT number FROM numbers(50)"
    $CLICKHOUSE_CLIENT --query "INSERT INTO $table SELECT number + 50 FROM numbers(50)"

    $CLICKHOUSE_CLIENT --send_logs_level=error --multiquery --query "
    SYSTEM ENABLE FAILPOINT $FP;
    SYSTEM START MERGES $table;
    OPTIMIZE TABLE $table FINAL SETTINGS optimize_throw_if_noop = 1;
    SYSTEM DISABLE FAILPOINT $FP;
    "

    $CLICKHOUSE_CLIENT --query "
        SELECT count(), sum(a), (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active) FROM $table"

    # Verify the reclaim warning was logged for a tmp_merge_ directory of this table.
    local found=0
    for _ in {1..10}
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        found=$($CLICKHOUSE_CLIENT --query "
            SELECT count() > 0 FROM system.text_log
            WHERE startsWith(logger_name, currentDatabase() || '.$table')
              AND message LIKE '%Removing stale temporary directory%'
              AND message LIKE '%/tmp_merge_%'
        ")
        [[ $found == 1 ]] && break
        sleep 0.5
    done

    if [[ $found == 1 ]]
    then
        echo "tmp_merge_ reclaim warning found"
    else
        echo "tmp_merge_ reclaim warning NOT found, messages logged for the table:"
        $CLICKHOUSE_CLIENT --query "
            SELECT logger_name, message FROM system.text_log
            WHERE startsWith(logger_name, currentDatabase() || '.$table') ORDER BY event_time_microseconds"
    fi
}

echo "plain"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_PLAIN SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE_PLAIN (a UInt64) ENGINE = MergeTree ORDER BY a"
run_case "$TABLE_PLAIN"

echo "packed"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_PACKED SYNC"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE $TABLE_PACKED (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_full_part_storage = 1073741824"
run_case "$TABLE_PACKED"

# All parts of the packed table (including the merged one) must use packed storage.
$CLICKHOUSE_CLIENT --query "
    SELECT DISTINCT part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE_PACKED' AND active"

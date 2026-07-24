#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- enables a server-wide failpoint that fires for every claimed or reclaimed
# temporary part directory of every table (including system log table flushes).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="claim_inject_stale_part_dir"
TABLE_SRC="t_clone_stale_src"
TABLE_DST="t_clone_stale_dst"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_SRC SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_DST SYNC" 2>/dev/null
}
trap cleanup EXIT

# The clone-based write paths (`cloneAndLoadDataPart`: REPLACE PARTITION, MOVE PARTITION TO TABLE)
# call `reclaimStaleTemporaryPartDirectory` directly, per destination disk. The failpoint fires
# inside the reclaim and injects a pre-existing non-empty `tmp_replace_from_<part>` /
# `tmp_move_from_<part>` directory in the destination table, right before the removal probe,
# simulating a stale leftover of a previously interrupted clone. The reclaim must remove the stale
# directory and the operation must succeed. Enable, run the operation and disable in a single
# client invocation so the server-wide failpoint is armed only for that one operation.
# send_logs_level=error hides the expected "Removing stale temporary directory" warning from stderr.
function check_reclaim_warning()
{
    local table=$1
    local prefix=$2

    local found=0
    for _ in {1..10}
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        found=$($CLICKHOUSE_CLIENT --query "
            SELECT count() > 0 FROM system.text_log
            WHERE startsWith(logger_name, currentDatabase() || '.$table')
              AND message LIKE '%Removing stale temporary directory%'
              AND message LIKE '%/$prefix%'
        ")
        [[ $found == 1 ]] && break
        sleep 0.5
    done

    if [[ $found == 1 ]]
    then
        echo "$prefix reclaim warning found"
    else
        echo "$prefix reclaim warning NOT found, messages logged for the table:"
        $CLICKHOUSE_CLIENT --query "
            SELECT logger_name, message FROM system.text_log
            WHERE startsWith(logger_name, currentDatabase() || '.$table') ORDER BY event_time_microseconds"
    fi
}

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_SRC SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_DST SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE_SRC (p UInt8, a UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY a"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE_DST (p UInt8, a UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY a"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE_SRC SELECT 1, number FROM numbers(100)"

echo "replace partition"
$CLICKHOUSE_CLIENT --send_logs_level=error --multiquery --query "
SYSTEM ENABLE FAILPOINT $FP;
ALTER TABLE $TABLE_DST REPLACE PARTITION 1 FROM $TABLE_SRC;
SYSTEM DISABLE FAILPOINT $FP;
"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(a) FROM $TABLE_DST"
check_reclaim_warning "$TABLE_DST" "tmp_replace_from_"

# Refill the source with a different partition and move it to the destination table.
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE_SRC SELECT 2, number + 100 FROM numbers(100)"

echo "move partition to table"
$CLICKHOUSE_CLIENT --send_logs_level=error --multiquery --query "
SYSTEM ENABLE FAILPOINT $FP;
ALTER TABLE $TABLE_SRC MOVE PARTITION 2 TO TABLE $TABLE_DST;
SYSTEM DISABLE FAILPOINT $FP;
"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(a) FROM $TABLE_DST"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $TABLE_SRC WHERE p = 2"
check_reclaim_warning "$TABLE_DST" "tmp_move_from_"

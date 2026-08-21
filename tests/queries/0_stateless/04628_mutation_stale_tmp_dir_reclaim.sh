#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- the failpoint is server-wide and fires for every table, system logs included.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="claim_inject_stale_part_dir"
TABLE="t_mutation_stale_tmp_reclaim"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE SELECT number, number FROM numbers(100)"

# The failpoint injects a non-empty `tmp_mut_<part>` directory inside the claim, as an interrupted
# mutation would leave behind. The claim must remove it and the mutation must succeed. Arm the
# server-wide failpoint for this one query only; `send_logs_level=error` hides the reclaim warning.
$CLICKHOUSE_CLIENT --send_logs_level=error --mutations_sync=2 --multiquery --query "
SYSTEM ENABLE FAILPOINT $FP;
ALTER TABLE $TABLE UPDATE v = v + 1 WHERE 1;
SYSTEM DISABLE FAILPOINT $FP;
"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(v) FROM $TABLE"

# Verify the reclaim warning was logged for a tmp_mut_ directory of this table.
found=0
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    found=$($CLICKHOUSE_CLIENT --query "
        SELECT count() > 0 FROM system.text_log
        WHERE startsWith(logger_name, currentDatabase() || '.$TABLE')
          AND message LIKE '%Removing stale temporary directory%'
          AND message LIKE '%/tmp_mut_%'
    ")
    [[ $found == 1 ]] && break
    sleep 0.5
done

if [[ $found == 1 ]]
then
    echo "tmp_mut_ reclaim warning found"
else
    echo "tmp_mut_ reclaim warning NOT found, messages logged for the table:"
    $CLICKHOUSE_CLIENT --query "
        SELECT logger_name, message FROM system.text_log
        WHERE startsWith(logger_name, currentDatabase() || '.$TABLE') ORDER BY event_time_microseconds"
fi

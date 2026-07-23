#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- enables a server-wide failpoint that fires for every claimed temporary part
# directory of every table (including system log table flushes).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="claim_inject_stale_part_dir"
TABLE="t_insert_stale_tmp_reclaim"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE (a UInt64) ENGINE = MergeTree ORDER BY a"

# The failpoint fires inside `claimTemporaryPartDirectory` and injects a pre-existing non-empty
# `tmp_insert_<part>` directory under the claim, right before the reclaim, simulating a stale leftover of a previously
# interrupted insert. The claim must reclaim (remove) the stale directory and the INSERT must
# succeed. Enable, insert and disable in a single client invocation so the server-wide failpoint is
# armed only for this one INSERT. send_logs_level=error hides the expected "Removing stale temporary
# directory" warning from stderr.
$CLICKHOUSE_CLIENT --send_logs_level=error --multiquery --query "
SYSTEM ENABLE FAILPOINT $FP;
INSERT INTO $TABLE SELECT number FROM numbers(100);
SYSTEM DISABLE FAILPOINT $FP;
"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(a) FROM $TABLE"

# Verify the reclaim warning was logged for a tmp_insert_ directory of this table.
found=0
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    found=$($CLICKHOUSE_CLIENT --query "
        SELECT count() > 0 FROM system.text_log
        WHERE startsWith(logger_name, currentDatabase() || '.$TABLE')
          AND message LIKE '%Removing stale temporary directory%'
          AND message LIKE '%/tmp_insert_%'
    ")
    [[ $found == 1 ]] && break
    sleep 0.5
done

if [[ $found == 1 ]]
then
    echo "tmp_insert_ reclaim warning found"
else
    echo "tmp_insert_ reclaim warning NOT found, messages logged for the table:"
    $CLICKHOUSE_CLIENT --query "
        SELECT logger_name, message FROM system.text_log
        WHERE startsWith(logger_name, currentDatabase() || '.$TABLE') ORDER BY event_time_microseconds"
fi

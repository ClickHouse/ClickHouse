#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings
# Tag no-parallel: uses a PAUSEABLE failpoint whose channel is global to the server, so concurrent
# test instances would interfere with each other's ENABLE/DISABLE/WAIT sequence.
# Tag no-random-settings: `async_insert_parse_threads` is randomized by clickhouse-test, and this test
# sets it explicitly.

# Regression test: when a flush with `async_insert_parse_threads > 1` is killed while some of its
# entries are already parsed, those entries must still be recorded in `system.asynchronous_insert_log`
# as `FlushError` with the cancellation exception, exactly as when the flushing thread parses every
# entry itself. The entries that were never parsed are not logged, as before.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TABLE="t_parse_threads_kill_${CLICKHOUSE_DATABASE}"
FP=async_insert_parse_pause_before_next_entry
QUERY_ID_PREFIX="parse_threads_kill_${CLICKHOUSE_DATABASE}_$$"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_kind = 'AsyncInsertFlush' AND query LIKE '%$TABLE%' SYNC FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE TABLE $TABLE (x UInt64) ENGINE = MergeTree ORDER BY x"

# The settings are part of the key of the asynchronous insert queue, so the three inserts below are
# collected into a single batch, which is flushed explicitly. With two parse threads the batch is
# split into two slices: the first two entries go to the slice parsed by the flushing thread, the
# third one to the slice parsed by the pool.
url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_ms=600000&async_insert_max_data_size=1000000000&async_insert_max_query_number=1000000&async_insert_parse_threads=2"

for i in 1 2 3
do
    ${CLICKHOUSE_CURL} -sS "${url}&query_id=${QUERY_ID_PREFIX}_${i}" -d "INSERT INTO $TABLE FORMAT JSONEachRow {\"x\": ${i}}"
done

# A slice pauses at the failpoint before every entry but its first one, so the flush parks right
# after the first entry of the batch has been parsed and before the second one.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE $TABLE" > /dev/null 2>&1 &
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"

# Kill only the background flush, without waiting for it to exit.
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_kind = 'AsyncInsertFlush' AND query LIKE '%$TABLE%' FORMAT Null"

# Release the parked slice: it observes the kill before parsing its second entry and aborts the flush.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait 2>/dev/null ||:

# The first entry was parsed before the kill and must be logged as `FlushError` with the cancellation
# exception. The second one was never parsed, so it is not logged at all. The third one raced with
# the kill on the other slice, so it is either logged like the first one or not logged, and the
# test does not look at it.
for _ in {1..100}
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    logged=$($CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE query_id = '${QUERY_ID_PREFIX}_1'
    ")
    if [[ "$logged" -ge 1 ]]
    then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(query_id, '${QUERY_ID_PREFIX}_', 'entry '),
        status,
        rows,
        exception LIKE '%Format streaming was cancelled%'
    FROM system.asynchronous_insert_log
    WHERE query_id IN ('${QUERY_ID_PREFIX}_1', '${QUERY_ID_PREFIX}_2')
    ORDER BY query_id
"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE"

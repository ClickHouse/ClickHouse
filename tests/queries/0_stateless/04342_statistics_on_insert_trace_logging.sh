#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest - uses `system.text_log`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

table=statistics_on_insert_trace_logging
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"

# NOTE: boost multitoken (--allow_repeated_settings) could prefer "first"
# instead of "last" value, hence you cannot simply append another
# --send_logs_level here.
CLICKHOUSE_CLIENT_TRACE=$(echo "${CLICKHOUSE_CLIENT}" | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
    SET allow_statistics = 1;
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table
    (
        a UInt64 STATISTICS(minmax, uniq),
        b Nullable(UInt32) STATISTICS(basic),
        c String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS auto_statistics_types = '';
"

$CLICKHOUSE_CLIENT_TRACE --query_id "$query_id" -q "
    INSERT INTO $table
    SELECT number, if(number % 5 = 0, NULL, number % 7), toString(number)
    FROM numbers(1000)
    SETTINGS materialize_statistics_on_insert = 1
" >/dev/null 2>&1

for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    count=$($CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "
        SELECT count()
        FROM system.text_log
        WHERE event_date >= yesterday()
            AND event_time >= now() - 600
            AND query_id = {query_id:String}
            AND level = 'Trace'
            AND message LIKE 'Calculated insert-time statistics:%'
        SETTINGS max_rows_to_read = 0
    ")

    [ "$count" -ge 3 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "
    SELECT
        count() = 3,
        countIf(message LIKE concat('%query_id: ', {query_id:String}, ',%')) = 3,
        countIf(message LIKE '%table: %statistics_on_insert_trace_logging, column:%') = 3,
        countIf(message LIKE '%column: a, data_type: UInt64, physical_type: %, statistics_kind: minmax, rows: 1000, bytes: %, elapsed_us: %') = 1,
        countIf(message LIKE '%column: a, data_type: UInt64, physical_type: %, statistics_kind: uniq, rows: 1000, bytes: %, elapsed_us: %, distinct_values: %') = 1,
        countIf(message LIKE '%column: b, data_type: Nullable(UInt32), physical_type: %, statistics_kind: basic, rows: 1000, bytes: %, elapsed_us: %') = 1
    FROM system.text_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND query_id = {query_id:String}
        AND level = 'Trace'
        AND message LIKE 'Calculated insert-time statistics:%'
    SETTINGS max_rows_to_read = 0
"

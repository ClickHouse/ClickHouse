#!/usr/bin/env bash
set -e

# Get all server logs
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="trace"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

cur_name=$(basename "${BASH_SOURCE[0]}")
server_logs_file=${CLICKHOUSE_TMP}/$cur_name"_server.logs"

server_logs="--server_logs_file=$server_logs_file"
rm -f "$server_logs_file"

settings="$server_logs --log_queries=1 --log_query_threads=1 --log_profile_events=1 --log_query_settings=1"


# Test insert logging on each block and checkPacket() method

$CLICKHOUSE_CLIENT $settings -n -q "
DROP TABLE IF EXISTS null;
CREATE TABLE null (i UInt8) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();"

head -c 1000 /dev/zero | $CLICKHOUSE_CLIENT $settings --max_insert_block_size=10 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 -q "INSERT INTO null FORMAT RowBinary"

$CLICKHOUSE_CLIENT $settings -n -q "
SELECT count() FROM null;
DROP TABLE null;"

(( `cat "$server_logs_file" | wc -l` >= 110 )) || echo Fail


# Check ProfileEvents in query_log

heavy_cpu_query="SELECT ignore(sum(sipHash64(hex(sipHash64(hex(sipHash64(hex(number)))))))) FROM (SELECT * FROM system.numbers_mt LIMIT 1000000)"
$CLICKHOUSE_CLIENT $settings --max_threads=1 -q "$heavy_cpu_query"
$CLICKHOUSE_CLIENT $settings -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT $settings -q "
WITH
    any(query_duration_ms*1000) AS duration,
    sumIf(PV, PN = 'RealTimeMicroseconds') AS threads_realtime,
    sumIf(PV, PN IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds', 'OSIOWaitMicroseconds', 'OSCPUWaitMicroseconds')) AS threads_time_user_system_io
SELECT
    -- duration, threads_realtime, threads_time_user_system_io,
    threads_realtime >= 0.99 * duration,
    threads_realtime >= threads_time_user_system_io,
    any(length(thread_numbers)) >= 1
    FROM
        (SELECT * FROM system.query_log PREWHERE query='$heavy_cpu_query' WHERE event_date >= today()-1 AND type=2 ORDER BY event_time DESC LIMIT 1)
    ARRAY JOIN ProfileEvents.Names AS PN, ProfileEvents.Values AS PV"


# Check ProfileEvents in query_thread_log

$CLICKHOUSE_CLIENT $settings --max_threads=3 -q "$heavy_cpu_query"
$CLICKHOUSE_CLIENT $settings -q "SYSTEM FLUSH LOGS"
query_id=`$CLICKHOUSE_CLIENT $settings -q "SELECT query_id FROM system.query_log WHERE event_date >= today()-1 AND type=2 AND query='$heavy_cpu_query' ORDER BY event_time DESC LIMIT 1"`
query_elapsed=`$CLICKHOUSE_CLIENT $settings -q "SELECT query_duration_ms*1000 FROM system.query_log WHERE event_date >= today()-1 AND type=2 AND query_id='$query_id' ORDER BY event_time DESC LIMIT 1"`
threads=`$CLICKHOUSE_CLIENT $settings -q "SELECT length(thread_numbers) FROM system.query_log WHERE event_date >= today()-1 AND type=2 AND query_id='$query_id' ORDER BY event_time DESC LIMIT 1"`

$CLICKHOUSE_CLIENT $settings -q "
SELECT
    -- max(thread_realtime), $query_elapsed, max(thread_time_user_system_io),
    0.9 * $query_elapsed <= max(thread_realtime) AND max(thread_realtime) <= 1.1 * $query_elapsed,
    0.7 * $query_elapsed <= max(thread_time_user_system_io) AND max(thread_time_user_system_io) <= 1.3 * $query_elapsed,
    uniqExact(thread_number) = $threads
FROM
(
    SELECT
        thread_number,
        sumIf(PV, PN = 'RealTimeMicroseconds') AS thread_realtime,
        sumIf(PV, PN IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds', 'OSIOWaitMicroseconds', 'OSCPUWaitMicroseconds')) AS thread_time_user_system_io
        FROM
            (SELECT * FROM system.query_thread_log PREWHERE query_id='$query_id' WHERE event_date >= today()-1)
        ARRAY JOIN ProfileEvents.Names AS PN, ProfileEvents.Values AS PV
        GROUP BY thread_number
)
"

# Check per-thread and per-query ProfileEvents consistency

$CLICKHOUSE_CLIENT $settings -q "
SELECT PN, PVq, PVt FROM
(
    SELECT PN, sum(PV) AS PVt
    FROM system.query_thread_log
    ARRAY JOIN ProfileEvents.Names AS PN, ProfileEvents.Values AS PV
    WHERE event_date >= today()-1 AND query_id='$query_id'
    GROUP BY PN
)
ANY INNER JOIN
(
    SELECT PN, PV AS PVq
    FROM system.query_log
    ARRAY JOIN ProfileEvents.Names AS PN, ProfileEvents.Values AS PV
    WHERE event_date >= today()-1 AND query_id='$query_id'
)
USING PN
WHERE
    NOT PN IN ('ContextLock') AND
    NOT (PVq <= PVt AND PVt <= 1.1 * PVq)
"

# Clean
rm "$server_logs_file"

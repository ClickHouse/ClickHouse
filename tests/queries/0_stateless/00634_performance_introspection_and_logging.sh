#!/usr/bin/env bash
set -e

# Get all server logs
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="trace"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cur_name=$(basename "${BASH_SOURCE[0]}")
server_logs_file=${CLICKHOUSE_TMP}/$cur_name"_server.logs"

server_logs="--server_logs_file=$server_logs_file"
rm -f "$server_logs_file"

settings="$server_logs --log_queries=1 --log_query_threads=1 --log_profile_events=1 --log_query_settings=1"


# Test insert logging on each block and checkPacket() method

$CLICKHOUSE_CLIENT $settings -q "
DROP TABLE IF EXISTS null_00634;
CREATE TABLE null_00634 (i UInt8) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();"

head -c 1000 /dev/zero | $CLICKHOUSE_CLIENT $settings --max_insert_block_size=10 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 -q "INSERT INTO null_00634 FORMAT RowBinary"

$CLICKHOUSE_CLIENT $settings -q "
SELECT count() FROM null_00634;
DROP TABLE null_00634;"

(( $(cat "$server_logs_file" | wc -l) >= 110 )) || echo Fail


# Check ProfileEvents in query_log

heavy_cpu_query="SELECT ignore(sum(sipHash64(hex(sipHash64(hex(sipHash64(hex(number)))))))) FROM (SELECT * FROM system.numbers_mt LIMIT 1000000)"
$CLICKHOUSE_CLIENT $settings --max_threads=1 -q "$heavy_cpu_query"
$CLICKHOUSE_CLIENT $settings -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT $settings -q "
WITH
    any(query_duration_ms*1000) AS duration,
    sum(ProfileEvents['RealTimeMicroseconds']) AS threads_realtime,
    sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] + ProfileEvents['OSIOWaitMicroseconds'] + ProfileEvents['OSCPUWaitMicroseconds']) AS threads_time_user_system_io
SELECT
    -- duration, threads_realtime, threads_time_user_system_io,
    threads_realtime >= 0.99 * duration,
    threads_realtime >= threads_time_user_system_io,
    any(length(thread_ids)) >= 1
    FROM
        (SELECT * FROM system.query_log PREWHERE query='$heavy_cpu_query' WHERE event_date >= today()-1 AND current_database = currentDatabase() AND type=2 ORDER BY event_time DESC LIMIT 1)
"

# Clean
rm "$server_logs_file"

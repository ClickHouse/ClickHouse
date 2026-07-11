#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Framing formats send server logs (with the `send_logs_level` setting) and profile events
# as packets multiplexed in the HTTP response stream. Their number and contents depend on timing,
# so only the presence and the structure are checked.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"

result_file="${CLICKHOUSE_TMP}/framing_packets_$$.ndjson"

echo '--- profile events packets'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT sum(number) FROM numbers(1000000) FORMAT JSONEachRow" > "$result_file"
[ "$(grep -c '"packet":"profile_events"' "$result_file")" -ge 1 ] && echo 'profile_events packets: OK'
grep -o -m1 '"packet":"profile_events","profile_events":\[{"host_name":' "$result_file" | head -1
grep -o '"type":"increment"' "$result_file" | head -1
rm "$result_file"

echo '--- log packets'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace" \
    -d "SELECT sum(number) FROM numbers_mt(1000000) GROUP BY number % 10 FORMAT Null" > "$result_file"
[ "$(grep -c '"packet":"log"' "$result_file")" -ge 1 ] && echo 'log packets: OK'
grep -o -m1 '"packet":"log","log":{"event_time":' "$result_file"
rm "$result_file"

echo '--- log packets in EventStream'
log_events=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&send_logs_level=trace" \
    -d "SELECT sum(number) FROM numbers_mt(1000000) GROUP BY number % 10 FORMAT Null" | grep -c '^event: log')
[ "$log_events" -ge 1 ] && echo 'log events: OK'

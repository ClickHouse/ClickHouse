#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Framing formats send server logs (with the `send_logs_level` setting) and profile events
# as packets multiplexed in the HTTP response stream. Their number and contents depend on timing,
# so only the presence and the structure are checked.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
# The buffered path (`http_wait_end_of_query=1`) discards all output and rebuilds the framing format
# for the exception; it must still drain the log / profile-events queues collected before the failure.
WAIT_URL="${CLICKHOUSE_URL}&http_wait_end_of_query=1&output_format_parallel_formatting=0"

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

# In `EventStream`, a `profile_events` batch with more than one row must be serialized as a single JSON
# array in one `data:` field. An SSE client reconstructs `event.data` by joining consecutive `data:`
# fields with '\n', so multiple fields would produce `{...}\n{...}`, which is not valid JSON.
echo '--- profile events in EventStream are a single JSON array'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream" \
    -d "SELECT sum(number) FROM numbers(1000000) FORMAT Null" \
    | awk '
        /^event: profile_events$/ { in_pe = 1; fields = 0; data = ""; next }
        in_pe && /^data: / { fields++; data = substr($0, 7); next }
        in_pe && /^$/ { seen = 1; if (fields != 1 || data !~ /^\[.*\]$/) bad = 1; in_pe = 0; next }
        END { if (seen && !bad) print "profile_events single JSON array: OK"; else print "MISMATCH" }'

# The logs and profile-events queues are attached before the query is interpreted, so packets emitted
# during parsing and planning are captured even when the query fails before producing any output. A
# query that parses but fails during analysis (an unknown table) still logs the query text at the trace
# level, and the framed response must carry those `log` packets alongside the final `exception` packet.
echo '--- planning-phase logs are framed when the query fails before producing output'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace" \
    -d "SELECT * FROM table_that_does_not_exist_04513" > "$result_file"
[ "$(grep -c '"packet":"log"' "$result_file")" -ge 1 ] && echo 'planning-phase log packets: OK'
[ "$(grep -c '"packet":"exception"' "$result_file")" -ge 1 ] && echo 'exception packet: OK'
rm "$result_file"

# The buffered path (`http_wait_end_of_query=1`) throws away everything buffered before the failure and
# recreates the framing format for the exception. It must carry over the queues attached during parsing
# and planning, so the framed response still delivers the `log` packets, not only the `exception` packet.
echo '--- planning-phase logs are framed with wait_end_of_query when the query fails before producing output'
${CLICKHOUSE_CURL} -sS "${WAIT_URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace" \
    -d "SELECT * FROM table_that_does_not_exist_04513" > "$result_file"
[ "$(grep -c '"packet":"log"' "$result_file")" -ge 1 ] && echo 'wait_end_of_query planning-phase log packets: OK'
[ "$(grep -c '"packet":"exception"' "$result_file")" -ge 1 ] && echo 'wait_end_of_query exception packet: OK'
rm "$result_file"

# The framing / logs / profile-events settings can be set by the query's own SETTINGS clause, which is
# applied only after parsing. The queues are reconciled with the effective settings after the query is
# interpreted, so a query that enables framing (and logs) from its SETTINGS clause - while the URL keeps
# the default `framing_output_format=None` - still gets its `profile_events` and `log` packets.
echo '--- profile events packets when framing is enabled by the query SETTINGS clause'
${CLICKHOUSE_CURL} -sS "${URL}" \
    -d "SELECT sum(number) FROM numbers(1000000) SETTINGS framing_output_format='JSONEachPacketString' FORMAT JSONEachRow" > "$result_file"
[ "$(grep -c '"packet":"profile_events"' "$result_file")" -ge 1 ] && echo 'query SETTINGS profile_events packets: OK'
rm "$result_file"

echo '--- log packets when framing and logs are enabled by the query SETTINGS clause'
${CLICKHOUSE_CURL} -sS "${URL}" \
    -d "SELECT sum(number) FROM numbers_mt(1000000) GROUP BY number % 10 SETTINGS framing_output_format='JSONEachPacketString', send_logs_level='trace' FORMAT Null" > "$result_file"
[ "$(grep -c '"packet":"log"' "$result_file")" -ge 1 ] && echo 'query SETTINGS log packets: OK'
rm "$result_file"

# The inverse override must not keep queues that nobody drains: a session / URL that enables framing (or
# the logs) but a query that disables it in its own SETTINGS clause must produce plain, unframed output
# (respectively, no `log` packets), because the queues are dropped once the effective settings are known.
echo '--- the query SETTINGS clause can disable framing enabled by the URL'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 AS x SETTINGS framing_output_format='None' FORMAT JSONEachRow" > "$result_file"
[ "$(grep -c '"packet":' "$result_file")" -eq 0 ] && echo 'query SETTINGS framing None disables framing: OK'
cat "$result_file"
rm "$result_file"

echo '--- the query SETTINGS clause can disable logs enabled by the URL while framing stays on'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace" \
    -d "SELECT sum(number) FROM numbers_mt(1000000) GROUP BY number % 10 SETTINGS send_logs_level='none' FORMAT Null" > "$result_file"
[ "$(grep -c '"packet":"log"' "$result_file")" -eq 0 ] && echo 'query SETTINGS send_logs_level none drops log packets: OK'
rm "$result_file"

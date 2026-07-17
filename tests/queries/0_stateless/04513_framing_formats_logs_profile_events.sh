#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the finalization-failure section enables the `framing_finalize_throw` fail point,
# which affects the whole server. It fires on the next framing-format finalization anywhere on the
# server, so a concurrent framing query from another test (e.g. `04512_framing_formats`) could consume
# the injected fault - making this test miss its own exception packet and the other test throw
# spuriously. Running the test alone keeps the fault scoped to this test's own query.

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
header_file="${CLICKHOUSE_TMP}/framing_headers_$$.txt"

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

# Auxiliary packets (`log`, `profile_events`, `exception`) are always JSON, unlike the query result
# payload, which - depending on the framing format - may embed non-UTF-8 bytes verbatim. Some of their
# string fields, such as `query_id` in the `log` packet, come from user input over HTTP (`HTTPHandler`
# only strips ASCII control characters from `query_id`, not arbitrary invalid UTF-8), so they must be
# sanitized to keep the packet - and the whole stream - valid UTF-8 / JSON.
echo '--- log packets sanitize a non-UTF-8 query_id to valid UTF-8 JSON'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace&query_id=bad_utf8_04513_%FF" \
    -d "SELECT sum(number) FROM numbers_mt(1000000) GROUP BY number % 10 FORMAT Null" | python3 -c "
import sys, json
try:
    data = sys.stdin.buffer.read().decode('utf-8')
except UnicodeDecodeError:
    print('MISMATCH: invalid UTF-8 in response')
else:
    found = False
    for line in data.splitlines():
        if not line:
            continue
        packet = json.loads(line)
        if packet.get('packet') == 'log' and packet['log']['query_id'].startswith('bad_utf8_04513_'):
            found = True
            break
    print('non-UTF-8 query_id sanitized in log packets: OK' if found else 'MISMATCH: no matching log packet')
"

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

# In contrast to the URL / session case above, a `send_logs_level` set only in the query's own SETTINGS
# clause takes effect only from query execution onward, because it is not known until the query has been
# parsed. A query that fails during analysis, before pipeline execution, therefore captures none of the
# parse/plan/analysis logs from a query-level `send_logs_level`: only the framed `exception` packet is
# delivered, and the query text logged during parsing does not appear as a `log` packet.
echo '--- a query-level send_logs_level does not capture analysis-phase logs when the query fails early'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT * FROM table_that_does_not_exist_04513 SETTINGS send_logs_level='trace'" > "$result_file"
[ "$(grep -c '"packet":"exception"' "$result_file")" -ge 1 ] && echo 'query-level send_logs_level early-failure exception packet: OK'
[ "$(grep '"packet":"log"' "$result_file" | grep -c 'table_that_does_not_exist_04513')" -eq 0 ] && echo 'query-level send_logs_level early-failure has no query-text log packet: OK'
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

# A framing format is also applied to queries that produce no result stream (a successful `INSERT`, a
# DDL query). This matches the native protocol, which streams progress, logs and profile events for such
# queries too. Without framing for these queries, `framing_output_format` would be a silent no-op: the
# response would not switch to the framing content type, no packets would be written, and the logs /
# profile-events queues would accumulate unread until query teardown.
echo '--- a successful INSERT is framed: content type, no data packets, log / profile-events packets'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE framing_no_result_04513 (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace" \
    -d "INSERT INTO framing_no_result_04513 SELECT number FROM numbers(1000000)" > "$result_file"
grep -qi '^content-type: *application/x-ndjson' "$header_file" && echo 'INSERT content type: OK'
[ "$(grep -c '"packet":"data"' "$result_file")" -eq 0 ] && echo 'INSERT no data packets: OK'
[ "$(grep -c '"packet":"profile_events"' "$result_file")" -ge 1 ] && echo 'INSERT profile_events packets: OK'
[ "$(grep -c '"packet":"log"' "$result_file")" -ge 1 ] && echo 'INSERT log packets: OK'
[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM framing_no_result_04513")" = "1000000" ] && echo 'INSERT rows written: OK'
${CLICKHOUSE_CLIENT} -q "DROP TABLE framing_no_result_04513"
rm "$result_file" "$header_file"

# A DDL query has no result stream either; it must still switch the response to the framing content type.
echo '--- a DDL query switches the response to the framing content type'
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=EventStream" \
    -d "CREATE TABLE IF NOT EXISTS framing_no_result_04513 (x UInt64) ENGINE = Memory" > "$result_file"
grep -qi '^content-type: *text/event-stream' "$header_file" && echo 'DDL content type: OK'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513"
rm "$result_file" "$header_file"

# The inverse override applies to no-result queries too: a query that disables framing in its own
# SETTINGS clause must produce a plain response and must not leave queues that nobody drains.
echo '--- the query SETTINGS clause can disable framing on a no-result INSERT'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE framing_no_result_04513 (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "INSERT INTO framing_no_result_04513 SETTINGS framing_output_format='None' SELECT number FROM numbers(10)" > "$result_file"
[ "$(grep -c '"packet":' "$result_file")" -eq 0 ] && echo 'no-result query SETTINGS framing None disables framing: OK'
[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM framing_no_result_04513")" = "10" ] && echo 'no-result query SETTINGS framing None rows written: OK'
${CLICKHOUSE_CLIENT} -q "DROP TABLE framing_no_result_04513"
rm "$result_file"

# A no-result query that fails must be delivered as a framed exception packet (no data packet), the same
# as a failing query with a result stream.
echo '--- a failing INSERT is delivered as a framed exception'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace" \
    -d "INSERT INTO table_that_does_not_exist_04513 VALUES (1)" > "$result_file"
[ "$(grep -c '"packet":"exception"' "$result_file")" -ge 1 ] && echo 'failing INSERT exception packet: OK'
[ "$(grep -c '"packet":"data"' "$result_file")" -eq 0 ] && echo 'failing INSERT no data packets: OK'
rm "$result_file"

# The final progress flush after the query finishes must reach the framed stream too. The `Null` payload
# carrier of the no-result path is not part of the pipeline, so it is finalized explicitly, flushing the
# pending (throttled) progress update: the last `progress` packet carries the final counters
# (`result_rows` / `result_bytes`), like the final progress packet of the native protocol.
echo '--- a framed INSERT ends with a final progress packet carrying the final counters'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE framing_no_result_04513 (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "INSERT INTO framing_no_result_04513 SELECT number FROM numbers(1000000)" > "$result_file"
[ "$(grep -c '"packet":"progress"' "$result_file")" -ge 1 ] && echo 'INSERT progress packets: OK'
grep '"packet":"progress"' "$result_file" | tail -1 | grep -q '"result_rows":"1000000"' && echo 'INSERT final progress result_rows: OK'
${CLICKHOUSE_CLIENT} -q "DROP TABLE framing_no_result_04513"
rm "$result_file"

# The output format is irrelevant for a no-result query (no payload is formatted), so the framing must
# not depend on it: a mistyped `default_format` must not fail the query, and a binary `default_format`
# (`Native`) must not flip the `EventStream` content type to `payload=base64`.
echo '--- a framed no-result query does not depend on the output format'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE framing_no_result_04513 (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&default_format=NoSuchFormat04513" \
    -d "INSERT INTO framing_no_result_04513 SELECT number FROM numbers(10)" > "$result_file"
[ "$(grep -c '"packet":"exception"' "$result_file")" -eq 0 ] && echo 'INSERT with unknown default_format no exception: OK'
[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM framing_no_result_04513")" = "10" ] && echo 'INSERT with unknown default_format rows written: OK'
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=EventStream&default_format=Native" \
    -d "CREATE TABLE IF NOT EXISTS framing_no_result_04513_ddl (x UInt64) ENGINE = Memory" > /dev/null
grep -qi '^content-type: *text/event-stream' "$header_file" && echo 'DDL EventStream content type with binary default_format: OK'
grep -qi 'payload=base64' "$header_file" || echo 'DDL EventStream no payload=base64 with binary default_format: OK'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS framing_no_result_04513_ddl"
rm "$result_file" "$header_file"

# The framing format's finalization (`finishExecutedQuery`, `output_format->finalize`,
# `framing->finalize`) runs after the query itself has otherwise succeeded and packets may already
# have been streamed to the client, so it is not covered by the ordinary `catch` around query
# execution. A failure there must still be delivered as a framed `exception` packet - not escape to
# the generic HTTP error path, which would append a plain-text error after an already-started packet
# stream and break the "always a stream of packets" contract.
echo '--- a failure while finalizing the framing format is delivered as a framed exception packet'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_finalize_throw"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 AS x FORMAT JSONEachRow" | python3 -c "
import sys, json
lines = [line for line in sys.stdin.read().splitlines() if line]
try:
    packets = [json.loads(line) for line in lines]
except json.JSONDecodeError:
    print('MISMATCH: response is not valid NDJSON')
else:
    exceptions = [p['exception'] for p in packets if p.get('packet') == 'exception']
    if len(exceptions) == 1 and 'Injecting fault' in exceptions[0]:
        print('finalization failure delivered as a framed exception packet: OK')
    else:
        print('MISMATCH:', [p.get('packet') for p in packets])
"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_finalize_throw"

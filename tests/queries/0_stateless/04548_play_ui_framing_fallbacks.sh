#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI (`programs/server/play.html`) requests every query with the `EventStream` framing
# format and falls back to a plain request when the framing cannot be used. This test pins the
# server-side contracts those client fallbacks rely on:
#
# 1. An explicit output format the framing does not accept (e.g. `JSONEachRowWithProgress`) is
#    rejected with a non-OK status as a framed `exception` packet whose message carries the
#    substring the page matches to retry the query without framing - and the same query succeeds
#    on the plain request shape (the retry target).
# 2. A query-level `SETTINGS framing_output_format = ...` overrides the page's URL parameter, so
#    the page sends such a query without its own framing and dispatches on the response content
#    type; the user-chosen NDJSON framing must report itself as `application/x-ndjson` and must
#    accept the page's framing-compatible default format.
# 3. An exception raised after data was already streamed arrives as an `exception` packet inside
#    a 200 OK event stream (the headers were already sent), which the page must report as a query
#    failure ("Run all" stops, the single run shows the error state).
# 4. The page detects a query-level `framing_output_format` setting with the SQL lexer, not a text
#    match, so the setting name appearing inside a string literal (or a comment) is not mistaken for
#    a real setting. That client-side detection is pinned by a unit test
#    (`src/Parsers/tests/gtest_play_detect_framing_setting.cpp`); here we only check that the server
#    accepts the framed request the page sends for such a query (its string value comes back as a
#    normal framed `data` packet), since there is no WebAssembly runtime here to run the page lexer.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
PLAY_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play"

result_file="${CLICKHOUSE_TMP}/play_ui_fallbacks_$$.out"
header_file="${CLICKHOUSE_TMP}/play_ui_fallbacks_headers_$$.txt"

page="$(${CLICKHOUSE_CURL} -sS "${PLAY_URL}")"
default_format="$(echo "$page" | sed -n "s/^const default_format = '\([A-Za-z0-9]*\)';$/\1/p")"
framed_default_format="$(echo "$page" | sed -n "s/^const framed_default_format = '\([A-Za-z0-9]*\)';$/\1/p")"
# The message substring the page matches to recognize a format-compatibility rejection (and retry
# the query without framing), extracted from the page source so the two cannot drift apart.
rejection_pattern="$(echo "$page" | sed -n "s/.*framed_error_stream\.includes('\([^']*\)').*/\1/p" | head -n1)"

[ -n "$default_format" ] && echo 'default format extracted: OK'
[ -n "$framed_default_format" ] && echo 'framed default format extracted: OK'
[ -n "$rejection_pattern" ] && echo 'rejection pattern extracted: OK'

echo '--- an incompatible explicit format is rejected as a framed exception the page can match'
# The same request shape the page sends for a framed query.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}&framing_output_format=EventStream&send_logs_level=trace" \
    -d "SELECT 1 FORMAT JSONEachRowWithProgress" > "$result_file"
grep -c '^HTTP/1.1 400' "$header_file"
grep -o -m1 'text/event-stream' "$header_file"
grep -o -m1 '^event: exception' "$result_file"
grep -o -m1 -F "$rejection_pattern" "$result_file"

echo '--- the same query succeeds on the plain request shape (the retry target)'
${CLICKHOUSE_CURL} -sS "${URL}&default_format=${default_format}" \
    -d "SELECT 1 FORMAT JSONEachRowWithProgress" | grep -o -m1 '"row"'

echo '--- a query-level framing setting reports its own content type and accepts the framed default format'
# The page detects the query-level setting and sends the query without its own framing, with the
# framing-compatible default format; the response is rendered as raw text (NDJSON packet lines).
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}" \
    -d "SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'" > "$result_file"
grep -o -m1 'application/x-ndjson' "$header_file"
[ "$(grep -c '"packet":"exception"' "$result_file")" -eq 0 ] && echo 'no exception: OK'
grep -o -m1 '"packet":"data"' "$result_file"

echo '--- an exception after streamed data arrives inside a 200 OK event stream'
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}&framing_output_format=EventStream&max_block_size=1&max_threads=1" \
    -d "SELECT throwIf(number = 2) FROM numbers(10)" > "$result_file"
grep -c '^HTTP/1.1 200 OK' "$header_file"
grep -o -m1 '^event: data' "$result_file"
grep -o -m1 '^event: exception' "$result_file"

echo '--- framing_output_format only inside a string literal is not a query-level setting'
# The page detects a query-level framing setting with the SQL lexer, so `framing_output_format`
# inside a string literal is not a real setting: the page frames the query itself (the framed
# request shape below) and the server returns the string as a framed `data` packet, instead of
# refusing the query (as `= 'None'` would) or dropping the framing.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}&framing_output_format=EventStream&send_logs_level=trace" \
    -d "SELECT 'framing_output_format = None' AS x" > "$result_file"
grep -o -m1 'text/event-stream' "$header_file"
grep -o -m1 '^event: data' "$result_file"
grep -q -F 'framing_output_format = None' "$result_file" && echo 'string literal is not a framing setting: OK'
[ "$(grep -c '^event: exception' "$result_file")" -eq 0 ] && echo 'no exception: OK'

rm -f "$result_file" "$header_file"

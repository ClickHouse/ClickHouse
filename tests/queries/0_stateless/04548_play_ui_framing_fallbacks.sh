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
# 4. A query that explicitly disables framing (`SETTINGS framing_output_format = 'None'`) is not
#    "the query's own framing" - the page requires framing to render results, so it refuses such a
#    query client-side instead of sending it (there is no HTTP request to assert on, so the guard
#    expression itself is extracted from the served page and exercised directly).

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

echo '--- a query that disables framing is refused client-side, not sent as a plain request'
# The guard expression is extracted from the served page (as above for `rejection_pattern`), so it
# cannot drift from the code it pins, and exercised directly under node.
disables_framing_expr="$(echo "$page" | sed -n "s/^ *const user_disables_framing = \(.*\);$/\1/p")"
[ -n "$disables_framing_expr" ] && echo 'disables-framing guard extracted: OK'
node -e "
function disables_framing(query) { return ${disables_framing_expr}; }
const cases = [
    [\"SELECT 1\", false],
    [\"SELECT 1 SETTINGS framing_output_format = 'None'\", true],
    [\"select 1 settings framing_output_format='none'\", true],
    [\"SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'\", false],
];
let ok = true;
for (const [query, expected] of cases) {
    if (disables_framing(query) !== expected) { ok = false; console.log('MISMATCH', JSON.stringify(query)); }
}
console.log(ok ? 'disables-framing guard: OK' : 'disables-framing guard: FAIL');
"

rm -f "$result_file" "$header_file"

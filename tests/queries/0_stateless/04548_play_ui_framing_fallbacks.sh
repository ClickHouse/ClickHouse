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
# 5. A user-chosen NDJSON framing (`JSONEachPacketString`) that fails after data was already
#    streamed ends with a `{"packet":"exception",...}` line inside a 200 OK response. The page shows
#    the packet lines verbatim but scans them for that exception packet to report the query as a
#    failure ("Run all" then stops), so the exact packet prefix must appear in the response.
# 6. A query carrying its own `framing_output_format` in a `SETTINGS` clause overrides the download's
#    `default_format`: the download request shape (`&default_format=CSV` + the query) still returns
#    the framing packet stream, not CSV. So the download button refuses such a query rather than
#    saving a mislabeled file - a client-side guard whose presence is checked on the served page.
# 7. The `JSONEachPacket*` stream a failed user-framed query saves begins with a `{"packet": ...}`
#    object. History restore replays such a saved packet stream as raw text (keeping the partial
#    output and the exception packet) instead of showing one opaque error string.
# 8. The result snapshot records the framing kind explicitly (`event_stream` / `ndjson_packets` /
#    ''), and both restore paths dispatch on it (`snapshotFramingKind`) instead of guessing from the
#    payload's first bytes - so a raw result whose text happens to start with `event:` or `{"packet":`
#    (e.g. `SELECT 'event: data' FORMAT RawBLOB`) is not replayed as a framing stream. That client
#    persistence/restore is a browser-only flow, so its guards are checked on the served page here.
# 9. The download strips only a real trailing `FORMAT` clause (via the SQL-lexer walk in
#    `detectExplicitFormatClause`), not a raw regex, so a query where `FORMAT ...` is only text or
#    ordinary SQL downloads the query that actually ran. That client logic is pinned by a unit test
#    (`src/Parsers/tests/gtest_play_detect_explicit_format.cpp`); the served page wires it into the
#    download handler, which is checked here.
# 10. A successful user-chosen `JSONEachPacket*` query whose underlying output format has its own
#    restore path (a `default_format` table or a `JSONCompactColumns` chart) is streamed as NDJSON
#    packets, so the saved snapshot's bytes are a packet stream while its `format` is that special
#    format. Both restore paths must replay any `ndjson_packets` snapshot as raw text (`updateRaw`),
#    keyed off the persisted framing kind, not the format - otherwise the saved packet stream would
#    be reparsed as that format's JSON on reload/tab switch. The server contract (the wire is NDJSON,
#    not the special format) and the served-page dispatch are checked here.
# 11. A user-chosen `JSONEachPacket*` framing can also fail *before* the response reaches 200 OK
#    (e.g. an explicit `FORMAT JSONEachRowWithProgress` the framing rejects): the server then returns
#    a non-200 `application/x-ndjson` response whose body is still a packet stream ending with a
#    `{"packet":"exception",...}` line - not a plain `{"exception":...}` body. The page dispatches
#    such a response through the same raw packet-stream path as the 200 OK case (scanning for the
#    exception packet and recording `framing_kind = 'ndjson_packets'`), so the user sees the framing
#    they asked for and the saved snapshot replays correctly, instead of rendering one opaque error
#    string. The server contract (non-200, NDJSON, packet exception, no plain-exception body) and the
#    served-page dispatch are checked here.

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

# The download button refuses a query that chooses its own `framing_output_format`, because that
# setting overrides the download's `default_format` and the file would be the framing stream under
# the chosen extension. Checked here by the presence of the guard message on the served page (the
# download itself is a browser-only flow, not reachable from this shell test).
echo "$page" | grep -q -F 'overrides the download format' && echo 'download framing guard present: OK'
# The result snapshot records the framing kind explicitly, and both restore paths (single result and
# "Run all") dispatch on it via `snapshotFramingKind` rather than sniffing the payload's first bytes.
echo "$page" | grep -q -F 'framing_kind:' && echo 'framing kind persisted: OK'
[ "$(echo "$page" | grep -c 'snapshotFramingKind(')" -ge 2 ] && echo 'restore keys off framing kind: OK'
# The download strips only a real trailing `FORMAT` clause using the SQL-lexer walk, not a raw regex.
echo "$page" | grep -q -F 'const format_clause = await detectExplicitFormatClause(query)' && echo 'download strips real format clause: OK'
# The single-result restore (`restoreFromHistory`) dispatches an `ndjson_packets` snapshot at the
# top level of its chain (`else if (kind === 'ndjson_packets')`), before the `!ok` and format
# branches, so a successful packet stream whose format has a special restore path is replayed raw
# via `updateRaw` and not reparsed as that format's JSON. ("Run all" via `renderSnapshotIntoElement`
# hoists the same check above its `!snap.ok` branch.) A browser-only flow, checked on the page.
ndjson_dispatch="else if (kind === 'ndjson_packets')"
echo "$page" | grep -qF "$ndjson_dispatch" && echo 'restore replays ndjson packets raw: OK'
# A non-200 `application/x-ndjson` response (a user-chosen NDJSON framing that fails before 200 OK)
# is dispatched through the raw packet-stream path, not the generic plain-error branch. Checked by
# the presence of that branch on the served page (the response handling is a browser-only flow).
non_ok_ndjson_dispatch="else if (!response.ok && content_type.startsWith('application/x-ndjson'))"
echo "$page" | grep -qF "$non_ok_ndjson_dispatch" && echo 'non-200 ndjson dispatched as packets: OK'
# The text kept for the tab/history snapshot is capped just past 100 KB as it is collected, so a
# single large framed data/log burst is not retained in full only to be dropped later by
# `saveHistory`. `appendCappedSnapshot` appends at most enough of a crossing chunk to exceed the
# limit; every streaming collection site goes through it, and none appends a chunk to `reply`
# uncapped. Retention is a browser-only concern, checked here by the guard's presence on the page.
echo "$page" | grep -q -F 'function appendCappedSnapshot(' && echo 'snapshot cap helper present: OK'
[ "$(echo "$page" | grep -c -F 'reply = appendCappedSnapshot(reply,')" -ge 3 ] && echo 'all collection sites capped: OK'
[ "$(echo "$page" | grep -c -F 'reply += ')" -eq 0 ] && echo 'no uncapped reply growth: OK'

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

echo '--- a user-chosen JSONEachPacketString failing after streamed data ends with an exception packet at 200 OK'
# The page shows a user-chosen NDJSON framing (`JSONEachPacketString`) verbatim, but scans the
# packet lines for a `{"packet":"exception",...}` line: a query can fail after data was already
# streamed and the 200 OK header sent. Pin that the server does emit that packet as a line of a
# 200 OK `application/x-ndjson` response - the exact prefix `{"packet":"exception"` the page
# matches - so "Run all" stops after such a failure instead of running later statements.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}&max_block_size=1&max_threads=1" \
    -d "SELECT throwIf(number = 5) FROM numbers(10) SETTINGS framing_output_format = 'JSONEachPacketString'" > "$result_file"
grep -c '^HTTP/1.1 200 OK' "$header_file"
grep -o -m1 'application/x-ndjson' "$header_file"
grep -o -m1 '"packet":"data"' "$result_file"
grep -o -m1 -F '{"packet":"exception"' "$result_file"
# The saved snapshot of such a failed run begins with a `{"packet": ...}` object; history restore
# keys off that prefix to replay the packet stream as raw text instead of one opaque error string.
head -c 10 "$result_file" | grep -q -F '{"packet":' && echo 'stream begins with a packet: OK'

echo '--- a query-level framing setting overrides the download default_format'
# The download button resubmits the query with the user-selected `default_format` (e.g. CSV) and
# strips only `FORMAT ...`. A query-level `SETTINGS framing_output_format = ...` still overrides
# that format, so the response is the NDJSON framing packet stream, not CSV - which is why the
# download refuses such a query (guard checked on the served page above) rather than saving the
# packet stream under a `.csv` name.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=CSV" \
    -d "SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'" > "$result_file"
grep -o -m1 'application/x-ndjson' "$header_file"
grep -o -m1 '"packet":"data"' "$result_file"

echo '--- a successful user-framed query whose format has a special restore path streams NDJSON packets'
# A successful `JSONEachPacketString` query with `FORMAT JSONCompactColumns` (a format the page would
# otherwise restore as a chart via `JSON.parse`) streams NDJSON packets as `application/x-ndjson`, so
# the saved snapshot's bytes are a packet stream while its recorded `format` is `JSONCompactColumns`.
# The restore paths must therefore replay it as raw text keyed off the framing kind (checked on the
# served page above), not reparse it as the underlying format's JSON.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}" \
    -d "SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString' FORMAT JSONCompactColumns" > "$result_file"
grep -o -m1 'application/x-ndjson' "$header_file"
grep -o -m1 '"packet":"data"' "$result_file"
[ "$(grep -c '"packet":"exception"' "$result_file")" -eq 0 ] && echo 'no exception: OK'

echo '--- a user-chosen JSONEachPacketString rejected before 200 OK is a non-200 NDJSON packet stream'
# The page sends a query carrying its own `framing_output_format` on the plain request shape (with
# the framing-compatible default format). When that query fails before the 200 OK header - here an
# explicit `FORMAT JSONEachRowWithProgress`, which the framing rejects during setup - the server
# returns a non-200 `application/x-ndjson` response whose body is a packet stream ending with a
# `{"packet":"exception",...}` line, NOT a plain `{"exception":...}` body. Pin that contract so the
# page can dispatch it through the raw packet-stream path (recording `framing_kind = 'ndjson_packets'`)
# instead of rendering the whole stream as one opaque error string.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}" \
    -d "SELECT 1 FORMAT JSONEachRowWithProgress SETTINGS framing_output_format = 'JSONEachPacketString'" > "$result_file"
grep -c '^HTTP/1.1 400' "$header_file"
grep -o -m1 'application/x-ndjson' "$header_file"
grep -o -m1 -F '{"packet":"exception"' "$result_file"
# The body is a packet stream, not a plain `{"exception":...}` error body, so the page's generic
# plain-error branch (which scans for `{"exception":`-prefixed lines) cannot parse it - which is why
# the dedicated non-200 NDJSON branch is needed.
[ "$(grep -c '^{"exception":' "$result_file")" -eq 0 ] && echo 'not a plain exception body: OK'

rm -f "$result_file" "$header_file"

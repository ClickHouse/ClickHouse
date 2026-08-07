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
# 12. When a query with an explicit `JSON*EachRowWithProgress` format (which the framing rejects for
#    its in-band progress) is retried without framing, its plain NDJSON stream can still signal a
#    failure in-band - a trailing top-level `{"exception":...}` object while the HTTP status stays
#    200 (`http_write_exception_in_output_format`). That is neither a framing `{"packet":"exception"}`
#    packet nor a plain-text error body, so the page scans such a retried WithProgress stream for the
#    in-band `{"exception":...}` object - keyed off the output format (`formatMayWriteInBandException`),
#    not only the user's own `JSONEachPacket*` framing - to report the failure so "Run all" stops. The
#    server contract (200 OK, NDJSON, streamed rows then an in-band exception object, no framing
#    packet) and the served-page detector are checked here.
# 13. Every request that expects an unframed response pins `framing_output_format=None` instead of
#    merely omitting the setting: a framing carried by the connection URL or by the HTTP session
#    behind it would otherwise frame the response of the chart path, of the compatibility retry, and
#    of the download. A query-level `SETTINGS framing_output_format = ...` clause is applied after the
#    URL parameters, so a query that intentionally chooses a framing still overrides that pin. Both
#    the server contract (session setting vs URL pin vs query clause) and the pinned URLs on the
#    served page are checked here.
# 14. `FORMAT JSONCompactEachRow ... WITH TOTALS` emits totals and extremes as separate framing
#    packets although its plain output drops those rows (they would be indistinguishable from data
#    rows). When the page shows such a format as raw text, only the `data` packets reconstruct the
#    plain output, so the raw-text path ignores the other payload packets for this family of formats
#    (checked on the served page; the server contract is pinned by `04512_framing_formats`).

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
# The server parses the `FORMAT` name with an identifier parser, so a quoted spelling is a real
# clause. Both branches of `detectExplicitFormatClause` must accept it: the lexer walk (via
# `TT.QuotedIdentifier`) and the no-WebAssembly fallback regex - otherwise a browser without
# WebAssembly still treats `FORMAT `JSONCompactColumns`` as "no explicit format", adds the page's own
# framing, and the download does not strip the real clause.
echo "$page" | grep -q -F 'tokens[i + 1].type === TT.QuotedIdentifier' && echo 'lexer walk accepts a quoted format name: OK'
echo "$page" | grep -q -F 'query.match(/\bFORMAT\s+(?:`([^`]+)`|"([^"]+)"|(\w+))(?=\s*(?:;|\bSETTINGS\b|$))/i)' && echo 'fallback regex accepts a quoted format name: OK'
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
# The raw explicit-format branch probes the terminal in-band exception on a bounded tail of the raw
# stream, kept separately from the capped snapshot: the cap stops `reply` at ~100 KB, so a trailer
# arriving after that would never be seen if probed on `reply` - the failed query would finish in
# the success state and "Run all" would continue past it.
echo "$page" | grep -q -F 'function appendBoundedTail(' && echo 'bounded in-band tail helper present: OK'
echo "$page" | grep -q -F 'inband_tail = appendBoundedTail(inband_tail, new_content);' && echo 'raw stream keeps in-band tail: OK'
[ "$(echo "$page" | grep -c -F 'parseInbandExceptionFromTail(inband_tail, format)')" -eq 2 ] && echo 'in-band probe reads the tail: OK'
# A retried plain `JSON*EachRowWithProgress` stream is scanned for its own in-band `{"exception":...}`
# object, keyed off the output format (`formatMayWriteInBandException`), not only the user's framing.
echo "$page" | grep -q -F 'function formatMayWriteInBandException(' && echo 'in-band exception detector present: OK'
# Format names are case-insensitive in ClickHouse, while `X-ClickHouse-Format` echoes the query's own
# spelling (pinned on the wire at the end of this test), so every dispatch on the response format
# compares a lowercased copy (`formatKey` / `formatIs`): otherwise `FORMAT jsoncompactcolumns` would
# lose the chart renderer, and a late in-band exception of `FORMAT xml` / `FORMAT json` /
# `FORMAT jsoneachrowwithprogress` would be missed entirely - letting "Run all" continue past a
# failed statement. The format-name sets are therefore lowercased, and no dispatch compares the raw
# header value against a canonically spelled name any more.
echo "$page" | grep -q -F 'function formatKey(' && echo 'format key helper present: OK'
echo "$page" | grep -q -F 'function formatIs(format, name)' && echo 'format comparison helper present: OK'
echo "$page" | grep -q -F "'jsoneachrowwithprogress'," && echo 'in-band format list lowercased: OK'
echo "$page" | grep -q -F "'json', 'jsonstrings', 'jsoncompact', 'jsoncompactstrings'," && echo 'single-document format list lowercased: OK'
echo "$page" | grep -q -F "if (format_key === 'xml')" && echo 'xml probe case-insensitive: OK'
[ "$(echo "$page" | grep -c -F "format_key.includes('withprogress')")" -eq 2 ] && echo 'withprogress probe case-insensitive: OK'
[ "$(echo "$page" | grep -c -F "formatIs(format, 'JSONCompactColumns')")" -eq 3 ] && echo 'chart dispatch case-insensitive: OK'
[ "$(echo "$page" | grep -c -F 'formatIs(format, default_format)')" -eq 6 ] && echo 'table dispatch case-insensitive: OK'
[ "$(echo "$page" | grep -c -E "format ===? '(XML|PNG|GeoJSON|JSONCompactColumns)'|format ===? default_format|format ===? framed_default_format")" -eq 0 ] && echo 'no case-sensitive format dispatch left: OK'
# Every URL that expects an unframed response pins `framing_output_format=None`, so a framing left in
# the connection URL or in the HTTP session cannot leak into the plain path (the chart request, the
# compatibility retry) or into the download.
echo "$page" | grep -q -F "url += '&framing_output_format=None&default_format=' + (user_framing ? framed_default_format : default_format);" && echo 'plain request pins framing off: OK'
[ "$(echo "$page" | grep -c -F "'&framing_output_format=None' +")" -ge 1 ] && echo 'download pins framing off: OK'
[ "$(echo "$page" | grep -c -F "&framing_output_format=None&default_format=")" -ge 3 ] && echo 'auxiliary requests pin framing off: OK'
# The raw-text path reconstructs the plain output from the `data` packets alone for the formats whose
# plain output drops the totals and the extremes rows, instead of appending every payload packet.
echo "$page" | grep -q -F 'function plainOutputDropsTotalsAndExtremes(' && echo 'plain-totals predicate present: OK'
echo "$page" | grep -q -F "if (name !== 'data' && (options.onBytes || options.rawText) && plainOutputDropsTotalsAndExtremes(options.format))" && echo 'raw text keeps only data packets: OK'
# A truncated event stream (EOF with a residual frame that never got its terminating blank line)
# fails closed: the residual is parsed as a final event so a cut-off `exception` is still surfaced,
# and otherwise the run is marked failed so "Run all" does not proceed. Browser-only lifecycle,
# checked by the guard's presence on the served page.
echo "$page" | grep -q -F 'The response stream was truncated before it completed.' && echo 'truncated stream fails closed: OK'
# A residual frame cut off inside its JSON payload makes the event handler's `JSON.parse` throw;
# that parse failure maps to the same synthesized truncation error (with the snapshot's framing
# kind preserved) instead of escaping to the generic catch as a raw `SyntaxError`.
echo "$page" | grep -q -F 'residual_dispatch_failed = true;' && echo 'residual parse failure maps to truncation: OK'
# The raw NDJSON packet reader fails closed too: both NDJSON producers terminate every line with a
# newline, so an EOF that leaves the last line unterminated (after flushing the streaming decoder)
# marks the stream truncated, and a broken response is not recorded as a success "Run all" would
# continue past.
echo "$page" | grep -q -F 'truncated: ends_mid_line' && echo 'ndjson reader fails closed mid-line: OK'
echo "$page" | grep -q -F 'if (res.saw_exception || res.truncated) {' && echo 'ndjson truncation stops the run: OK'
# A truncated framed stream's synthesized carrier is appended to the snapshot itself, not only
# captured for the oversized-compaction path: a sub-cap failure snapshot is saved as `reply`
# verbatim (`compactFailureSnapshot` never runs for it) and the framed restore replays the stream
# text alone, so without the carrier a reload/tab switch would show only the partial output and
# lose the truncation error. Present in all three truncation sites - the event-stream residual and
# both NDJSON truncation branches (non-200 and 200 OK; the third `exception_carrier` append is the
# transport catch) - matching the transport-catch handling.
echo "$page" | grep -q -F "+ exception_block + '\\n\\n');" && echo 'event-stream truncation carrier saved in snapshot: OK'
[ "$(echo "$page" | grep -c -F ") + exception_carrier + '\\n');")" -ge 3 ] && echo 'ndjson truncation carrier saved in snapshot: OK'
# A "Run all" whose total per-statement snapshot exceeds the size budget keeps a COMPACT failure
# snapshot for each failed statement (dropping only oversized SUCCESSFUL payloads), so a large
# statement that failed still restores its error instead of the whole run reopening blank.
echo "$page" | grep -q -F 'if (failed && data != null && data.length > 100000)' && echo 'multi keeps compact failed snapshots: OK'
# The framed failure paths render the exception from the stream itself and used to return no
# `display_error` at all. When a "Run all" is still over the budget after dropping the successful
# payloads, every body (and the framing kind) is dropped, and the restore renders a bodyless
# statement only from `display_error` - so those paths must persist the exception text, extracted
# from the failure carrier by `framedFailureMessage` (shared with `compactFailureSnapshot` through
# `findFailureCarrier`). A failure that carried no exception at all keeps the not-stored notice, so
# no failed statement can restore blank.
echo "$page" | grep -q -F 'function findFailureCarrier(' && echo 'failure carrier locator present: OK'
echo "$page" | grep -q -F 'function framedFailureMessage(' && echo 'framed failure message helper present: OK'
[ "$(echo "$page" | grep -c -F 'display_error = framedFailureMessage(reply, framing_kind, exception_carrier) ?? undefined;')" -eq 3 ] && echo 'framed failures persist display_error: OK'
echo "$page" | grep -q -F "s.display_error = 'The query failed, but its error output was not stored.';" && echo 'dropped multi failures keep an error text: OK'
# The pending log queue is bounded, not just the rendered DOM: `appendLog` drops the oldest queued
# lines beyond the retained budget so a burst faster than the per-frame flush cannot grow it without
# limit. The budget is SHARED with the lines already rendered - counting the queue alone would retain
# a full DOM plus a full queue - and its floor is one frame's chunk, so the newest lines still get
# through and displace the oldest rendered ones. The sparkline history is bounded too -
# `downsampleHistoryByHalf` halves every metric's history in lockstep once the point cap is reached.
echo "$page" | grep -q -F 'this._log_buffer.length - queue_budget' && echo 'log queue bounded: OK'
echo "$page" | grep -q -F 'const queue_budget = Math.max(MAX_LOG_DOM_LINES - rendered, MAX_LOG_LINES_PER_FRAME);' && echo 'log budget shared with the DOM: OK'
echo "$page" | grep -q -F 'function downsampleHistoryByHalf(' && echo 'metric history bounded: OK'
# The Logs/Metrics view and toggle availability are tab-owned, not global: the `set-view` handler
# records the view on the active tab and applies it only to that tab's results, `_markLogsAvailable`
# marks availability on the owning tab, and `syncActiveTabChrome` replays both via `setViewState`.
echo "$page" | grep -q -F 'if (tab) tab.view = e.detail.view;' && echo 'view is tab-owned: OK'
echo "$page" | grep -q -F 'setViewState(view, logsAvailable, metricsAvailable)' && echo 'toggles replayed per tab: OK'
# The realtime resource meters are tab-owned too: CPU counters in `profile_events` packets are
# per-packet increments, so a backgrounded tab's batches keep accumulating on the tab
# (`accumulateResourceEvents`) instead of being dropped, and `syncActiveTabChrome` re-adopts the
# state so a reopened tab's meter continues from its live values instead of restarting near zero.
echo "$page" | grep -q -F 'accumulateResourceEvents(tab.resources, events);' && echo 'background meter batches accumulate: OK'
echo "$page" | grep -q -F 'progressEl.adoptResourceState(tab.resources);' && echo 'meter state re-adopted on tab open: OK'
# An NDJSON stream cut off in the middle of its terminal exception line is a truncation, not a real
# exception: the reader reports `saw_exception` only once the exception line reached its newline
# (`exception_done`), so the partial JSON line is never persisted or replayed as the failure carrier.
echo "$page" | grep -q -F 'saw_exception: saw_exception && exception_done,' && echo 'partial exception line is a truncation: OK'
# The meter state is one aggregate per RUN, not per stream: "Run all" executes a parallelizable
# group concurrently, so several framed statements of one tab can stream at once. The run's first
# stream creates the aggregate (`clearPanel` dropped the previous run's) and every stream
# accumulates into it, so a sibling statement cannot reset away increments already collected; the
# per-host peak keeps the maximum, so a sibling's smaller peak cannot erase a larger one.
echo "$page" | grep -q -F 'if (!tab.resources) tab.resources = freshResourceState();' && echo 'meter aggregate is per run: OK'
echo "$page" | grep -q -F 'host.peak = Math.max(host.peak, value);' && echo 'peak gauge keeps the maximum: OK'
# A transport failure (network drop / cancellation) striking a framed stream mid-flight keeps the
# snapshot's framing kind and synthesizes the failure carrier in the form that kind replays, so a
# restored tab replays the framed prefix with the error beneath it instead of falling back to the
# non-framing-aware failure replay; the replay itself skips an unparseable partial block rather
# than throwing out of the whole restore.
echo "$page" | grep -q -F "exception_carrier = 'event: exception\ndata: ' + JSON.stringify({ exception: display_error });" && echo 'transport catch keeps event-stream framing: OK'
echo "$page" | grep -q -F "? JSON.stringify({ packet: 'exception', exception: display_error })" && echo 'transport catch keeps ndjson framing: OK'
echo "$page" | grep -q -F 'try { dispatchEventStreamBlock(block, handleEvent); }' && echo 'replay skips broken frames: OK'
# A table whose query failed mid-stream is finalized the same way on every failure path - the live
# framed and non-framed branches and both restore paths (single result and "Run all") route through
# one shared `finalizeFailedTable`: partial rows get the same totals/coloring/transpose/layout under
# the error, and a header-only failure hides its empty table without wiping the already-rendered
# error and streamed logs (unlike `clear`). Browser-only rendering, checked by the shared method's
# presence and its call sites (live sites defer the measuring passes for a background tab).
echo "$page" | grep -q -F 'finalizeFailedTable(measureNow)' && echo 'failed-table finalization shared: OK'
[ "$(echo "$page" | grep -c -F 'finalizeFailedTable(tab.id === activeTabId);')" -eq 2 ] && echo 'live failures finalize the table: OK'
[ "$(echo "$page" | grep -c -F 'el.finalizeFailedTable(true);')" -eq 2 ] && echo 'restored failures finalize the table: OK'
# What a framed snapshot restores as is decided by the output format (and whether the payload turned
# out to be an image), never by the payload ENCODING: every snapshot the page saves now carries
# `base64: true`, so gating the table finish on it left a restored "Run all" framed table
# half-materialized - no totals, coloring, transpose, or single-value expansion. The "Run all" path
# derives it the way the single-result restore and the live path do, and runs the same finishing
# passes, so a reopened tab looks exactly like the live run.
echo "$page" | grep -q -F 'const is_table = !rendered_image && isDefaultFormat(format);' && echo 'restored framed table decided by the format: OK'
[ "$(echo "$page" | grep -c -F 'el.expandSingleValueIfNeeded();')" -eq 3 ] && echo 'every restore path expands a single value: OK'
# Replaying a saved `EventStream` snapshot dispatches the whole stream back-to-back, so the metrics
# model must be timestamped from the packets themselves (`current_time`) instead of the wall clock -
# otherwise a multi-second query restores with the rates and the sparkline history of the replay
# speed. Live streaming keeps the wall clock (`updateMetrics` called with no packet time).
echo "$page" | grep -q -F "const t = Date.parse(String(e.current_time ?? '').replace(' ', 'T') + 'Z');" && echo 'replay clock comes from the packets: OK'
echo "$page" | grep -q -F 'targetResultEl.updateMetrics(events, options.replay ? replayTimeSeconds(events) : undefined);' && echo 'replay time reaches the metrics model: OK'
# A restored framed result rebuilds the tab-owned resource state from the replayed `profile_events`
# through the same `accumulateResourceEvents` path the live reader uses, so the shared CPU/RAM/peak-RAM
# line reappears after a reload / Back / Forward (`clearPanel` dropped that state); `syncActiveTabChrome`
# repaints it. Every replay site passes its tab.
echo "$page" | grep -q -F 'resourceMeter: tab ? (events) => accumulateResourceEvents(tab.resources, events) : undefined,' && echo 'replay rebuilds the resource state: OK'
[ "$(echo "$page" | grep -c -E 'renderEventStreamText\(.*, format, tab, (false|!!)')" -eq 3 ] && echo 'every replay site passes its tab: OK'
# The live reader deliberately never renders an image collected from a truncated stream
# (`verbatim.finish(truncated)` skips it), so a saved truncated `FORMAT PNG` snapshot must not
# reopen as a partially decoded picture either: the truncation state is persisted with the
# snapshot (it cannot be re-derived - the synthesized failure carrier is a well-formed terminal
# block, indistinguishable from a complete failed stream) and replay passes it back into the sink.
echo "$page" | grep -q -F 'return verbatim ? verbatim.finish(!!truncated) : false;' && echo 'replay honors the saved truncation state: OK'
echo "$page" | grep -q -F 'is_truncated = res.truncated;' && echo 'live truncation state is captured: OK'
[ "$(echo "$page" | grep -c -F 'truncated: !!result.is_truncated,')" -eq 2 ] && echo 'both single-query saves persist it: OK'
echo "$page" | grep -q -F 'truncated: r ? !!r.is_truncated : false,' && echo 'the multi-statement save persists it: OK'
echo "$page" | grep -q -F 'truncated: stateData.truncated ?? false,' && echo 'the tab snapshot round-trips it: OK'
[ "$(echo "$page" | grep -c -E 'renderEventStreamText\(.*\.truncated\);')" -eq 2 ] && echo 'both restore sites replay it: OK'

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

echo '--- a plain WithProgress format retried without framing signals failure in-band at 200 OK'
# When a query with an explicit `FORMAT JSONEachRowWithProgress` (a format the framing rejects for
# its in-band progress) is retried without framing, the plain NDJSON stream can end with a trailing
# `{"exception":...}` object while the HTTP status stays 200 (`http_write_exception_in_output_format`).
# The page requests this on the plain shape with its own `default_format`, but the explicit `FORMAT`
# wins, so the wire is `JSONEachRowWithProgress`: rows stream, then the in-band exception. This is
# neither a framing `{"packet":"exception",...}` packet nor a plain-text error body, so the page must
# scan the retried WithProgress stream (keyed off the format) for the `{"exception":...}` object to
# report the failure - otherwise "Run all" would run later statements after a failed retried query.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${default_format}" \
    -d "SELECT throwIf(number = 5) FROM numbers(10) FORMAT JSONEachRowWithProgress SETTINGS http_write_exception_in_output_format = 1, max_block_size = 1, max_threads = 1" > "$result_file"
grep -c '^HTTP/1.1 200 OK' "$header_file"
grep -o -m1 'application/json' "$header_file"
grep -o -m1 -F '"row"' "$result_file"
grep -o -m1 -F '{"exception":' "$result_file"
# The failure is an in-band JSON object, not a framing packet: the page cannot rely on the
# `{"packet":"exception"` prefix for this retried WithProgress path.
[ "$(grep -c -F '{"packet":"exception"' "$result_file")" -eq 0 ] && echo 'not a framing packet: OK'

echo '--- a URL-pinned framing_output_format=None overrides a session framing, and a query clause overrides the pin'
# The page pins `framing_output_format=None` on every request that expects an unframed response,
# rather than only omitting the setting: the connection URL (or the HTTP session behind it) can
# already carry a framing, which would then frame the plain path too. Here the session sets a
# framing, so the unpinned request is framed (the control) while the pinned one is plain - and a
# query-level `SETTINGS` clause still wins over the pin, which is how a query that intentionally
# chooses a framing keeps working.
session="${CLICKHOUSE_DATABASE}_framing_none_$$"
# The `SET` response itself is irrelevant here (and is framed by the setting it establishes).
${CLICKHOUSE_CURL} -sS "${URL}&session_id=${session}" -d "SET framing_output_format = 'EventStream'" > /dev/null
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&session_id=${session}" -d "SELECT 1 FORMAT TSV" > "$result_file"
grep -o -m1 'text/event-stream' "$header_file"
grep -o -m1 '^event: data' "$result_file"
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&session_id=${session}&framing_output_format=None" -d "SELECT 1 FORMAT TSV" > "$result_file"
grep -o -m1 'text/tab-separated-values' "$header_file"
[ "$(grep -c '^event: ' "$result_file")" -eq 0 ] && echo 'the pinned request is not framed: OK'
cat "$result_file"
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&session_id=${session}&framing_output_format=None" \
    -d "SELECT 1 FORMAT TSV SETTINGS framing_output_format = 'JSONEachPacketString'" > "$result_file"
grep -o -m1 'application/x-ndjson' "$header_file"
grep -o -m1 '"packet":"data"' "$result_file"

echo '--- X-ClickHouse-Format echoes the query spelling, and a lowercased FORMAT still writes its in-band exception'
# The premise of the page's case-insensitive format dispatch: format names are case-insensitive
# (`FormatFactory` looks them up lowercased), but `X-ClickHouse-Format` reports the identifier exactly
# as the `FORMAT` clause spelled it. So a valid non-canonical spelling reaches the page as-is, and
# every dispatch on that header must lowercase it - otherwise the chart path, the table path and the
# in-band exception probes silently take the wrong branch for a perfectly valid query.
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=None" \
    -d "SELECT 1 FORMAT jsoncompactcolumns" > "$result_file"
grep -o -m1 'X-ClickHouse-Format: jsoncompactcolumns' "$header_file"
cat "$result_file"
# A lowercased `FORMAT xml` streams rows and then writes its failure as the same top-level
# `<exception>` trailer at 200 OK, so the page's probe must recognize it under this spelling too.
# The trailer is indented by a real tab, and POSIX `grep -E` has no `\t` escape (GNU `grep` matches
# a literal `t` instead), so the tab is spelled out here and interpolated into the patterns.
tab=$'\t'
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=None" \
    -d "SELECT throwIf(number = 5) FROM numbers(10) FORMAT xml SETTINGS http_write_exception_in_output_format = 1, max_block_size = 1, max_threads = 1" > "$result_file"
grep -c '^HTTP/1.1 200 OK' "$header_file"
grep -o -m1 'X-ClickHouse-Format: xml' "$header_file"
grep -o -m1 -E "^${tab}<exception>" "$result_file"
# The same for a lowercased single-document `FORMAT json`: the failure is a top-level `"exception"`
# member of the one document.
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=None" \
    -d "SELECT throwIf(number = 5) FROM numbers(10) FORMAT json SETTINGS http_write_exception_in_output_format = 1, max_block_size = 1, max_threads = 1" > "$result_file"
grep -o -m1 'X-ClickHouse-Format: json' "$header_file"
grep -o -m1 -E "^${tab}\"exception\":" "$result_file"
# And for a lowercased `*WithProgress` row stream: a terminal top-level `{"exception":...}` line.
${CLICKHOUSE_CURL} -sS -D "$header_file" "${URL}&framing_output_format=None" \
    -d "SELECT throwIf(number = 5) FROM numbers(10) FORMAT jsoneachrowwithprogress SETTINGS http_write_exception_in_output_format = 1, max_block_size = 1, max_threads = 1" > "$result_file"
grep -o -m1 'X-ClickHouse-Format: jsoneachrowwithprogress' "$header_file"
grep -o -m1 -F '{"exception":' "$result_file"

rm -f "$result_file" "$header_file"

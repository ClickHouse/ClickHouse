#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI (`programs/server/play.html`) requests every query with the `EventStream` framing
# format. The framing rejects `*WithProgress` formats (they already emit progress in-band), so the
# page carries a separate default format for framed requests (`framed_default_format`) and
# reassembles its compact rows client-side (`makeEventStreamHandler`): each payload packet is a
# single base64-encoded `data:` field (the `Content-Type` carries `payload=base64`) that decodes to
# the formatted block, the first two array lines of the decoded stream are the column names and the
# types, the packet name tells data, totals, and extremes rows apart, and an extremes packet
# carries the min row first, then the max row.
#
# This test pins that contract between the served page and the server: the page's framed default
# format must be accepted by the framing and produce the packet shapes the reassembly relies on,
# and the page's plain default format must (still) be rejected by the framing - if the server ever
# starts accepting it, the two-format split in the page should be revisited.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&output_format_parallel_formatting=0"
PLAY_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play"

result_file="${CLICKHOUSE_TMP}/play_ui_framing_$$.sse"
header_file="${CLICKHOUSE_TMP}/play_ui_framing_headers_$$.txt"

page="$(${CLICKHOUSE_CURL} -sS "${PLAY_URL}")"
default_format="$(echo "$page" | sed -n "s/^const default_format = '\([A-Za-z0-9]*\)';$/\1/p")"
framed_default_format="$(echo "$page" | sed -n "s/^const framed_default_format = '\([A-Za-z0-9]*\)';$/\1/p")"

[ -n "$default_format" ] && echo 'default format extracted: OK'
[ -n "$framed_default_format" ] && echo 'framed default format extracted: OK'

echo '--- the framed default format is accepted by the framing'
# The same request shape the page sends for a query without an explicit FORMAT clause.
${CLICKHOUSE_CURL} -sS -D "$header_file" \
    "${URL}&default_format=${framed_default_format}&framing_output_format=EventStream&send_logs_level=trace&extremes=1" \
    -d "SELECT number % 2 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS ORDER BY k" > "$result_file"
grep -o -m1 'text/event-stream' "$header_file"
[ "$(grep -c 'payload=base64' "$header_file")" -ge 1 ] && echo 'base64 payload: OK'
[ "$(grep -c '^event: exception' "$result_file")" -eq 0 ] && echo 'no exception: OK'

# The decoded payloads of each packet kind: every payload packet is a single base64-encoded
# `data:` field; the concatenation of the decoded payloads reconstructs the formatted output.
sse_payload_lines()
{
    awk -v kind="event: $1" 'BEGIN { RS = ""; FS = "\n" } $1 == kind { for (i = 2; i <= NF; i++) if ($i ~ /^data:/) print substr($i, 7) }' "$result_file" \
        | while read -r payload; do printf '%s' "$payload" | base64 -d; done
}

echo '--- names line, types line, then the data rows'
sse_payload_lines data
echo '--- the totals row arrives in a totals packet'
sse_payload_lines totals
echo '--- the extremes packet carries the min row first, then the max row'
sse_payload_lines extremes

echo '--- the plain default format is still rejected by the framing'
${CLICKHOUSE_CURL} -sS "${URL}&default_format=${default_format}&framing_output_format=EventStream" -d "SELECT 1" > "$result_file"
grep -o -m1 '^event: exception' "$result_file"
grep -o -m1 'BAD_ARGUMENTS' "$result_file"

rm -f "$result_file" "$header_file"

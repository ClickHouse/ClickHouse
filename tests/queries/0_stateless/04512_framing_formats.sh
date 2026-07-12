#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Framing formats multiplex data, totals, extremes, progress, logs, and profile events packets
# in a single HTTP response stream. The number of progress and profile events packets depends
# on timing, so these packets are filtered out where the exact output is checked.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"

# A `data` packet follows every output block boundary. For the aggregation queries below, pin the
# settings that determine these boundaries (the number of threads, two-level aggregation, the output
# block size, and external sorting), so the result is a single block and the number of `data` packets
# is deterministic regardless of the settings randomization in CI.
SINGLE_BLOCK="&max_threads=1&group_by_two_level_threshold=0&group_by_two_level_threshold_bytes=0"
SINGLE_BLOCK="${SINGLE_BLOCK}&max_block_size=65535&max_bytes_before_external_sort=0&max_bytes_ratio_before_external_sort=0"

echo '--- JSONEachPacketString, data packets follow block boundaries'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&max_block_size=2" \
    -d "SELECT number FROM numbers(3) FORMAT JSONEachRow" \
    | grep -v -e '"packet":"progress"' -e '"packet":"profile_events"'

echo '--- JSONEachPacketString, totals and extremes'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&extremes=1${SINGLE_BLOCK}" \
    -d "SELECT intDiv(number, 2) AS k, count() AS c FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k FORMAT TSV" \
    | grep -v -e '"packet":"progress"' -e '"packet":"profile_events"'

echo '--- the concatenation of the payloads is exactly the output of the format'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64&extremes=1${SINGLE_BLOCK}" \
    -d "SELECT intDiv(number, 2) AS k, count() AS c FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k FORMAT TSV" \
    | grep -v -e '"packet":"progress"' -e '"packet":"profile_events"' | sed -E 's/.*"data":"([^"]*)".*/\1/' \
    | while read -r encoded_payload; do echo "$encoded_payload" | base64 --decode; done

echo '--- JSONEachPacketBase64, payload decodes to the JSONEachRow output'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64" \
    -d "SELECT number FROM numbers(3) FORMAT JSONEachRow" \
    | grep '"packet":"data"' | sed -E 's/.*"data":"([^"]*)".*/\1/' \
    | while read -r encoded_payload; do echo "$encoded_payload" | base64 --decode; done

echo '--- EventStream'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&extremes=1${SINGLE_BLOCK}" \
    -d "SELECT intDiv(number, 2) AS k, count() AS c FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k FORMAT TSV" \
    | awk '/^event: /{name=$2; next} /^data: /{if (name != "progress" && name != "profile_events") print name" | "substr($0, 7)}'

echo '--- EventStream, payload without a trailing newline'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT Values" \
    | awk '/^event: /{name=$2; next} /^data: /{if (name != "progress" && name != "profile_events") print name" | "substr($0, 7)}'

# The client rebuilds `event.data` by joining the values of consecutive `data:` fields with '\n' and then
# stripping a single trailing '\n' (per the SSE specification). Reconstruct it here and compare byte-for-byte
# with the unframed output: the trailing newline that line-based formats emit must survive the round trip.
echo '--- EventStream reconstructs the payload byte-for-byte, including the trailing newline'
expected_output=$(mktemp "$CLICKHOUSE_TMP/04512_expected_XXXXXX")
reconstructed_output=$(mktemp "$CLICKHOUSE_TMP/04512_reconstructed_XXXXXX")
${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT number FROM numbers(3) FORMAT TSV" > "$expected_output"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT TSV" \
    | awk '
        /^event: /{event=$2; data=""; next}
        /^data: /{data=data substr($0, 7) "\n"; next}
        /^$/{if (event == "data") {sub(/\n$/, "", data); printf "%s", data} event=""; data=""}' > "$reconstructed_output"
if cmp -s "$expected_output" "$reconstructed_output"; then echo 'byte-exact round trip: OK'; else echo 'byte-exact round trip: MISMATCH'; fi
rm -f "$expected_output" "$reconstructed_output"

echo '--- framing works with HTTP compression'
${CLICKHOUSE_CURL} -sS --compressed "${URL}&framing_output_format=JSONEachPacketBase64&enable_http_compression=1" \
    -d "SELECT number FROM numbers(3) FORMAT JSONEachRow" \
    | grep '"packet":"data"' | sed -E 's/.*"data":"([^"]*)".*/\1/' \
    | while read -r encoded_payload; do echo "$encoded_payload" | base64 --decode; done

echo '--- EventStream content type'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" -d "SELECT 1"

echo '--- JSONEachPacket content type'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=JSONEachPacketString" -d "SELECT 1"

echo '--- progress packets are sent'
progress_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&interactive_delay=0" \
    -d "SELECT sum(number) FROM numbers(1000000) FORMAT JSONEachRow" | grep -c '"packet":"progress"')
[ "$progress_packets" -ge 1 ] && echo 'progress packets: OK'

echo '--- None framing format works as if no framing is used'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=None" -d "SELECT number FROM numbers(3) FORMAT JSONEachRow"

echo '--- unknown framing format'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=Unknown" -d "SELECT 1" | grep -o -m1 'Unknown framing format Unknown'

echo '--- exception is written as a packet (streaming)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&max_block_size=1" \
    -d "SELECT throwIf(number = 2) FROM numbers(10) FORMAT JSONEachRow" | grep -c '"packet":"exception"'

echo '--- exception is written as a packet (wait_end_of_query)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=1&framing_output_format=JSONEachPacketString&max_block_size=1" \
    -d "SELECT throwIf(number = 2) FROM numbers(10) FORMAT JSONEachRow" | grep -c '"packet":"exception"'

echo '--- exception is written as a packet (parse error)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT wrong syntax here" | grep -c '"packet":"exception"'

echo '--- framing is rejected for formats that defer totals and extremes to finalize (Template)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 AS k FORMAT Template SETTINGS format_template_row_format = '\${k:CSV}\n', format_template_resultset_format = '\${data}'" \
    | grep -o -m1 'is not compatible with framing formats'

echo '--- text framings are rejected for binary output formats (EventStream + Native)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream" \
    -d "SELECT number FROM numbers(3) FORMAT Native" \
    | grep -o -m1 'is not compatible with the output format Native'

echo '--- text framings are rejected for binary output formats (JSONEachPacketString + RowBinary)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT RowBinary" \
    | grep -o -m1 'is not compatible with the output format RowBinary'

# Raw passthrough formats advertise a textual content type but write the column bytes verbatim, so the
# output is not guaranteed to be valid UTF-8. They must be rejected for text framings just like binary formats.
echo '--- text framings are rejected for always-raw output formats (EventStream + RawBLOB)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream" \
    -d "SELECT toString(number) FROM numbers(3) FORMAT RawBLOB" \
    | grep -o -m1 'is not compatible with the output format RawBLOB'

echo '--- text framings are rejected for text-labeled raw output formats (JSONEachPacketString + TSVRaw)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT TSVRaw" \
    | grep -o -m1 'is not compatible with the output format TSVRaw'

echo '--- text framings are rejected for text-labeled raw output formats (EventStream + LineAsString)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream" \
    -d "SELECT toString(number) FROM numbers(3) FORMAT LineAsString" \
    | grep -o -m1 'is not compatible with the output format LineAsString'

echo '--- JSONEachPacketBase64 carries binary output formats (Native)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT Native" \
    | grep -c '"packet":"data"'

echo '--- JSONEachPacketBase64 carries raw output formats (RawBLOB)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64${SINGLE_BLOCK}" \
    -d "SELECT toString(number) FROM numbers(3) FORMAT RawBLOB" \
    | grep -c '"packet":"data"'

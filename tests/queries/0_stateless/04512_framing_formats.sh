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

# `EventStream` base64-encodes the payloads for output formats that may produce non-UTF-8 bytes
# (binary formats, and raw passthrough formats that write the column bytes verbatim), signalling it
# with a `payload=base64` content-type parameter. The base64-decoded payload is byte-for-byte the
# output the format would have produced without framing.
echo '--- EventStream base64-encodes binary output formats (Native)'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d "SELECT number FROM numbers(3) FORMAT Native"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT Native" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT number FROM numbers(3) FORMAT Native") \
    && echo 'Native payload round-trips' || echo 'MISMATCH'

echo '--- JSONEachPacketString is still rejected for binary output formats (RowBinary)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT RowBinary" \
    | grep -o -m1 'is not compatible with the output format RowBinary'

echo '--- EventStream base64-encodes always-raw output formats (RawBLOB)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT toString(number) FROM numbers(3) FORMAT RawBLOB" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT toString(number) FROM numbers(3) FORMAT RawBLOB") \
    && echo 'RawBLOB payload round-trips' || echo 'MISMATCH'

echo '--- JSONEachPacketString is still rejected for text-labeled raw output formats (TSVRaw)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT TSVRaw" \
    | grep -o -m1 'is not compatible with the output format TSVRaw'

echo '--- JSONEachPacketBase64 carries binary output formats (Native)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT Native" \
    | grep -c '"packet":"data"'

echo '--- JSONEachPacketBase64 carries raw output formats (RawBLOB)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64${SINGLE_BLOCK}" \
    -d "SELECT toString(number) FROM numbers(3) FORMAT RawBLOB" \
    | grep -c '"packet":"data"'

# Some formats produce raw bytes only under certain settings: `CustomSeparated` with the `Raw` escaping
# rule writes the column bytes verbatim (like `TSVRaw`), so it is treated the same as the other raw
# passthrough formats. This is detected with a settings-aware capability check.
echo '--- JSONEachPacketString is rejected for CustomSeparated with the Raw escaping rule'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&format_custom_escaping_rule=Raw" \
    -d "SELECT number FROM numbers(3) FORMAT CustomSeparated" \
    | grep -o -m1 'is not compatible with the output format CustomSeparated'

echo '--- EventStream base64-encodes CustomSeparated with the Raw escaping rule'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=Raw" \
    -d "SELECT toString(number) FROM numbers(3) FORMAT CustomSeparated"

echo '--- JSONEachPacketString accepts CustomSeparated with an escaping rule that escapes (Escaped)'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&format_custom_escaping_rule=Escaped" \
    -d "SELECT number FROM numbers(3) FORMAT CustomSeparated" | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'CustomSeparated (Escaped) accepted: OK'

# When the failure is the framing/output-format compatibility check itself, the error is still delivered
# as a framed `exception` packet (the exception is always JSON regardless of the output format), rather
# than a plain HTTP error body, so the client can always parse the response as a stream of packets.
echo '--- a compatibility error is delivered as a framed exception packet (streaming)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT RowBinary" | grep -c '"packet":"exception"'

echo '--- a compatibility error is delivered as a framed exception packet (wait_end_of_query)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=1&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT RowBinary" | grep -c '"packet":"exception"'

# A carriage return (`\r`) cannot survive the text `EventStream` framing (server-sent events treat it
# as a line terminator), so output formats that may emit one - `TSV` / `CSV` with a CRLF row terminator -
# are base64-encoded as well. The base64-decoded payload keeps the `\r\n` byte-for-byte.
echo '--- EventStream base64-encodes TSV with a CRLF row terminator'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&output_format_tsv_crlf_end_of_line=1" \
    -d "SELECT number FROM numbers(3) FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&output_format_tsv_crlf_end_of_line=1${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT TSV" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}&output_format_tsv_crlf_end_of_line=1" -d "SELECT number FROM numbers(3) FORMAT TSV") \
    && echo 'TSV CRLF payload round-trips' || echo 'MISMATCH'

echo '--- EventStream base64-encodes CSV with a CRLF row terminator'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&output_format_csv_crlf_end_of_line=1" \
    -d "SELECT number FROM numbers(3) FORMAT CSV"

# `JSONEachPacketString` puts the payload bytes into a JSON string, which escapes `\r`, so a CRLF row
# terminator is carried losslessly and the format is not rejected (unlike the text `EventStream`).
echo '--- JSONEachPacketString accepts TSV with a CRLF row terminator (the carriage return is escaped)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&output_format_tsv_crlf_end_of_line=1${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT TSV" \
    | grep -v -e '"packet":"progress"' -e '"packet":"profile_events"'

# The `*WithProgress` output formats write progress as in-band rows that are part of their own output.
# A framing format delivers progress as separate `progress` packets instead, so it rejects them, and the
# error is delivered as a framed `exception` packet.
echo '--- framing is rejected for output formats that write progress in-band (JSONEachRowWithProgress)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT JSONEachRowWithProgress" \
    | grep -o -m1 'writes progress in-band'

# When the query fails before any output is produced (for example an unknown table), the exception stream
# must carry only the `exception` packet: the real output format must not write its empty skeleton (for
# `FORMAT JSON`, `{"meta":[],"data":[],...}`) as a `data` packet.
echo '--- exception-only stream carries no data packet, only the exception (FORMAT JSON, streaming)'
response=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT * FROM no_such_table_04512 FORMAT JSON")
echo "data packets: $(echo "$response" | grep -c '"packet":"data"')"
echo "exception packets: $(echo "$response" | grep -c '"packet":"exception"')"

echo '--- exception-only stream carries no data packet, only the exception (FORMAT JSON, wait_end_of_query)'
response=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=1&framing_output_format=JSONEachPacketString" \
    -d "SELECT * FROM no_such_table_04512 FORMAT JSON")
echo "data packets: $(echo "$response" | grep -c '"packet":"data"')"
echo "exception packets: $(echo "$response" | grep -c '"packet":"exception"')"

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

echo '--- framing makes JSONCompactEachRow emit totals and extremes (dropped in the plain output)'
${CLICKHOUSE_CURL} -sS "${URL}&extremes=1${SINGLE_BLOCK}" \
    -d "SELECT intDiv(number, 2) AS k, count() AS c FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k FORMAT JSONCompactStringsEachRowWithNamesAndTypes"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&extremes=1${SINGLE_BLOCK}" \
    -d "SELECT intDiv(number, 2) AS k, count() AS c FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k FORMAT JSONCompactStringsEachRowWithNamesAndTypes" \
    | awk '/^event: /{name=$2; next} /^data: /{if (name != "progress" && name != "profile_events") print name" | "substr($0, 7)}'

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

# `setFraming` throws this rejection after the output format was already created, so the
# exception-recovery path must not reuse the leftover unframed format: the error is still
# delivered as a framed `exception` packet.
echo '--- the deferred-totals rejection is delivered as a framed exception packet'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 AS k FORMAT Template SETTINGS format_template_row_format = '\${k:CSV}\n', format_template_resultset_format = '\${data}'" \
    | grep -c '"packet":"exception"'

# The exception-only framing uses a `Null` carrier format, so the exception is framed even when the
# query's own format cannot be constructed on the exception path (here `Template` references a column
# of the header, which is empty when the query failed before producing a header).
echo '--- an exception raised before the output format exists is framed even for Template'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT k FROM table_does_not_exist_04512 FORMAT Template SETTINGS format_template_row_format = '\${k:CSV}\n', format_template_resultset_format = '\${data}'" \
    | grep -c '"packet":"exception"'

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
# as a line terminator), so output formats that may emit one - for example `TSV` with a CRLF row
# terminator - are base64-encoded as well. The base64-decoded payload keeps the `\r\n` byte-for-byte.
echo '--- EventStream base64-encodes TSV with a CRLF row terminator'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&output_format_tsv_crlf_end_of_line=1" \
    -d "SELECT number FROM numbers(3) FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&output_format_tsv_crlf_end_of_line=1${SINGLE_BLOCK}" \
    -d "SELECT number FROM numbers(3) FORMAT TSV" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}&output_format_tsv_crlf_end_of_line=1" -d "SELECT number FROM numbers(3) FORMAT TSV") \
    && echo 'TSV CRLF payload round-trips' || echo 'MISMATCH'

# The carriage return can also come from the data itself: the CSV quoting passes `\r` inside a `String`
# value through verbatim, so `CSV` is base64-encoded under `EventStream` regardless of the row
# terminator setting, and the decoded payload reproduces the unframed output byte-for-byte.
echo '--- EventStream base64-encodes CSV (the CSV quoting passes a carriage return in the data verbatim)'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d "SELECT 1 FORMAT CSV"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT 'a\rb' FORMAT CSV" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT 'a\rb' FORMAT CSV") \
    && echo 'CSV payload with a carriage return round-trips' || echo 'MISMATCH'

echo '--- EventStream base64-encodes formats that write values without escaping (Vertical)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT 'a\rb' AS x FORMAT Vertical" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT 'a\rb' AS x FORMAT Vertical") \
    && echo 'Vertical payload with a carriage return round-trips' || echo 'MISMATCH'

echo '--- EventStream base64-encodes SQLInsert (the table name setting is written verbatim)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&output_format_sql_insert_table_name=a%0Db${SINGLE_BLOCK}" \
    -d "SELECT 1 FORMAT SQLInsert" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}&output_format_sql_insert_table_name=a%0Db" -d "SELECT 1 FORMAT SQLInsert") \
    && echo 'SQLInsert payload with a carriage return in the table name round-trips' || echo 'MISMATCH'

echo '--- EventStream base64-encodes Pretty and XML (values are written without escaping the carriage return)'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d "SELECT 1 FORMAT PrettyCompact"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d "SELECT 1 FORMAT XML"

echo '--- EventStream base64-encodes CustomSeparated with the CSV escaping rule'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=CSV" \
    -d "SELECT 1 FORMAT CustomSeparated"

echo '--- EventStream base64-encodes CustomSeparated when a delimiter contains a carriage return'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=Escaped&format_custom_row_after_delimiter=%0D%0A" \
    -d "SELECT 1 FORMAT CustomSeparated"

echo '--- EventStream still embeds CustomSeparated with the Escaped rule as plain text'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=Escaped" \
    -d "SELECT 1 FORMAT CustomSeparated"

echo '--- EventStream base64-encodes Markdown only with escape_special_characters (that path passes a carriage return verbatim)'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&output_format_markdown_escape_special_characters=1" \
    -d "SELECT 1 FORMAT Markdown"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d "SELECT 1 FORMAT Markdown"

echo '--- JSONEachPacketString accepts CSV with a carriage return in the data (escaped in the JSON string)'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString${SINGLE_BLOCK}" \
    -d "SELECT 'a\rb' FORMAT CSV" \
    | grep -v -e '"packet":"progress"' -e '"packet":"profile_events"'

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

echo '--- the in-band-progress rejection is delivered as a framed exception packet'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT number FROM numbers(3) FORMAT JSONEachRowWithProgress" \
    | grep -c '"packet":"exception"'

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

# Some output formats write literal setting values verbatim, so a setting that is not valid UTF-8 makes
# the output non-textual even when the escaping rule itself escapes the data. This is knowable from the
# settings, so the text framings reject or base64-encode the output the same way as for the always-raw
# formats. `CustomSeparated` writes its delimiters verbatim (here `format_custom_row_after_delimiter`),
# and `SQLInsert` writes `output_format_sql_insert_table_name` verbatim.
echo '--- JSONEachPacketString is rejected for CustomSeparated with a non-UTF-8 delimiter'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&format_custom_escaping_rule=Escaped&format_custom_row_after_delimiter=%FF%0A" \
    -d "SELECT 1 FORMAT CustomSeparated" \
    | grep -o -m1 'is not compatible with the output format CustomSeparated'

echo '--- EventStream base64-encodes CustomSeparated with a non-UTF-8 delimiter'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=Escaped&format_custom_row_after_delimiter=%FF%0A" \
    -d "SELECT 1 FORMAT CustomSeparated"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=Escaped&format_custom_row_after_delimiter=%FF%0A${SINGLE_BLOCK}" \
    -d "SELECT 1 FORMAT CustomSeparated" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}&format_custom_escaping_rule=Escaped&format_custom_row_after_delimiter=%FF%0A" -d "SELECT 1 FORMAT CustomSeparated") \
    && echo 'CustomSeparated payload with a non-UTF-8 delimiter round-trips' || echo 'MISMATCH'

# A valid multi-byte UTF-8 delimiter (here U+2713 CHECK MARK) must not be misdetected as raw bytes.
echo '--- JSONEachPacketString accepts CustomSeparated with a valid UTF-8 (multi-byte) delimiter'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&format_custom_escaping_rule=Escaped&format_custom_row_after_delimiter=%E2%9C%93%0A" \
    -d "SELECT 1 FORMAT CustomSeparated" | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'CustomSeparated (valid UTF-8 delimiter) accepted: OK'

echo '--- JSONEachPacketString is rejected for SQLInsert with a non-UTF-8 table name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&output_format_sql_insert_table_name=a%FFb" \
    -d "SELECT 1 FORMAT SQLInsert" \
    | grep -o -m1 'is not compatible with the output format SQLInsert'

echo '--- EventStream base64-encodes SQLInsert with a non-UTF-8 table name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&output_format_sql_insert_table_name=a%FFb" \
    -d "SELECT 1 FORMAT SQLInsert"
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&output_format_sql_insert_table_name=a%FFb${SINGLE_BLOCK}" \
    -d "SELECT 1 FORMAT SQLInsert" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}&output_format_sql_insert_table_name=a%FFb" -d "SELECT 1 FORMAT SQLInsert") \
    && echo 'SQLInsert payload with a non-UTF-8 table name round-trips' || echo 'MISMATCH'

# The column names of the header are also written verbatim by `SQLInsert` (`printColumnNames`), and a
# quoted identifier can contain arbitrary bytes (`SELECT 1 AS `a\xFFb``), so a non-UTF-8 column name is
# knowable before any row is written and is treated the same as a non-UTF-8 table name.
echo '--- JSONEachPacketString is rejected for SQLInsert with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT SQLInsert' \
    | grep -o -m1 'is not compatible with the output format SQLInsert'

echo '--- EventStream base64-encodes SQLInsert with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT SQLInsert'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT SQLInsert' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT 1 AS `a\xFFb` FORMAT SQLInsert') \
    && echo 'SQLInsert payload with a non-UTF-8 column name round-trips' || echo 'MISMATCH'

# A valid multi-byte UTF-8 column name (here `col` followed by U+2713 CHECK MARK) must not be
# misdetected as raw bytes.
echo '--- JSONEachPacketString accepts SQLInsert with a valid UTF-8 (multi-byte) column name'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `col\xE2\x9C\x93` FORMAT SQLInsert' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'SQLInsert (valid UTF-8 column name) accepted: OK'

# `TSKV` always writes the column names into the header (`writeAnyEscapedString<'='>`, which escapes
# control characters but does not validate UTF-8), so a non-UTF-8 column name is knowable before any
# row is written and makes the output non-textual, exactly like the `*WithNames*` variants below.
echo '--- JSONEachPacketString is rejected for TSKV with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSKV' \
    | grep -o -m1 'is not compatible with the output format TSKV'

echo '--- EventStream base64-encodes TSKV with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSKV'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSKV' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT 1 AS `a\xFFb` FORMAT TSKV') \
    && echo 'TSKV payload with a non-UTF-8 column name round-trips' || echo 'MISMATCH'

# The `*WithNames`/`*WithNamesAndTypes` variants of the line-based text formats (`TSV`, `CSV`,
# `CustomSeparated`) write the column names (and data type names) into the header verbatim, so a
# non-UTF-8 column name makes them non-textual as well. The plain variants write no header and are
# unaffected: the column name is not part of the output.
echo '--- JSONEachPacketString is rejected for TSVWithNames with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSVWithNames' \
    | grep -o -m1 'is not compatible with the output format TSVWithNames'

echo '--- EventStream base64-encodes TSVWithNames with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSVWithNames'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSVWithNames' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT 1 AS `a\xFFb` FORMAT TSVWithNames') \
    && echo 'TSVWithNames payload with a non-UTF-8 column name round-trips' || echo 'MISMATCH'

echo '--- JSONEachPacketString accepts plain TSV with a non-UTF-8 column name (no header is written)'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT TSV' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'plain TSV (non-UTF-8 column name, no header) accepted: OK'

# A valid multi-byte UTF-8 column name (here `col` followed by U+2713 CHECK MARK) must not be
# misdetected as raw bytes.
echo '--- JSONEachPacketString accepts TSVWithNames with a valid UTF-8 (multi-byte) column name'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `col\xE2\x9C\x93` FORMAT TSVWithNames' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'TSVWithNames (valid UTF-8 column name) accepted: OK'

# `Markdown`, `Pretty*`, and `Vertical` also write the column names into the header verbatim (through
# escaping that escapes control characters but does not validate UTF-8, or through `serializeText`), so
# a non-UTF-8 column name is knowable before any row is written and makes the output non-textual, just
# like `TSKV` and the `*WithNames*` variants above. None of these formats write the data type names.
echo '--- JSONEachPacketString is rejected for Markdown with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Markdown' \
    | grep -o -m1 'is not compatible with the output format Markdown'

echo '--- EventStream base64-encodes Markdown with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Markdown'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Markdown' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT 1 AS `a\xFFb` FORMAT Markdown') \
    && echo 'Markdown payload with a non-UTF-8 column name round-trips' || echo 'MISMATCH'

echo '--- JSONEachPacketString is rejected for Pretty with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Pretty' \
    | grep -o -m1 'is not compatible with the output format Pretty'

echo '--- EventStream base64-encodes Pretty with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Pretty'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Pretty' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT 1 AS `a\xFFb` FORMAT Pretty') \
    && echo 'Pretty payload with a non-UTF-8 column name round-trips' || echo 'MISMATCH'

echo '--- JSONEachPacketString is rejected for Vertical with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Vertical' \
    | grep -o -m1 'is not compatible with the output format Vertical'

echo '--- EventStream base64-encodes Vertical with a non-UTF-8 column name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Vertical'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT 1 AS `a\xFFb` FORMAT Vertical' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT 1 AS `a\xFFb` FORMAT Vertical') \
    && echo 'Vertical payload with a non-UTF-8 column name round-trips' || echo 'MISMATCH'

# A valid multi-byte UTF-8 column name (here `col` followed by U+2713 CHECK MARK) must not be
# misdetected as raw bytes for these formats either.
echo '--- JSONEachPacketString accepts Markdown with a valid UTF-8 (multi-byte) column name'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT 1 AS `col\xE2\x9C\x93` FORMAT Markdown' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'Markdown (valid UTF-8 column name) accepted: OK'

# `CSVWithNames` (and the CSV-shaped `CustomSeparated*` variants) flatten a Tuple column into its leaf
# fields in the header (dotted names like `t.a`, `t.b`) when
# `output_format_csv_header_serialize_tuple_into_separate_columns` is enabled (the default). A named
# Tuple field with a non-UTF-8 element name therefore ends up in the header even though it never
# appears in the top-level column names, so the raw-bytes gate must validate the actual flattened
# header, not just the top-level block names.
echo '--- JSONEachPacketString is rejected for CSVWithNames with a non-UTF-8 Tuple element name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CSVWithNames' \
    | grep -o -m1 'is not compatible with the output format CSVWithNames'

echo '--- EventStream base64-encodes CSVWithNames with a non-UTF-8 Tuple element name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream" \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CSVWithNames'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CSVWithNames' \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CSVWithNames') \
    && echo 'CSVWithNames payload with a non-UTF-8 Tuple element name round-trips' || echo 'MISMATCH'

# The gate follows the actual header: with the header flattening disabled the header keeps the single
# top-level Tuple name `t`, which is valid UTF-8, so the same query is accepted.
echo '--- JSONEachPacketString accepts CSVWithNames with a non-UTF-8 Tuple element name when the header is not flattened'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&output_format_csv_header_serialize_tuple_into_separate_columns=0" \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CSVWithNames' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'CSVWithNames (unflattened header, non-UTF-8 Tuple element name) accepted: OK'

# A valid multi-byte UTF-8 Tuple element name must not be misdetected as raw bytes.
echo '--- JSONEachPacketString accepts CSVWithNames with a valid UTF-8 (multi-byte) Tuple element name'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d 'SELECT CAST((1, 2) AS Tuple(`col\xE2\x9C\x93` UInt8, c UInt8)) AS t FORMAT CSVWithNames' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'CSVWithNames (valid UTF-8 Tuple element name) accepted: OK'

# The matching `CustomSeparated` CSV path flattens the header the same way when the escaping rule is
# `CSV` and the field delimiter is the single character equal to `format_csv_delimiter` (`,`).
echo '--- JSONEachPacketString is rejected for CustomSeparatedWithNames (CSV rule) with a non-UTF-8 Tuple element name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&format_custom_escaping_rule=CSV&format_custom_field_delimiter=," \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CustomSeparatedWithNames' \
    | grep -o -m1 'is not compatible with the output format CustomSeparatedWithNames'

echo '--- EventStream base64-encodes CustomSeparatedWithNames (CSV rule) with a non-UTF-8 Tuple element name'
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{content_type}\n' "${URL}&framing_output_format=EventStream&format_custom_escaping_rule=CSV&format_custom_field_delimiter=," \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CustomSeparatedWithNames'

# With a non-matching field delimiter the header is not flattened (it keeps the top-level name `t`), so
# the same query is accepted - the gate follows the actual header under the current settings.
echo '--- JSONEachPacketString accepts CustomSeparatedWithNames (CSV rule) with a non-matching delimiter (header not flattened)'
data_packets=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&format_custom_escaping_rule=CSV&format_custom_field_delimiter=|" \
    -d 'SELECT CAST((1, 2) AS Tuple(`a\xFFb` UInt8, c UInt8)) AS t FORMAT CustomSeparatedWithNames' | grep -c '"packet":"data"')
[ "$data_packets" -ge 1 ] && echo 'CustomSeparatedWithNames (non-matching delimiter, header not flattened) accepted: OK'

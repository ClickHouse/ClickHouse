#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `http_write_exception_in_output_format` makes the JSON and XML output formats wrap their output in
# a `PeekableWriteBuffer`, so that a row whose serialization fails can be replaced with the
# exception. That buffer writes straight into the memory of the buffer it wraps and keeps its own
# copy of the write position there, while a framing format finalizes and restarts its payload buffer
# at every packet boundary. Unless the peekable buffer is re-attached to the restarted payload
# buffer, every packet after the first one repeats the bytes of the first packet padded with zero
# bytes, the rows are written past the end of the payload buffer, and the query eventually fails
# with `std::length_error` on a broken HTTP stream.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&http_write_exception_in_output_format=1&max_threads=1&max_block_size=100"

QUERY="SELECT number, toString(number) AS s FROM numbers(1000)"
TMP="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# The concatenation of the `data` payloads of an `EventStream` response: a block of formatted data is
# base64-encoded into a single `data:` field, so every packet is decoded on its own. The
# concatenation of the payloads is exactly what the output format would have written.
event_stream_data() {
    awk '/^event: /{name=$2; next} /^data: /{if (name == "data") print substr($0, 7)}' \
    | while read -r payload; do echo "$payload" | base64 --decode; done
}

# The same for the NDJSON `JSONEachPacketBase64` framing.
each_packet_data() {
    grep '"packet":"data"' | sed -E 's/.*"data":"([^"]*)".*/\1/' \
    | while read -r payload; do echo "$payload" | base64 --decode; done
}

# Runs the query with and without framing and checks that the framed payloads reproduce the plain
# output byte for byte, over more than one packet and with no zero-byte padding. The elapsed time of
# the `XML` statistics block is the only nondeterministic part of the output, so it is masked.
check() {
    local framing="$1" format="$2" extra="$3"
    local packets

    echo "--- ${framing}, ${format}${extra}"

    ${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=${framing}&default_format=${format}${extra}" -d "${QUERY}" > "${TMP}.raw"
    ${CLICKHOUSE_CURL} -sS "${URL}&default_format=${format}${extra}" -d "${QUERY}" \
        | sed -E 's#<elapsed>[0-9.]+</elapsed>#<elapsed>ELAPSED</elapsed>#' > "${TMP}.plain"

    if [ "${framing}" = "EventStream" ]; then
        packets=$(grep -c '^event: data$' "${TMP}.raw")
        event_stream_data < "${TMP}.raw" | sed -E 's#<elapsed>[0-9.]+</elapsed>#<elapsed>ELAPSED</elapsed>#' > "${TMP}.framed"
    else
        packets=$(grep -c '"packet":"data"' "${TMP}.raw")
        each_packet_data < "${TMP}.raw" | sed -E 's#<elapsed>[0-9.]+</elapsed>#<elapsed>ELAPSED</elapsed>#' > "${TMP}.framed"
    fi

    if [ "${packets}" -gt 1 ]; then echo "more than one data packet"; else echo "FAIL: ${packets} data packet(s)"; fi
    if cmp -s "${TMP}.framed" "${TMP}.plain"; then echo "payloads reproduce the plain output"; else echo "FAIL: payloads differ from the plain output"; fi
    if [ "$(wc -c < "${TMP}.framed")" = "$(LC_ALL=C tr -d '\0' < "${TMP}.framed" | wc -c)" ]; then echo "no zero bytes"; else echo "FAIL: zero bytes in the payloads"; fi

    rm -f "${TMP}.raw" "${TMP}.framed" "${TMP}.plain"
}

check EventStream JSONEachRow ''
check EventStream JSONEachRow '&output_format_json_validate_utf8=1'
check EventStream JSONCompactStringsEachRowWithNamesAndTypes ''
check EventStream XML ''
check JSONEachPacketBase64 JSONEachRow ''

# A query that fails after several blocks have been streamed: every row written before the failure
# must be complete and written once, and the stream must end with the terminal `exception` packet.
echo '--- exception after several data packets'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream&default_format=JSONEachRow" \
    -d "SELECT number, throwIf(number = 550) FROM numbers(1000)" > "${TMP}.raw"
event_stream_data < "${TMP}.raw" > "${TMP}.framed"

if [ "$(wc -c < "${TMP}.framed")" = "$(LC_ALL=C tr -d '\0' < "${TMP}.framed" | wc -c)" ]; then echo "no zero bytes"; else echo "FAIL: zero bytes in the payloads"; fi
if [ "$(grep -c '^{.*}$' "${TMP}.framed")" = "$(wc -l < "${TMP}.framed")" ]; then echo "every row is complete"; else echo "FAIL: incomplete row in the payloads"; fi
if [ "$(sort -u "${TMP}.framed" | wc -l)" = "$(wc -l < "${TMP}.framed")" ]; then echo "no row is repeated"; else echo "FAIL: repeated rows in the payloads"; fi
grep '^event: exception$' "${TMP}.raw"
rm -f "${TMP}.raw" "${TMP}.framed"

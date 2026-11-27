#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the failpoint below is server-global.
# A failure in a *later* parallel gzip compression pass — after the gzip header and the first pass
# have been buffered in `WriteBufferFromHTTPServerResponse`, but before that buffer has sent
# anything to the client — must still result in a clean HTTP exception response, not a truncated
# `200 OK` with a partial gzip body.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=8"
# Enough data for several compression passes, while the compressed output of the first pass stays
# well below the HTTP response buffer size, so nothing has been sent when the second pass fails.
QUERY="SELECT number FROM numbers(3000000) FORMAT TSV"

HDR="${CLICKHOUSE_TMP}/05044_headers.txt"
BODY="${CLICKHOUSE_TMP}/05044_body.txt"
rm -f "$HDR" "$BODY"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT parallel_gzip_compression_late_fail"
${CLICKHOUSE_CURL} -sS -D "$HDR" -H 'Accept-Encoding: gzip' "$URL" -d "$QUERY" -o "$BODY"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT parallel_gzip_compression_late_fail"

# The client must see a readable (uncompressed) exception message, with the matching exception-code
# header and without a Content-Encoding label.
grep -c "FAULT_INJECTED" "$BODY"
tr -d '\r' < "$HDR" | grep -c "^X-ClickHouse-Exception-Code: 710"
tr -d '\r' < "$HDR" | grep -ci "^Content-Encoding" || true

# Control: without the failpoint the same response is a valid gzip stream with all the rows.
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "$URL" -d "$QUERY" -o "$BODY"
gzip -dc "$BODY" | wc -l

rm -f "$HDR" "$BODY"

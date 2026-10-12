#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the failpoint below is server-global.
# A failure inside the parallel gzip deflater before any compressed output is produced must still
# result in a clean HTTP exception response: the failed compression pass must not commit the gzip
# header, and the raw exception body must not carry a `Content-Encoding: gzip` label.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=8"
# Enough data to fill the parallel deflater's staging buffer, so the first (and, with the failpoint,
# failing) compression pass runs during query execution rather than on finalize.
QUERY="SELECT number FROM numbers(1000000) FORMAT TSV"

HDR="${CLICKHOUSE_TMP}/04829_headers.txt"
BODY="${CLICKHOUSE_TMP}/04829_body.txt"
rm -f "$HDR" "$BODY"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT parallel_gzip_compression_fail"
${CLICKHOUSE_CURL} -sS -D "$HDR" -H 'Accept-Encoding: gzip' "$URL" -d "$QUERY" -o "$BODY"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT parallel_gzip_compression_fail"

# The client must see a readable (uncompressed) exception message, with the matching exception-code
# header and without a Content-Encoding label.
grep -c "FAULT_INJECTED" "$BODY"
tr -d '\r' < "$HDR" | grep -c "^X-ClickHouse-Exception-Code: 710"
tr -d '\r' < "$HDR" | grep -ci "^Content-Encoding" || true

rm -f "$HDR" "$BODY"

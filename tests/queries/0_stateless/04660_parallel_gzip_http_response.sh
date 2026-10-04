#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# `max_generic_compression_threads` must also apply to HTTP responses compressed with
# `Content-Encoding: gzip`, not only to file exports. The HTTP path wraps the response with
# compress_empty=false, so the parallel deflater has to honour that: emit a valid gzip stream when
# there is data, and nothing at all when the response body is empty.
PAR_URL="${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=8"
SEQ_URL="${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=1"

# 100k rows is ~0.6 MiB of TSV, spanning several 256 KiB blocks, so the parallel path really engages.
QUERY="SELECT number FROM numbers(100000) FORMAT TSV"

PAR="${CLICKHOUSE_TMP}/04660_http_parallel.tsv.gz"
SEQ="${CLICKHOUSE_TMP}/04660_http_serial.tsv.gz"
rm -f "$PAR" "$SEQ"

${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "$PAR_URL" -d "$QUERY" -o "$PAR"
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "$SEQ_URL" -d "$QUERY" -o "$SEQ"

gzip -t "$PAR" && echo "http response is valid gzip"
# The decompressed body must match what the same query produces uncompressed.
diff <(gzip -dc "$PAR") <(${CLICKHOUSE_CLIENT} --query "$QUERY") > /dev/null \
    && echo "http gzip response decompresses to the expected result"
# The parallel deflater emits independently-flushed blocks, so its framing differs from the serial
# writer's single stream. This is what proves the setting is honoured over HTTP rather than silently
# falling back to the serial buffer, while both still decode to the same bytes.
cmp -s "$PAR" "$SEQ" && echo "UNEXPECTED: parallel and serial http bodies are byte-identical" \
    || echo "parallel and serial http gzip framing differ"
diff <(gzip -dc "$PAR") <(gzip -dc "$SEQ") > /dev/null \
    && echo "parallel and serial http responses decompress identically"

# An empty response body must stay empty rather than becoming an empty gzip member, otherwise the
# HTTP path could not replace the body with an exception message.
EMPTY="${CLICKHOUSE_TMP}/04660_http_empty.gz"
rm -f "$EMPTY"
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "$PAR_URL" -d "SELECT 1 WHERE 0 FORMAT TSV" -o "$EMPTY"
echo "empty response body size: $(wc -c < "$EMPTY")"

rm -f "$PAR" "$SEQ" "$EMPTY"

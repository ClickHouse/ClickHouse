#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# HTTP has a second gzip output path, independent of `Content-Encoding`: the `compression` setting
# (or a `.gz` URL path suffix) compresses the response body itself, and the client is expected to
# decompress it. `max_generic_compression_threads` must apply there too, otherwise the setting is
# silently single-threaded for `compression = 'gzip'`.
PAR_URL="${CLICKHOUSE_URL}&compression=gzip&max_generic_compression_threads=8"
SEQ_URL="${CLICKHOUSE_URL}&compression=gzip&max_generic_compression_threads=1"

# 100k rows is ~0.6 MiB of TSV, spanning several 256 KiB blocks, so the parallel path really engages.
QUERY="SELECT number FROM numbers(100000) FORMAT TSV"

PAR="${CLICKHOUSE_TMP}/05137_generic_parallel.tsv.gz"
SEQ="${CLICKHOUSE_TMP}/05137_generic_serial.tsv.gz"
rm -f "$PAR" "$SEQ"

# No `Accept-Encoding: gzip` here on purpose: this must be the body codec, not `Content-Encoding`.
${CLICKHOUSE_CURL} -sS "$PAR_URL" -d "$QUERY" -o "$PAR"
${CLICKHOUSE_CURL} -sS "$SEQ_URL" -d "$QUERY" -o "$SEQ"

gzip -t "$PAR" && echo "generic compression response is valid gzip"
diff <(gzip -dc "$PAR") <(${CLICKHOUSE_CLIENT} --query "$QUERY") > /dev/null \
    && echo "generic compression response decompresses to the expected result"
# The parallel deflater emits independently-flushed blocks, so its framing differs from the serial
# writer's single stream. This is what proves the setting reaches this wrapper rather than silently
# falling back to the serial buffer, while both still decode to the same bytes.
cmp -s "$PAR" "$SEQ" && echo "UNEXPECTED: parallel and serial bodies are byte-identical" \
    || echo "parallel and serial generic compression framing differ"
diff <(gzip -dc "$PAR") <(gzip -dc "$SEQ") > /dev/null \
    && echo "parallel and serial generic compression responses decompress identically"

rm -f "$PAR" "$SEQ"

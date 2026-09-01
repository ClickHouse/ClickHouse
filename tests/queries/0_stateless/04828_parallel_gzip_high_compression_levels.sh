#!/usr/bin/env bash
# Tags: no-fasttest
# Enabling `max_generic_compression_threads` must not reject gzip compression levels that are valid
# for the serial writer. With libdeflate, gzip accepts levels up to 12, but the parallel deflater
# uses zlib, which supports levels only up to 9: levels above 9 must transparently stay on the
# serial libdeflate path instead of failing, for both `INTO OUTFILE ... LEVEL` and
# `http_zlib_compression_level`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# libdeflate extends the gzip max compression level from 9 to 12.
max_level=$(${CLICKHOUSE_LOCAL} -q "SELECT if(value = '1', 12, 9) FROM system.build_options WHERE name = 'USE_LIBDEFLATE'")

query="SELECT number, toString(number % 100) FROM numbers(100000)"
reference=$(${CLICKHOUSE_LOCAL} -q "$query FORMAT TSV" | md5sum)

function check_outfile_level()
{
    local level=$1
    local file="${CLICKHOUSE_TMP}/04828_level_test.tsv.gz"
    rm -f "$file"
    ${CLICKHOUSE_CLIENT} -q "$query INTO OUTFILE '$file' TRUNCATE COMPRESSION 'gzip' LEVEL $level FORMAT TSV SETTINGS max_generic_compression_threads = 8"
    gzip -t "$file"
    [ "$(gzip -dc "$file" | md5sum)" = "$reference" ]
    rm -f "$file"
}

# Level 9 engages the parallel deflater.
check_outfile_level 9 && echo "level 9 with parallel compression ok"
# The build's maximum level must also work with the setting enabled; on libdeflate builds it
# exceeds zlib's range and must fall back to the serial writer instead of failing.
check_outfile_level "$max_level" && echo "build max level with parallel compression ok"

# The HTTP response path validates `http_zlib_compression_level` against the same range.
URL="${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=8&http_zlib_compression_level=${max_level}"
BODY="${CLICKHOUSE_TMP}/04828_http_test.tsv.gz"
rm -f "$BODY"
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "$URL" -d "$query FORMAT TSV" -o "$BODY"
gzip -t "$BODY"
[ "$(gzip -dc "$BODY" | md5sum)" = "$reference" ] && echo "http response at build max level with parallel compression ok"
rm -f "$BODY"

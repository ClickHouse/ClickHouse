#!/usr/bin/env bash
# Tags: no-fasttest
# ClickHouse gzip output is produced by libdeflate as a SINGLE valid gzip member. Verify it is
# readable by the external system gunzip (not just by ClickHouse), for both the HTTP interface and
# file output, across compression levels.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

query="SELECT number, toString(number % 100) FROM numbers(100000)"
reference=$(${CLICKHOUSE_LOCAL} -q "$query FORMAT TSV" | md5sum)

echo "== HTTP Content-Encoding: gzip, decoded by external gunzip =="
for level in 1 6 9; do
    got=$(${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' \
            "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=${level}" \
            -d "$query FORMAT TSV" | gunzip | md5sum)
    [ "$got" = "$reference" ] && echo "level $level: OK" || echo "level $level: MISMATCH"
done

echo "== INTO OUTFILE *.gz, decoded by external gunzip =="
for level in 1 6 9; do
    out="${CLICKHOUSE_TMP}/04489_out_${level}.gz"
    rm -f "$out"
    ${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' FORMAT TSV SETTINGS output_format_compression_level=${level}" >/dev/null
    got=$(gunzip -c "$out" | md5sum)
    [ "$got" = "$reference" ] && echo "level $level: OK" || echo "level $level: MISMATCH"
    rm -f "$out"
done

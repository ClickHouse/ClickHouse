#!/usr/bin/env bash
# Tags: no-fasttest
# `INTO OUTFILE ... COMPRESSION 'gzip'/'deflate' LEVEL N` validates N against `getCompressionLevelRange`.
# With libdeflate, gzip/zlib accept compression levels up to 12; the zlib-ng fallback build
# (`ENABLE_LIBDEFLATE=0`) keeps the zlib max of 9. Either way the SQL `LEVEL` form must stay in line
# with the writer (and with the `output_format_compression_level` / `http_zlib_compression_level` paths),
# so detect the build's max level from `system.build_options` and check the boundary accordingly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

# libdeflate extends the gzip/zlib max compression level from 9 to 12.
max_level=$(${CLICKHOUSE_LOCAL} -q "SELECT if(value = '1', 12, 9) FROM system.build_options WHERE name = 'USE_LIBDEFLATE'")
over_level=$((max_level + 1))

query="SELECT number, toString(number % 100) FROM numbers(100000)"
reference=$(${CLICKHOUSE_LOCAL} -q "$query FORMAT TSV" | md5sum)

echo "== gzip: low levels and the build's max level round-trip via external gunzip =="
for level in 1 9 "$max_level"; do
    out="${CLICKHOUSE_TMP}/04491_out_${level}.gz"
    rm -f "$out"
    ${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'gzip' LEVEL ${level} FORMAT TSV" >/dev/null
    got=$(gunzip -c "$out" | md5sum)
    [ "$got" = "$reference" ] && echo "level <= max: OK" || echo "level <= max: MISMATCH"
    rm -f "$out"
done

echo "== gzip: above-max level rejected before writing =="
out="${CLICKHOUSE_TMP}/04491_out_over.gz"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'gzip' LEVEL ${over_level} FORMAT TSV" 2>&1 | grep -o -m1 "Invalid compression level"
[ -e "$out" ] && echo "file written: UNEXPECTED" || echo "file not written: OK"
rm -f "$out"

echo "== deflate (zlib): max level accepted, output round-trips via ClickHouse =="
out="${CLICKHOUSE_TMP}/04491_out.deflate"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'deflate' LEVEL ${max_level} FORMAT TSV" >/dev/null
got=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$out', 'TSV', 'a UInt64, b String', 'deflate') FORMAT TSV" | md5sum)
[ "$got" = "$reference" ] && echo "deflate max level: OK" || echo "deflate max level: MISMATCH"
rm -f "$out"

echo "== deflate (zlib): above-max level rejected =="
out="${CLICKHOUSE_TMP}/04491_out2.deflate"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'deflate' LEVEL ${over_level} FORMAT TSV" 2>&1 | grep -o -m1 "Invalid compression level"
rm -f "$out"

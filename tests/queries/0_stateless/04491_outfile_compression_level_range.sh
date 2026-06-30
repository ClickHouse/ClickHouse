#!/usr/bin/env bash
# Tags: no-fasttest
# `INTO OUTFILE ... COMPRESSION 'gzip'/'deflate' LEVEL N` validates N against `getCompressionLevelRange`.
# With libdeflate, gzip/zlib accept compression levels up to 12, so the SQL `LEVEL` form must stay in line
# with the writer (and with the `output_format_compression_level` / `http_zlib_compression_level` paths):
# accept levels 1..12 and reject 13.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

query="SELECT number, toString(number % 100) FROM numbers(100000)"
reference=$(${CLICKHOUSE_LOCAL} -q "$query FORMAT TSV" | md5sum)

echo "== gzip: LEVEL accepted through 12, output round-trips via external gunzip =="
for level in 1 9 10 12; do
    out="${CLICKHOUSE_TMP}/04491_out_${level}.gz"
    rm -f "$out"
    ${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'gzip' LEVEL ${level} FORMAT TSV" >/dev/null
    got=$(gunzip -c "$out" | md5sum)
    [ "$got" = "$reference" ] && echo "level $level: OK" || echo "level $level: MISMATCH"
    rm -f "$out"
done

echo "== gzip: LEVEL 13 rejected before writing =="
out="${CLICKHOUSE_TMP}/04491_out_13.gz"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'gzip' LEVEL 13 FORMAT TSV" 2>&1 | grep -o -m1 "Invalid compression level"
[ -e "$out" ] && echo "file written: UNEXPECTED" || echo "file not written: OK"
rm -f "$out"

echo "== deflate (zlib): LEVEL 12 accepted, output round-trips via ClickHouse =="
out="${CLICKHOUSE_TMP}/04491_out.deflate"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'deflate' LEVEL 12 FORMAT TSV" >/dev/null
got=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$out', 'TSV', 'a UInt64, b String', 'deflate') FORMAT TSV" | md5sum)
[ "$got" = "$reference" ] && echo "deflate level 12: OK" || echo "deflate level 12: MISMATCH"
rm -f "$out"

echo "== deflate (zlib): LEVEL 13 rejected =="
out="${CLICKHOUSE_TMP}/04491_out2.deflate"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'deflate' LEVEL 13 FORMAT TSV" 2>&1 | grep -o -m1 "Invalid compression level"
rm -f "$out"

#!/usr/bin/env bash
# Tags: no-fasttest
# `INTO OUTFILE ... COMPRESSION 'gzip'/'deflate' LEVEL N` validates N against `getCompressionLevelRange`.
# `gzip`/`deflate` accept `1-12`: 26.7 shipped `libdeflate`, whose maximum level is 12, and those levels
# stay accepted for compatibility even though the writer is back on `zlib`, which clamps them to 9.
# Anything above 12 must still be rejected before the file is written.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

query="SELECT number, toString(number % 100) FROM numbers(100000)"
reference=$(${CLICKHOUSE_LOCAL} -q "$query FORMAT TSV" | md5sum)

echo "== gzip: the whole accepted level range round-trips via external gunzip =="
for level in 1 9 10 12; do
    out="${CLICKHOUSE_TMP}/04843_out_${level}.gz"
    rm -f "$out"
    ${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'gzip' LEVEL ${level} FORMAT TSV" >/dev/null
    got=$(gunzip -c "$out" | md5sum)
    if [ "$got" = "$reference" ]; then echo "level ${level}: OK"; else echo "level ${level}: MISMATCH"; fi
    rm -f "$out"
done

echo "== gzip: above-max level rejected before writing =="
out="${CLICKHOUSE_TMP}/04843_out_over.gz"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'gzip' LEVEL 13 FORMAT TSV" 2>&1 | grep -o -m1 "Invalid compression level"
[ -e "$out" ] && echo "file written: UNEXPECTED" || echo "file not written: OK"
rm -f "$out"

echo "== deflate (zlib): max accepted level round-trips through ClickHouse =="
out="${CLICKHOUSE_TMP}/04843_out.deflate"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'deflate' LEVEL 12 FORMAT TSV" >/dev/null
# ORDER BY a: reading back through `file` does not preserve row order under `input_format_parallel_parsing`.
got=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$out', 'TSV', 'a UInt64, b String', 'deflate') ORDER BY a FORMAT TSV" | md5sum)
if [ "$got" = "$reference" ]; then echo "deflate level 12: OK"; else echo "deflate level 12: MISMATCH"; fi
rm -f "$out"

echo "== deflate (zlib): above-max level rejected =="
out="${CLICKHOUSE_TMP}/04843_out2.deflate"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "$query INTO OUTFILE '$out' COMPRESSION 'deflate' LEVEL 13 FORMAT TSV" 2>&1 | grep -o -m1 "Invalid compression level"
rm -f "$out"

echo "== output_format_compression_level above zlib's maximum is clamped, not rejected =="
out="${CLICKHOUSE_TMP}/04843_out_setting.gz"
rm -f "$out"
${CLICKHOUSE_LOCAL} -q "INSERT INTO FUNCTION file('$out', 'TSV', 'a UInt64, b String', 'gzip') $query SETTINGS output_format_compression_level = 12, engine_file_truncate_on_insert = 1"
got=$(gunzip -c "$out" | md5sum)
if [ "$got" = "$reference" ]; then echo "setting level 12: OK"; else echo "setting level 12: MISMATCH"; fi
rm -f "$out"

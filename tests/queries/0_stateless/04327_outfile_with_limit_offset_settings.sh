#!/usr/bin/env bash

# The `limit` / `offset` settings wrap the query as a derived table and apply `LIMIT` / `OFFSET` on
# the outer query. A `SELECT ... INTO OUTFILE` can be wrapped too: the output options (`INTO OUTFILE`,
# `FORMAT`, the outfile compression) are popped up to the outer query, so the cap applies to the
# final result that gets written to the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP}/04327_out.tsv"
OUT_GZ="${CLICKHOUSE_TMP}/04327_out.tsv.gz"
rm -f "$OUT" "$OUT_GZ"

echo "--- INTO OUTFILE with limit setting (analyzer) ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(10) ORDER BY number INTO OUTFILE '${OUT}' SETTINGS limit = 3, enable_analyzer = 1"
cat "$OUT"
rm -f "$OUT"

echo "--- INTO OUTFILE with limit setting (no analyzer) ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(10) ORDER BY number INTO OUTFILE '${OUT}' SETTINGS limit = 3, enable_analyzer = 0"
cat "$OUT"
rm -f "$OUT"

echo "--- INTO OUTFILE with limit + offset settings ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(10) ORDER BY number INTO OUTFILE '${OUT}' SETTINGS limit = 2, offset = 5"
cat "$OUT"
rm -f "$OUT"

echo "--- INTO OUTFILE FORMAT CSV with filter + limit settings ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(10) ORDER BY number INTO OUTFILE '${OUT}' FORMAT CSV SETTINGS filter = 'number % 2 = 0', limit = 2"
cat "$OUT"
rm -f "$OUT"

echo "--- compressed INTO OUTFILE keeps compression when wrapped ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(10) ORDER BY number INTO OUTFILE '${OUT_GZ}' SETTINGS limit = 4"
gzip -dc "$OUT_GZ"
rm -f "$OUT_GZ"

#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` preserves a top-level `ColumnConst` on the wire (`COL_IS_CONST`), so a chunk
# reaching `StreamingFormatExecutor` can legitimately hold one. Asynchronous inserts batch
# several such chunks and then run the preallocation pass, which feeds the chunk's columns to
# `prepareForSquashing`; for a complex destination such as `String` that casts the source to the
# concrete column type, so the const wrapper has to be stripped before preallocation and not
# merely before the `insertRangeFrom` that follows it.
CLICKHOUSE_CLIENT_CB="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05046"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05046 (s String) ENGINE = Memory"

# Distinct constants per batch part, so asynchronous insert deduplication cannot drop one.
for value in alpha beta gamma; do
    ${CLICKHOUSE_CLIENT_CB} --query \
        "SELECT '${value}' AS s FROM numbers(1000) INTO OUTFILE '${CLICKHOUSE_TMP}/05046_${value}.bin' TRUNCATE FORMAT ColumnBinary"
done

# A busy timeout long enough that all three parts land in one batch, giving the preallocation
# pass more than one chunk to work with (it is skipped outright for a single chunk).
URL="${CLICKHOUSE_URL}&allow_experimental_column_binary_format=1&async_insert=1&wait_for_async_insert=1"
URL="${URL}&async_insert_busy_timeout_min_ms=3000&async_insert_busy_timeout_max_ms=3000"
for value in alpha beta gamma; do
    ${CLICKHOUSE_CURL} -sS "${URL}&query=INSERT+INTO+t_05046+FORMAT+ColumnBinary" \
        --data-binary "@${CLICKHOUSE_TMP}/05046_${value}.bin" &
done
wait

${CLICKHOUSE_CLIENT} --query "SELECT s, count() FROM t_05046 GROUP BY s ORDER BY s"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05046"
rm -f "${CLICKHOUSE_TMP}"/05046_*.bin

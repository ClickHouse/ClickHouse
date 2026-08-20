#!/usr/bin/env bash
# Tags: no-fasttest
# ^^ the fast-test build does not ship all Parquet compression codecs.

# Exercise ParquetBlockInputFormat / ParquetBlockOutputFormat across the full
# type soup, compression codecs, multiple row groups, filter pushdown,
# predicate evaluation and schema inference.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP:-/tmp}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# -----------------------------------------------------------------------------
# 1. Type soup round-trip.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT
    number::Int32 AS i32,
    -number::Int64 AS i64_neg,
    number::UInt64 AS u64,
    number::Float64 AS f64,
    toString(number) AS s,
    toDate('2023-01-01') + number AS d,
    toDateTime('2023-01-01 00:00:00', 'UTC') + number AS dt,
    [number, number + 1]::Array(Int64) AS arr,
    (number, toString(number))::Tuple(Int64, String) AS tup,
    map('k', toString(number))::Map(String, String) AS m,
    toUUID('00000000-0000-0000-0000-0000000000' || lpad(toString(number), 2, '0')) AS u,
    CAST(number, 'Decimal(10, 2)') AS dec,
    (number % 2 = 0) AS b
FROM numbers(3)
FORMAT Parquet
" > "${OUT}"

echo '--- row count + DESCRIBE ---'
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${OUT}', 'Parquet')"
${CLICKHOUSE_LOCAL} --query "DESCRIBE TABLE file('${OUT}', 'Parquet')" | sort

echo '--- full readback ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'Parquet') ORDER BY i32 FORMAT Vertical"

# -----------------------------------------------------------------------------
# 2. Compression codec variations.
# -----------------------------------------------------------------------------
for codec in NONE SNAPPY ZSTD LZ4 LZ4_HC GZIP BROTLI; do
    echo "--- codec: ${codec} ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT number AS n FROM numbers(100) FORMAT Parquet" \
        --output_format_parquet_compression_method="$(echo ${codec} | tr A-Z a-z)" > "${OUT}" 2>&1 || { echo "skip ${codec}"; continue; }
    ${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(n) FROM file('${OUT}', 'Parquet')"
done

# -----------------------------------------------------------------------------
# 3. Multiple row groups (forces the parallel decoder / row_group pruning).
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT number AS n, toString(number) AS s FROM numbers(5000) FORMAT Parquet
" --output_format_parquet_row_group_size=500 > "${OUT}"

echo '--- multiple row groups: count ---'
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(n) FROM file('${OUT}', 'Parquet')"

echo '--- filter pushdown: row groups pruned ---'
# Profile events may be emitted on multiple lines (one per thread/batch under
# parallel decode), so sum the counters and emit a binary verdict. A regression
# that disables Parquet stats pushdown would drop pruned to 0.
${CLICKHOUSE_LOCAL} --print-profile-events --query "
    SELECT count() FROM file('${OUT}', 'Parquet') WHERE n BETWEEN 100 AND 200
" 2>&1 | awk '
    /ParquetReadRowGroups:/   { read   += $(NF-1) }
    /ParquetPrunedRowGroups:/ { pruned += $(NF-1) }
    END {
        if (pruned > 0 && read > 0) print "pruned"
        else                        print "not pruned (read=" read+0 " pruned=" pruned+0 ")"
    }
'

echo '--- filter pushdown: result correctness ---'
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${OUT}', 'Parquet') WHERE n BETWEEN 100 AND 200"
${CLICKHOUSE_LOCAL} --query "SELECT min(n), max(n) FROM file('${OUT}', 'Parquet') WHERE n < 50"

echo '--- filter pushdown: result identical with pushdown off ---'
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${OUT}', 'Parquet') WHERE n BETWEEN 100 AND 200 SETTINGS input_format_parquet_filter_push_down = 0"

# -----------------------------------------------------------------------------
# 4. Column pruning: SELECT a subset forces projection.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT number AS a, number * 2 AS b, number * 3 AS c FROM numbers(100) FORMAT Parquet
" > "${OUT}"
echo '--- column pruning ---'
${CLICKHOUSE_LOCAL} --query "SELECT sum(b) FROM file('${OUT}', 'Parquet')"
${CLICKHOUSE_LOCAL} --query "SELECT sum(a), sum(c) FROM file('${OUT}', 'Parquet')"

# Parquet column pruning: reading a single column issues fewer decoding tasks
# than reading all three. Sum across lines (CI emits one event line per thread
# under parallel decode) and assert single-column total < all-columns total.
# A regression that read all columns regardless of projection would tie them.
echo '--- column pruning: decoding tasks shrink with projection ---'
ONE=$(${CLICKHOUSE_LOCAL} --print-profile-events --query "SELECT sum(b) FROM file('${OUT}', 'Parquet')" 2>&1 \
    | awk '/ParquetDecodingTasks:/ { sum += $(NF-1) } END { print sum+0 }')
ALL=$(${CLICKHOUSE_LOCAL} --print-profile-events --query "SELECT * FROM file('${OUT}', 'Parquet') FORMAT Null" 2>&1 \
    | awk '/ParquetDecodingTasks:/ { sum += $(NF-1) } END { print sum+0 }')
if [ "$ONE" -lt "$ALL" ]; then echo "pruned"; else echo "not pruned (one=$ONE all=$ALL)"; fi

# -----------------------------------------------------------------------------
# 5. Nullable handling.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT
    if(number % 3 = 0, NULL, number)::Nullable(Int64) AS n,
    if(number % 2 = 0, NULL, toString(number))::Nullable(String) AS s
FROM numbers(10)
FORMAT Parquet
" > "${OUT}"
echo '--- nullable columns: null counts ---'
${CLICKHOUSE_LOCAL} --query "SELECT countIf(n IS NULL), countIf(s IS NULL) FROM file('${OUT}', 'Parquet')"

# -----------------------------------------------------------------------------
# 6. Schema inference.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "SELECT 1::Int32 AS a, 'x' AS s, [1, 2]::Array(Int32) AS arr FORMAT Parquet" > "${OUT}"
echo '--- schema inference ---'
${CLICKHOUSE_LOCAL} --query "DESCRIBE TABLE file('${OUT}', 'Parquet')" | sort

# -----------------------------------------------------------------------------
# 7. Empty parquet file.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "SELECT 1::Int64 AS n WHERE 0 FORMAT Parquet" > "${OUT}"
echo '--- empty parquet: count ---'
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${OUT}', 'Parquet', 'n Int64')"

rm -f "${OUT}"

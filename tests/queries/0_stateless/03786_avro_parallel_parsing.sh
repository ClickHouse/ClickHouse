#!/usr/bin/env bash
# Tags: no-fasttest

# Test for parallel Avro parsing.
# Generates Avro files and verifies that parallel and sequential reading produce identical results.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AVRO_FILE="${CLICKHOUSE_TMP}/parallel_avro_test_${CLICKHOUSE_DATABASE}.avro"

echo "=== Test 1: Basic parallel vs sequential - aggregate comparison ==="

# Generate a test Avro file with enough rows to create multiple blocks
${CLICKHOUSE_CLIENT} -q "
    SELECT
        number as id,
        toString(number) as str_val,
        number * 1.5 as float_val
    FROM numbers(50000)
    FORMAT Avro
" > "${AVRO_FILE}"

# Sequential read
SEQ_RESULT=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=0 -q "
    SELECT sum(id), count(), sum(length(str_val))
    FROM file('${AVRO_FILE}', Avro, 'id UInt64, str_val String, float_val Float64')
")

# Parallel read with 4 threads
PAR_RESULT=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=1 --max_threads=4 -q "
    SELECT sum(id), count(), sum(length(str_val))
    FROM file('${AVRO_FILE}', Avro, 'id UInt64, str_val String, float_val Float64')
")

if [ "$SEQ_RESULT" == "$PAR_RESULT" ]; then
    echo "OK: Sequential and parallel results match"
    echo "$SEQ_RESULT"
else
    echo "FAIL: Results differ"
    echo "Sequential: $SEQ_RESULT"
    echo "Parallel:   $PAR_RESULT"
    exit 1
fi

echo "=== Test 2: Row-by-row hash comparison ==="

# Generate test file with diverse data
${CLICKHOUSE_CLIENT} -q "
    SELECT
        number as id,
        concat('row_', toString(number)) as name,
        number % 100 as group_id,
        toFloat64(number) / 7.0 as ratio
    FROM numbers(10000)
    FORMAT Avro
" > "${AVRO_FILE}"

# Compare row-by-row hashes (order by id to ensure consistent ordering)
SEQ_HASH=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=0 -q "
    SELECT groupBitXor(cityHash64(id, name, group_id, ratio))
    FROM file('${AVRO_FILE}', Avro, 'id UInt64, name String, group_id UInt32, ratio Float64')
")

PAR_HASH=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=1 --max_threads=4 -q "
    SELECT groupBitXor(cityHash64(id, name, group_id, ratio))
    FROM file('${AVRO_FILE}', Avro, 'id UInt64, name String, group_id UInt32, ratio Float64')
")

if [ "$SEQ_HASH" == "$PAR_HASH" ]; then
    echo "OK: Row hashes match"
else
    echo "FAIL: Row hashes differ"
    echo "Sequential hash: $SEQ_HASH"
    echo "Parallel hash:   $PAR_HASH"
    exit 1
fi

echo "=== Test 3: Larger file with multiple data types ==="

# Generate a larger test file with various data types
${CLICKHOUSE_CLIENT} -q "
    SELECT
        number as id,
        concat('prefix_', toString(number), '_suffix') as str_val,
        number % 1000 as group_id,
        toInt32(number % 2147483647) as int_val,
        number % 2 = 0 as bool_val
    FROM numbers(100000)
    FORMAT Avro
" > "${AVRO_FILE}"

# Compare aggregates
SEQ_RESULT=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=0 -q "
    SELECT sum(id), count(), countDistinct(group_id), sum(int_val), countIf(bool_val)
    FROM file('${AVRO_FILE}', Avro, 'id UInt64, str_val String, group_id UInt32, int_val Int32, bool_val UInt8')
")

PAR_RESULT=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=1 --max_threads=4 -q "
    SELECT sum(id), count(), countDistinct(group_id), sum(int_val), countIf(bool_val)
    FROM file('${AVRO_FILE}', Avro, 'id UInt64, str_val String, group_id UInt32, int_val Int32, bool_val UInt8')
")

if [ "$SEQ_RESULT" == "$PAR_RESULT" ]; then
    echo "OK: Larger file results match"
    echo "$SEQ_RESULT"
else
    echo "FAIL: Results differ"
    echo "Sequential: $SEQ_RESULT"
    echo "Parallel:   $PAR_RESULT"
    exit 1
fi

echo "=== Test 4: Verify all rows are present (no duplicates, no missing) ==="

# Create file with known sequence
${CLICKHOUSE_CLIENT} -q "
    SELECT number as id FROM numbers(25000) FORMAT Avro
" > "${AVRO_FILE}"

# Sequential: check min, max, count, sum
SEQ_CHECK=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=0 -q "
    SELECT min(id), max(id), count(), sum(id)
    FROM file('${AVRO_FILE}', Avro, 'id UInt64')
")

# Parallel: same check
PAR_CHECK=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=1 --max_threads=8 -q "
    SELECT min(id), max(id), count(), sum(id)
    FROM file('${AVRO_FILE}', Avro, 'id UInt64')
")

EXPECTED="0	24999	25000	312487500"

if [ "$SEQ_CHECK" == "$EXPECTED" ] && [ "$PAR_CHECK" == "$EXPECTED" ]; then
    echo "OK: All rows present, no duplicates"
    echo "$SEQ_CHECK"
else
    echo "FAIL: Row integrity check failed"
    echo "Expected:   $EXPECTED"
    echo "Sequential: $SEQ_CHECK"
    echo "Parallel:   $PAR_CHECK"
    exit 1
fi

echo "=== Test 5: Different thread counts ==="

${CLICKHOUSE_CLIENT} -q "
    SELECT number as id, toString(number) as val FROM numbers(50000) FORMAT Avro
" > "${AVRO_FILE}"

SEQ_SUM=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=0 -q "
    SELECT sum(id) FROM file('${AVRO_FILE}', Avro, 'id UInt64, val String')
")

for threads in 1 2 4 8; do
    PAR_SUM=$(${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=1 --max_threads=$threads -q "
        SELECT sum(id) FROM file('${AVRO_FILE}', Avro, 'id UInt64, val String')
    ")
    if [ "$SEQ_SUM" != "$PAR_SUM" ]; then
        echo "FAIL: Results differ with $threads threads"
        echo "Sequential: $SEQ_SUM"
        echo "Parallel ($threads threads): $PAR_SUM"
        exit 1
    fi
done
echo "OK: All thread counts produce same results"

echo "=== Test 6: Default setting (parallel parsing disabled) ==="

# Verify the setting defaults to off
DEFAULT_VALUE=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.settings WHERE name = 'input_format_avro_parallel_parsing'")
if [ "$DEFAULT_VALUE" == "0" ] || [ "$DEFAULT_VALUE" == "false" ]; then
    echo "OK: Default value is disabled"
else
    echo "FAIL: Default value is '$DEFAULT_VALUE', expected '0' or 'false'"
    exit 1
fi

# Cleanup
rm -f "${AVRO_FILE}"

echo "=== All tests passed ==="

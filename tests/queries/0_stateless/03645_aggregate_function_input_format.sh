#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Cleanup function
cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_agg_single_${CLICKHOUSE_DATABASE}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_agg_multi_${CLICKHOUSE_DATABASE}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_agg_string_${CLICKHOUSE_DATABASE}"
}

trap cleanup EXIT

# Create test tables
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE test_agg_single_${CLICKHOUSE_DATABASE} (
    user_id UInt64,
    avg_session_length AggregateFunction(avg, UInt32)
) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE test_agg_multi_${CLICKHOUSE_DATABASE} (
    user_id UInt64,
    corr_values AggregateFunction(corr, Float64, Float64)
) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE test_agg_string_${CLICKHOUSE_DATABASE} (
    user_id UInt64,
    unique_strings AggregateFunction(uniq, String)
) ENGINE = Memory;
"

echo "=== Test 1: State format (default) ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'state';
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} 
SELECT 123, avgState(CAST(456 AS UInt32));
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 123 GROUP BY user_id;
"

echo "=== Test 2: Value format - single argument ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} VALUES (124, '456'), (125, '789'), (126, '321');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"

echo "=== Test 3: Array format - single argument ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} VALUES (127, '[100,200,300]'), (128, '[400,500]'), (129, '[600]');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"

echo "=== Test 4: Value format - multiple arguments (tuple) ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_multi_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_multi_${CLICKHOUSE_DATABASE} VALUES (200, '(1.0,2.0)'), (201, '(3.0,4.0)');
SELECT user_id, isNaN(corrMerge(corr_values)) as is_nan FROM test_agg_multi_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"

echo "=== Test 5: Array format - multiple arguments ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_multi_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_multi_${CLICKHOUSE_DATABASE} VALUES (202, '[(1.0,2.0),(3.0,4.0),(5.0,6.0)]');
SELECT user_id, corrMerge(corr_values) FROM test_agg_multi_${CLICKHOUSE_DATABASE} WHERE user_id = 202 GROUP BY user_id;
"

echo "=== Test 6: String aggregate function - value format ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_string_${CLICKHOUSE_DATABASE} VALUES (300, 'hello'), (301, 'world');
SELECT user_id, uniqMerge(unique_strings) FROM test_agg_string_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"

echo "=== Test 7: String aggregate function - array format ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_string_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_string_${CLICKHOUSE_DATABASE} VALUES (302, '[\"apple\",\"banana\",\"cherry\"]'), (303, '[\"dog\",\"cat\",\"dog\"]');
SELECT user_id, uniqMerge(unique_strings) FROM test_agg_string_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"

echo "=== Test 8: VALUES and TabSeparated format - value and TabSeparated ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} VALUES (400, '999');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 400 GROUP BY user_id;"
echo -e "400\t999" | ${CLICKHOUSE_CLIENT} -q "SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT TabSeparated"
${CLICKHOUSE_CLIENT} -q "SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 400 GROUP BY user_id;"

echo "=== Test 9: Error handling - invalid format ==="
out1="$(${CLICKHOUSE_CLIENT} -q "SET aggregate_function_input_format = 'invalid';" 2>&1)"
echo "$out1" | grep -q "Unexpected value of AggregateFunctionInputFormat" && echo "Error validation works correctly" || echo "No validation error found"

echo "=== Test 10: Multiple inserts with different formats ==="
${CLICKHOUSE_CLIENT} -q "
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};

-- State format
SET aggregate_function_input_format = 'state';
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} SELECT 500, avgState(CAST(100 AS UInt32));

-- Value format  
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} VALUES (501, '200');

-- Array format
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} VALUES (502, '[300,400,500]');

SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"

echo "=== Test 11: Empty array handling ==="
out2="$(${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} VALUES (600, '[]');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 600 GROUP BY user_id;
" 2>&1)"

echo "$out2"

echo "=== Test 12: CSV format - value ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT CSV
700,456;

SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 700 GROUP BY user_id;
"

echo "=== Test 13: CSV format - array ==="
echo '701,"[100,200,300]"' | ${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT CSV
"
${CLICKHOUSE_CLIENT} -q "SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 701 GROUP BY user_id;"

echo "=== Test 14: TabSeparated format - value ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT TabSeparated
702	789;"

${CLICKHOUSE_CLIENT} -q "SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 702 GROUP BY user_id;
"

echo "=== Test 15: TabSeparated format - array ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT TabSeparated
703	[400,500,600];"

${CLICKHOUSE_CLIENT} -q "SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 703 GROUP BY user_id;
"

echo "=== Test 16: JSONEachRow format - value ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT JSONEachRow
{\"user_id\": 704, \"avg_session_length\": \"999\"};"

${CLICKHOUSE_CLIENT} -q " SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 704 GROUP BY user_id;
"

echo "=== Test 17: JSONEachRow format - array ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT JSONEachRow
{\"user_id\": 705, \"avg_session_length\": \"[700,800,900]\"};"

${CLICKHOUSE_CLIENT} -q "SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} WHERE user_id = 705 GROUP BY user_id;
"

echo "=== Test 18: Multiple JSON records ==="
${CLICKHOUSE_CLIENT} -q "
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single_${CLICKHOUSE_DATABASE};
INSERT INTO test_agg_single_${CLICKHOUSE_DATABASE} FORMAT JSONEachRow
{\"user_id\": 706, \"avg_session_length\": \"111\"}
{\"user_id\": 707, \"avg_session_length\": \"222\"};"

${CLICKHOUSE_CLIENT} -q "SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single_${CLICKHOUSE_DATABASE} GROUP BY user_id ORDER BY user_id;
"
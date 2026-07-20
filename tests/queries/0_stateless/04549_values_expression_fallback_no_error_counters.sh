#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the test compares global error counters in system.errors before and after
# an INSERT; a concurrent test could increment the same counters and produce false diffs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Values format first parses each value with a fast streaming parser and falls back to the
# SQL expression parser when the value is not a plain literal. The fallback must not construct
# exceptions: every constructed exception is counted in system.errors (and system.error_log)
# even when it is caught and handled, producing confusing error records for successful queries.
#
# The inserts go through HTTP so that the data is parsed on the server side
# (clickhouse-client parses inline Values data on the client side).

PARSE_ERROR_NAMES="'CANNOT_PARSE_INPUT_ASSERTION_FAILED', 'CANNOT_PARSE_QUOTED_STRING', 'CANNOT_PARSE_NUMBER',
    'CANNOT_PARSE_DATE', 'CANNOT_PARSE_DATETIME', 'CANNOT_PARSE_BOOL', 'CANNOT_PARSE_UUID',
    'CANNOT_READ_ARRAY_FROM_TEXT', 'CANNOT_READ_MAP_FROM_TEXT', 'CANNOT_READ_ALL_DATA',
    'TOO_LARGE_STRING_SIZE', 'ARGUMENT_OUT_OF_BOUND', 'INCORRECT_DATA',
    'CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING', 'CANNOT_PARSE_IPV4', 'CANNOT_PARSE_IPV6',
    'UNKNOWN_ELEMENT_OF_ENUM', 'CANNOT_PARSE_ESCAPE_SEQUENCE', 'UNEXPECTED_DATA_AFTER_PARSED_VALUE'"

function get_parse_error_counters()
{
    $CLICKHOUSE_CLIENT -q "SELECT name, value FROM system.errors WHERE name IN (${PARSE_ERROR_NAMES}) ORDER BY name"
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_fallback (x UInt64, s String, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_decimal (d Decimal32(2)) ENGINE = MergeTree ORDER BY d"
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "CREATE TABLE t_values_dynamic (d Dynamic) ENGINE = Memory"
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "CREATE TABLE t_values_dynamic_literal_retry (id UInt8, d Dynamic) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_nested_decimal (a Array(Decimal32(2)), m Map(String, Decimal32(2))) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_variant_decimal (v Variant(Decimal32(2), String)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_variant_array_decimal (v Variant(Array(Decimal32(2)), String)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_array_variant_decimal (v Array(Variant(Decimal32(2), String))) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_mixed_variant_decimal (v Tuple(Decimal32(2), Variant(Decimal32(2), String))) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_decimal_string (v Tuple(Decimal32(2), String)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_decimal_map_key (m Map(Decimal64(2), UInt8)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_json (j JSON) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_aggregate (x AggregateFunction(sum, UInt64)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_values_aggregate_decimal (x AggregateFunction(sum, Decimal32(2))) ENGINE = Memory"
$CLICKHOUSE_CLIENT --allow_experimental_qbit_type=1 -q "CREATE TABLE t_values_qbit (q QBit(Float32, 1)) ENGINE = Memory"

counters_before=$(get_parse_error_counters)

# Expressions, NULL as default, DEFAULT keyword, a non-DEFAULT value starting with 'd',
# and a Map given as an escaped string: all of them miss the streaming fast path
# (or take its special branches) and must not increment any error counter.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_fallback VALUES (1 + 1, 'a', {'k1' : 1}), (3, upper('b'), {}), (NULL, 'c', '{\'k2\' : 2}'), (DEFAULT, 'd', {}), (divide(9, 3), 'div', {})"

# Decimal expression fallback, including an expression starting with 'd', must not construct exceptions either.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_decimal VALUES (1.23), (1.20 + 0.03), (divide(9, 3)), (1e-2147483649)"

# Dynamic type inference must report a failed literal probe without throwing before SQL expression fallback.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_dynamic_type=1" --data-binary \
    "INSERT INTO t_values_dynamic VALUES (toDate('2021-01-01')), (toIPv4('192.168.0.1')), ([+12])"

# Once a Dynamic column switches to expression parsing, retrying a later literal must preserve
# the previous literal semantics and not apply NULL-as-default handling from the initial probe.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_dynamic_type=1" --data-binary \
    "INSERT INTO t_values_dynamic_literal_retry VALUES (1, 42::UInt64), (2, NULL)"
${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&allow_experimental_dynamic_type=1&input_format_values_deduce_templates_of_expressions=0" \
    --data-binary "INSERT INTO t_values_dynamic_literal_retry VALUES (3, 42::UInt64), (4, NULL)"

# Composite Decimal function expressions and quoted complex values must skip the throwing literal probe.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_nested_decimal VALUES ([3.40 + 0.05], {}), (array(1.20 + 0.03), map('k', divide(9, 3))), ('[2.34]', '{\'q\':4.56}')"

# Variant probes its alternatives with non-throwing methods, so retrying its outer serialization
# cannot add Decimal overflow detection. Obvious function expressions must use the non-throwing probe.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_variant_decimal VALUES (toDecimal32(1.23, 2))"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_variant_array_decimal VALUES ([1.20 + 0.03])"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_array_variant_decimal VALUES ([(1.20 + 0.03)])"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_mixed_variant_decimal VALUES ((1.23, (1.20 + 0.03)))"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_decimal_string VALUES ((1.23, 123))"

# Serializations without a native non-throwing probe must not see obvious SQL function expressions.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_json VALUES (CAST('{\"a\":1}', 'JSON')), ((CAST('{\"b\":2}', 'JSON'))), (NULL)"

# Parenthesized AggregateFunction and QBit expressions used to reach exception-backed probes.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_aggregate VALUES ((initializeAggregation('sumState', 1::UInt64)))"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&aggregate_function_input_format=array" --data-binary \
    "INSERT INTO t_values_aggregate VALUES ('[1' || ',2]')"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_qbit_type=1" --data-binary \
    "INSERT INTO t_values_qbit VALUES ([1.0 + 1.0])"

counters_after=$(get_parse_error_counters)

echo '--- inserted data ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_values_fallback ORDER BY x, s"

echo '--- parse error counters ---'
if [ "$counters_before" == "$counters_after" ]; then
    echo 'no new parse errors'
else
    echo 'parse error counters changed:'
    diff <(echo "$counters_before") <(echo "$counters_after")
fi

echo '--- dynamic data ---'
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "SELECT d, dynamicType(d) FROM t_values_dynamic ORDER BY dynamicType(d)"

echo '--- dynamic literal retry data ---'
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 \
    -q "SELECT id, d, dynamicType(d) FROM t_values_dynamic_literal_retry ORDER BY id"

echo '--- nested decimal data ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_values_nested_decimal ORDER BY a"

echo '--- variant decimal data ---'
$CLICKHOUSE_CLIENT -q "SELECT v, variantType(v) FROM t_values_variant_decimal"
$CLICKHOUSE_CLIENT -q "SELECT v, variantType(v) FROM t_values_variant_array_decimal"
$CLICKHOUSE_CLIENT -q "SELECT v, arrayMap(variantType, v) FROM t_values_array_variant_decimal"
$CLICKHOUSE_CLIENT -q "SELECT v.1, v.2, variantType(v.2) FROM t_values_mixed_variant_decimal"
$CLICKHOUSE_CLIENT -q "SELECT v.1, v.2 FROM t_values_decimal_string"

echo '--- JSON data ---'
$CLICKHOUSE_CLIENT -q "SELECT j FROM t_values_json ORDER BY toString(j)"

echo '--- aggregate and QBit data ---'
$CLICKHOUSE_CLIENT -q "SELECT finalizeAggregation(x) FROM t_values_aggregate ORDER BY 1"
$CLICKHOUSE_CLIENT --allow_experimental_qbit_type=1 -q "SELECT CAST(q AS Array(Float32)) FROM t_values_qbit"

# Decimal columns keep the old behavior: an overflowing decimal literal must fail the query
# instead of being read as a Float64 expression that would silently lose precision.
echo '--- decimal overflow still fails ---'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "INSERT INTO t_values_decimal VALUES (12345678.91)" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- decimal overflow before operator still fails ---'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_decimal VALUES (1e8 - 99999999)" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- nested decimal overflow still fails ---'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "INSERT INTO t_values_nested_decimal VALUES ([12345678.91], {})" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- decimal map key overflow still fails ---'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_decimal_map_key VALUES ({10000000000000000.00:1})" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- decimal outside Variant overflow still fails ---'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary \
    "INSERT INTO t_values_mixed_variant_decimal VALUES ((12345678.91, 'x'))" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- aggregate decimal overflow still fails ---'
${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&aggregate_function_input_format=value&input_format_values_deduce_templates_of_expressions=0" \
    --data-binary "INSERT INTO t_values_aggregate_decimal VALUES ('12345678.91')" | grep -o "ARGUMENT_OUT_OF_BOUND" | head -1

echo '--- decimal data ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_values_decimal ORDER BY d"

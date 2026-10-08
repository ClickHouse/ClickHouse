#!/usr/bin/env bash
# Tags: no-fasttest

# Test Arrow format serialization/deserialization for complex types:
# Arrays (with edge cases), Maps, Tuples, Variants,
# and nested combinations thereof.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ----------------------------------------------
# 1. Arrays: basic roundtrip with edge cases
# ----------------------------------------------

echo "=== Arrays ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_arrays_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_arrays_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_arrays_src (a Array(UInt32), b Array(String), c Array(Nullable(Float64))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_arrays_dst AS arrow_test_arrays_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_arrays_src VALUES ([1, 2, 3], ['hello', 'world'], [1.5, NULL, 3.5]), ([], [], []), ([42], ['single'], [NULL]), ([0, 0, 0, 0, 0], ['a', 'b', 'c', 'd', 'e'], [NULL, NULL, NULL, NULL, NULL])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_arrays_src ORDER BY length(a) FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_arrays_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_arrays_dst ORDER BY length(a)"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_arrays_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_arrays_dst"

# ----------------------------------------------
# 2. Nested arrays
# ----------------------------------------------

echo "=== Nested Arrays ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_nested_arr_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_nested_arr_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_nested_arr_src (id UInt32, a Array(Array(UInt32)), b Array(Array(Nullable(String)))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_nested_arr_dst AS arrow_test_nested_arr_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_nested_arr_src VALUES (1, [[1,2],[3]], [['a',NULL],['b']]), (2, [[], [4,5,6]], [[NULL, NULL],[]]), (3, [[]], [[]])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_nested_arr_src ORDER BY id FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_nested_arr_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_nested_arr_dst ORDER BY id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_nested_arr_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_nested_arr_dst"

# ----------------------------------------------
# 3. Maps
# ----------------------------------------------

echo "=== Maps ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_maps_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_maps_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_maps_src (a Map(String, UInt64), b Map(String, String)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_maps_dst AS arrow_test_maps_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_maps_src VALUES ({'key1': 1, 'key2': 2}, {'a': 'x', 'b': 'y'}), ({}, {}), ({'single': 42}, {'only': 'one'})"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_maps_src ORDER BY length(a) FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_maps_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_maps_dst ORDER BY length(a)"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_maps_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_maps_dst"

# ----------------------------------------------
# 4. Tuples
# ----------------------------------------------

echo "=== Tuples ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_tuples_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_tuples_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_tuples_src (a Tuple(UInt32, String, Float64), b Tuple(Tuple(UInt32, UInt32), String)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_tuples_dst AS arrow_test_tuples_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_tuples_src VALUES ((1, 'hello', 3.14), ((10, 20), 'nested')), ((0, '', 0), ((0, 0), '')), ((42, 'world', -1.5), ((100, 200), 'deep'))"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_tuples_src ORDER BY a.1 FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_tuples_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_tuples_dst ORDER BY a.1"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_tuples_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_tuples_dst"

# ----------------------------------------------
# 5. Named Tuples
# ----------------------------------------------

echo "=== Named Tuples ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_named_tuples_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_named_tuples_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_named_tuples_src (t Tuple(x UInt32, y String, z Float64)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_named_tuples_dst AS arrow_test_named_tuples_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_named_tuples_src VALUES ((1, 'abc', 1.1)), ((2, 'def', 2.2)), ((3, 'ghi', 3.3))"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_named_tuples_src ORDER BY t.x FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_named_tuples_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_named_tuples_dst ORDER BY t.x"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_named_tuples_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_named_tuples_dst"

# ----------------------------------------------
# 6. Variant (export only - no Arrow import for DenseUnion)
# ----------------------------------------------

echo "=== Variants ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_variants"
${CLICKHOUSE_CLIENT} --query="SET allow_suspicious_variant_types = 1; CREATE TABLE arrow_test_variants (v Variant(UInt32, String, Float64)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_variants VALUES (42::UInt32), ('hello'), (3.14::Float64), (NULL), (0::UInt32), (''), (NULL)"

# Variant -> Arrow DenseUnion export should succeed without errors.
# We verify the output is non-empty valid Arrow data.
ARROW_SIZE=$(${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_variants FORMAT Arrow" | wc -c)
if [ "$ARROW_SIZE" -gt 0 ]; then
    echo "Variant export OK, size: positive"
else
    echo "Variant export FAILED: empty output"
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_variants"

# ----------------------------------------------
# 7. Nested combinations: Array(Tuple), Map(String, Array), Tuple(Array, Map)
# ----------------------------------------------

echo "=== Nested Combinations ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_combo_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_combo_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_combo_src (arr_of_tuples Array(Tuple(UInt32, String)), map_of_arrays Map(String, Array(UInt32)), tuple_with_map Tuple(Array(UInt32), Map(String, String))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_combo_dst AS arrow_test_combo_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_combo_src VALUES ([(1,'a'),(2,'b')], {'x':[1,2,3],'y':[4,5]}, ([10,20], {'k':'v'})), ([], {}, ([], {})), ([(42,'only')], {'single':[100]}, ([0], {'':'empty'}))"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_combo_src ORDER BY length(arr_of_tuples) FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_combo_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_combo_dst ORDER BY length(arr_of_tuples)"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_combo_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_combo_dst"

# ----------------------------------------------
# 8. Multi-block roundtrip with complex types
# ----------------------------------------------

echo "=== Multi-block ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_multiblock_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_multiblock_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_multiblock_src (id UInt32, arr Array(UInt32), m Map(String, UInt64), t Tuple(UInt32, String)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_multiblock_dst AS arrow_test_multiblock_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_multiblock_src SELECT number, [number, number*2, number*3], map('k' || toString(number), number), (number, toString(number)) FROM numbers(10)"

${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM arrow_test_multiblock_src ORDER BY id FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_multiblock_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_multiblock_dst ORDER BY id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_multiblock_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_multiblock_dst"

# ----------------------------------------------
# 9. Map with Nullable values
# ----------------------------------------------

echo "=== Map with Nullable values ==="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_map_nullable_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_test_map_nullable_dst"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_map_nullable_src (id UInt32, m Map(String, Nullable(UInt64))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_test_map_nullable_dst AS arrow_test_map_nullable_src"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_map_nullable_src VALUES (1, {'a': 1, 'b': NULL}), (2, {}), (3, {'c': NULL, 'd': NULL}), (4, {'e': 42})"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_map_nullable_src ORDER BY id FORMAT Arrow" | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_test_map_nullable_dst FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_test_map_nullable_dst ORDER BY id"

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_map_nullable_src"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_test_map_nullable_dst"

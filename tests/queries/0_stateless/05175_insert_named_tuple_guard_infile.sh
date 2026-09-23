#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The named-tuple insert guard (`allow_named_tuple_conversion_with_extra_source_fields_on_insert`)
# must be enforced for every client-parsed INSERT input path, not only the inline data of
# `INSERT ... VALUES`: `INSERT ... FROM INFILE` is parsed client-side too, through a `StorageFile`.
# It must also reject a partially overlapping named tuple in the interpreted `VALUES` path, where the
# conversion goes through `convertFieldToType` instead of `CAST`.

DATA_FILE="${CLICKHOUSE_TMP}"/data_05175.values

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05175;
    CREATE TABLE t_05175 (t Tuple(a Int32, b Int32)) ENGINE = Memory;
"

# A partially overlapping named tuple drops the source field `b2`, so it must be rejected on all
# paths: `INSERT ... SELECT` (server-side CAST), inline `VALUES` and `FROM INFILE` (both client-side).
echo '--- INSERT ... SELECT'
${CLICKHOUSE_CLIENT} --enable_named_columns_in_function_tuple 1 --query "
    INSERT INTO t_05175 SELECT tuple('a', 'b2')(1, 2);
" 2>&1 | grep -q -F "CANNOT_CONVERT_TYPE" && echo "rejected"

echo '--- INSERT ... VALUES'
${CLICKHOUSE_CLIENT} --enable_named_columns_in_function_tuple 1 --input_format_values_interpret_expressions 1 --query "
    INSERT INTO t_05175 VALUES (tuple('a', 'b2')(1, 2));
" 2>&1 | grep -q -F "CANNOT_CONVERT_TYPE" && echo "rejected"

echo '--- INSERT ... FROM INFILE'
echo "(tuple('a', 'b2')(1, 2))" > "${DATA_FILE}"
${CLICKHOUSE_CLIENT} --enable_named_columns_in_function_tuple 1 --input_format_values_interpret_expressions 1 --query "
    INSERT INTO t_05175 FROM INFILE '${DATA_FILE}' FORMAT Values;
" 2>&1 | grep -q -F "CANNOT_CONVERT_TYPE" && echo "rejected"

# Nothing was inserted.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05175"

# Matching names are accepted and matched by name on every path, and the guard can be lifted with
# `allow_named_tuple_conversion_with_extra_source_fields_on_insert`.
echo '--- accepted'
echo "(tuple('b', 'a')(1, 2))" > "${DATA_FILE}"
${CLICKHOUSE_CLIENT} --enable_named_columns_in_function_tuple 1 --input_format_values_interpret_expressions 1 --query "
    INSERT INTO t_05175 FROM INFILE '${DATA_FILE}' FORMAT Values;
"
echo "(tuple('a', 'b2')(3, 4))" > "${DATA_FILE}"
${CLICKHOUSE_CLIENT} --enable_named_columns_in_function_tuple 1 --input_format_values_interpret_expressions 1 --allow_named_tuple_conversion_with_extra_source_fields_on_insert 1 --query "
    INSERT INTO t_05175 FROM INFILE '${DATA_FILE}' FORMAT Values;
"
${CLICKHOUSE_CLIENT} --query "SELECT t.a, t.b FROM t_05175 ORDER BY t.a"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05175"
rm -f "${DATA_FILE}"

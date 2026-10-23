#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"

# Separate strides contain parent NULLs, element NULLs, a set member, and a non-member.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', ORC)
    SELECT if(number < 2000, NULL, tuple(if(number < 4000, NULL, if(number < 6000, 0, 2)::Int64)))
               ::Nullable(Tuple(x Nullable(Int64))) AS t,
           tupleElement(t, 'x') AS x
    FROM numbers(8000)
    SETTINGS enable_nullable_tuple_type = 1, engine_file_truncate_on_insert = 1,
             output_format_orc_row_index_stride = 2000"

predicates=(
    'value IN (0, 1)'
    'value NOT IN (0, 1)'
    'value IN (0, NULL)'
    'value NOT IN (0, NULL)'
    'value IN (NULL)'
    'value NOT IN (NULL)'
    'nullIn(value, (0, 1))'
    'notNullIn(value, (0, 1))'
    'NOT (value IN (0, 1))'
    'NOT (value NOT IN (0, 1))'
    '(value NOT IN (0, 1)) OR isNull(value)'
    '(value NOT IN (0, 1)) AND isNotNull(value)'
    'value IN (0.5, 1.5)'
    'value NOT IN (0.5, 1.5)'
)

# One client per column runs all its predicates; each row carries the plain and the pushed-down count.
for null_in in 0 1; do
    for column in x "tupleElement(t, 'x')"; do
        queries="SET enable_nullable_tuple_type = 1; SET enable_analyzer = 1; SET max_threads = 1;
                 SET transform_null_in = ${null_in};"
        for predicate in "${predicates[@]}"; do
            filter="${predicate//value/${column}}"
            label="${null_in} | ${column} | ${predicate}"
            for optimize in 0 1; do
                prefix=""
                if [[ "$optimize" == 0 ]]; then
                    prefix="'${label//\'/\\\'}', "
                fi
                queries+="SELECT ${prefix}count() FROM file('${DATA_FILE}', ORC) WHERE ${filter}
                          SETTINGS optimize_functions_to_subcolumns = ${optimize},
                                   input_format_orc_filter_push_down = ${optimize};"
            done
        done
        ${CLICKHOUSE_CLIENT} --format TSVRaw --multiquery --query "$queries" | paste - - | sed 's/\t/ | /g'
    done
done

# NULL-aware exclusion still prunes the stride whose values all belong to the set.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM file('${DATA_FILE}', ORC) WHERE tupleElement(t, 'x') NOT IN (0, 1)
    SETTINGS enable_nullable_tuple_type = 1, enable_analyzer = 1, max_threads = 1,
             transform_null_in = 1, optimize_functions_to_subcolumns = 1, input_format_orc_filter_push_down = 1
    FORMAT JSON" | jq -c '{result: .data, rows_read: .statistics.rows_read}'

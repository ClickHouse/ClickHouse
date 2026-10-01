#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"

# Ten row index strides make pruning observable even when every stride contains parent NULLs.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', ORC)
    SELECT number AS id,
           if(number % 3 = 0, NULL, tuple(number, if(number % 5 = 0, NULL, toString(number % 31))))
               ::Nullable(Tuple(value UInt64, text Nullable(String))) AS t
    FROM numbers(20000)
    SETTINGS enable_nullable_tuple_type = 1, engine_file_truncate_on_insert = 1,
             output_format_orc_row_index_stride = 2000"

run_and_report() {
    local label="$1"
    local predicate="$2"
    local structure="${3:-}"
    local optimize="${4:-1}"
    local projection="${5:-count()}"
    local source="file('${DATA_FILE}', ORC)"
    [ -n "${structure}" ] && source="file('${DATA_FILE}', ORC, '${structure}')"

    ${CLICKHOUSE_CLIENT} --query "
        SELECT ${projection} FROM ${source} WHERE ${predicate}
        SETTINGS enable_nullable_tuple_type = 1, enable_analyzer = 1, max_threads = 1,
                 optimize_functions_to_subcolumns = ${optimize}, input_format_orc_filter_push_down = 1
        FORMAT JSON" \
        | jq -c --arg name "${label}" '{label: $name, result: .data, rows_read: .statistics.rows_read}'
}

run_and_report top_level 'id = 11111'
run_and_report inferred 'tupleElement(t, 1) = 11111'
run_and_report disabled 'tupleElement(t, 1) = 11111' '' 0
run_and_report explicit 'tupleElement(t, -2) = 11111' \
    'id Int64, t Nullable(Tuple(value Int64, text Nullable(String)))'
run_and_report full_tuple 'tupleElement(t, 1) = 11111' '' 1 'toString(t) AS value'
run_and_report parent_null 'isNull(tupleElement(t, 1))'
run_and_report element_null 'isNull(tupleElement(t, 2))'

# Unnamed fields cannot be matched to physical ORC field names by their generated ordinal names.
run_and_report unnamed 'tupleElement(t, 1) = 11111' \
    'id Int64, t Nullable(Tuple(Int64, Nullable(String)))'

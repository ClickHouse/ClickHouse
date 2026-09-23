#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05216_orc_nested_null_as_default_pushdown.lib
. "$CUR_DIR"/05216_orc_nested_null_as_default_pushdown.lib

set -euo pipefail

write_orc_file
run_cases 0

# Predicates unaffected by conversion retain pruning, including nullable child fields.
for structure in 't Nullable(Tuple(x Int64))' 't Nullable(Tuple(x Nullable(Int64)))'; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM file('${DATA_FILE}', ORC, '${structure}') WHERE tupleElement(t, 'x') = 2
        SETTINGS enable_nullable_tuple_type = 1, enable_analyzer = 1, input_format_null_as_default = 1,
                 optimize_functions_to_subcolumns = 1, input_format_orc_filter_push_down = 1, max_threads = 1
        FORMAT JSON" | jq -c '{result: .data, rows_read: .statistics.rows_read}'
done

# A dotted name can identify either a virtual subcolumn or a tuple field, according to field order.
for structure in 'v Tuple(s String, `s.size` UInt64)' 'v Tuple(`s.size` UInt64, s String)'; do
    if [[ "$structure" == 'v Tuple(s String, `s.size` UInt64)' ]]; then
        value="tuple('abc', 99::UInt64)"
    else
        value="tuple(99::UInt64, 'abc')"
    fi
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DATA_FILE}', ORC, '${structure}')
        SELECT ${value} FROM numbers(2000)
        SETTINGS engine_file_truncate_on_insert = 1, output_format_orc_row_index_stride = 2000"
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SELECT count() FROM file('${DATA_FILE}', ORC, '${structure}') WHERE v.s.size = 3
        SETTINGS input_format_orc_filter_push_down = 0;
        SELECT count() FROM file('${DATA_FILE}', ORC, '${structure}') WHERE v.s.size = 3
        SETTINGS input_format_orc_filter_push_down = 1;"
done

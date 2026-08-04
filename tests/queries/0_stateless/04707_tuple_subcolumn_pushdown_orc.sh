#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', ORC)
    SELECT
        number AS id,
        tuple(number, toString(number % 31)) AS tup,
        map(toString(number % 7), number) AS m,
        [tuple(number, 'x')::Tuple(a UInt64, b String)] AS arrtup,
        if(number % 3 = 0, NULL, number)::Nullable(UInt64) AS nn,
        range(number % 4) AS arr,
        [tuple(number, toString(number % 31))]::Nested(na UInt64, nb String) AS n
    FROM numbers(100000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_orc_row_index_stride = 10000"

# SelectedRows is the pruning oracle: liborc skips row index strides whose statistics exclude
# the predicate, so a pruned read reports fewer rows than the file holds.
run_and_report() {
    local label="$1"
    local structure="$2"
    local predicate="$3"
    local extra="$4"
    local query_id="${CLICKHOUSE_DATABASE}_${label}_$RANDOM"
    local source="file('${DATA_FILE}', ORC)"
    [ -n "${structure}" ] && source="file('${DATA_FILE}', ORC, '${structure}')"

    ${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "
        SELECT count() FROM ${source} WHERE ${predicate}
        SETTINGS max_threads = 1, enable_analyzer = 1, optimize_functions_to_subcolumns = 1,
                 input_format_orc_filter_push_down = 1${extra:+, ${extra}}"

    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT '${label}', ProfileEvents['SelectedRows']
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '${query_id}' AND type = 'QueryFinish'
          AND current_database = currentDatabase()"
}

echo '-- pruning: a tuple element must prune like the top-level column holding the same values'
run_and_report top_level '' 'id = 55555'
run_and_report tuple_element '' 'tup.1 = 55555'
run_and_report tuple_element_ci 'id Int64, TUP Tuple(`1` Int64, `2` String)' 'TUP.1 = 55555' 'input_format_orc_case_insensitive_column_matching = 1'

echo '-- refused as key names: only named tuple elements have their own statistics'
run_and_report map_keys '' "has(m.keys, '3')"
run_and_report map_values '' 'length(m.values) = 1'
run_and_report nullable_null '' 'nn.null = 1'
run_and_report array_size '' 'arr.size0 = 2'
run_and_report unnamed_tuple 'id Int64, tup Tuple(Int64, String)' 'tup.1 = 55555'
run_and_report array_of_tuple '' 'has(arrtup.a, 55555)'

echo '-- a structure hint whose element type disagrees with the file must not prune'
run_and_report type_mismatch 'id Int64, tup Tuple(`1` Int32, `2` String)' 'tup.1 = 55555'

# A flattened Nested column is written as list<struct<...>>, so resolving `n.na` descends the
# LIST-of-STRUCT branch, which rewrites the CH type to the element type. Results must stay
# correct whether or not that shape prunes.
echo '-- flattened Nested: the resolver descends a LIST of STRUCT'
run_and_report nested_natural '' 'has(n.na, 55555)'
run_and_report nested_flattened 'id Int64, n Nested(na UInt64, nb String)' 'has(n.na, 55555)'
run_and_report nested_flattened_array 'id Int64, n Nested(na UInt64, nb String)' 'n.na = [55555]'
run_and_report nested_flattened_string 'id Int64, n Nested(na UInt64, nb String)' "n.nb = ['7']"

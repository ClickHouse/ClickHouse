#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"

# 20000 rows at a 2000-row index stride is 10 strides, so a predicate matching inside one stride
# reads a tenth of the file and pruning stays observable.
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
    FROM numbers(20000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_orc_row_index_stride = 2000"

# rows_read is the pruning oracle: liborc skips row index strides whose statistics exclude the
# predicate, so a pruned read reports fewer rows than the file holds.
run_and_report() {
    local label="$1"
    local structure="$2"
    local predicate="$3"
    local extra="$4"
    local source="file('${DATA_FILE}', ORC)"
    [ -n "${structure}" ] && source="file('${DATA_FILE}', ORC, '${structure}')"

    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM ${source} WHERE ${predicate}
        SETTINGS max_threads = 1, enable_analyzer = 1, optimize_functions_to_subcolumns = 1,
                 input_format_orc_filter_push_down = 1${extra:+, ${extra}}
        FORMAT JSON" \
        | jq -c --arg name "${label}" '{label: $name, result: .data, rows_read: .statistics.rows_read}'
}

echo '-- pruning: a tuple element must prune like the top-level column holding the same values'
run_and_report top_level '' 'id = 11111'
run_and_report tuple_element '' 'tup.1 = 11111'
run_and_report tuple_element_ci 'id Int64, TUP Tuple(`1` Int64, `2` String)' 'TUP.1 = 11111' 'input_format_orc_case_insensitive_column_matching = 1'

echo '-- refused as key names: only named tuple elements have their own statistics'
run_and_report map_keys '' "has(m.keys, '3')"
run_and_report map_values '' 'length(m.values) = 1'
run_and_report nullable_null '' 'nn.null = 1'
run_and_report array_size '' 'arr.size0 = 2'
run_and_report unnamed_tuple 'id Int64, tup Tuple(Int64, String)' 'tup.1 = 11111'
run_and_report array_of_tuple '' 'has(arrtup.a, 11111)'

echo '-- a structure hint whose element type disagrees with the file must not prune'
run_and_report type_mismatch 'id Int64, tup Tuple(`1` Int32, `2` String)' 'tup.1 = 11111'

# A flattened Nested column is written as list<struct<...>>, so resolving `n.na` descends the
# LIST-of-STRUCT branch, which rewrites the CH type to the element type. Results must stay
# correct whether or not that shape prunes.
echo '-- flattened Nested: the resolver descends a LIST of STRUCT'
run_and_report nested_natural '' 'has(n.na, 11111)'
run_and_report nested_flattened 'id Int64, n Nested(na UInt64, nb String)' 'has(n.na, 11111)'
run_and_report nested_flattened_array 'id Int64, n Nested(na UInt64, nb String)' 'n.na = [11111]'
run_and_report nested_flattened_string 'id Int64, n Nested(na UInt64, nb String)' "n.nb = ['7']"

# `a.b` and the nested `a`.`b` flatten to the same name. The CH side matches an element name
# exactly while the ORC side resolves that name by walking prefixes of the schema, so the
# flattened name does not identify one field and the rewrite must not happen. Both field orders
# are covered because the prefix walk returns the first match, so only one order misbinds.
echo '-- a dotted element name colliding with a nested path returns the dotted element'
COLLISION_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_collision.orc"

check_collision() {
    local structure="$1"
    local value="$2"

    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${COLLISION_FILE}', ORC, '${structure}')
        SELECT ${value} FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1"

    ${CLICKHOUSE_CLIENT} --query "
        SELECT tupleElement(t, 'a.b'), tupleElement(tupleElement(t, 'a'), 'b')
        FROM file('${COLLISION_FILE}', ORC, '${structure}') LIMIT 1
        SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, input_format_orc_filter_push_down = 1;
        SELECT countIf(tupleElement(t, 'a.b') = 999), countIf(tupleElement(t, 'a.b') = 111)
        FROM file('${COLLISION_FILE}', ORC, '${structure}')
        SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, input_format_orc_filter_push_down = 1"
}

check_collision 't Tuple(a Tuple(b UInt64), `a.b` UInt64)' 'tuple(tuple(111::UInt64), 999::UInt64)'
check_collision 't Tuple(`a.b` UInt64, a Tuple(b UInt64))' 'tuple(999::UInt64, tuple(111::UInt64))'

# The unnamed-tuple hint through a subquery: the subcolumn pushdown pass must keep reading the
# whole tuple as well instead of pushing the ordinal element name into the file read.
echo '-- an unnamed tuple hint through a subquery reads element values, not defaults'
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(tupleElement(tup, 1) = 11111), countIf(tupleElement(tup, 2) = '7')
    FROM (SELECT tup FROM file('${DATA_FILE}', ORC, 'id Int64, tup Tuple(Int64, String)'))
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, optimize_push_subcolumns_into_subqueries = 1"

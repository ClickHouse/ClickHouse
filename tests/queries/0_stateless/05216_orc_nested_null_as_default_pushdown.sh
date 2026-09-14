#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"

# Five strides distinguish outer NULLs, inner tuple NULLs, child NULLs, zeroes, and twos.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', ORC)
    SELECT number AS id,
           if(number < 6000, NULL, if(number < 8000, 0, 2)::Int64) AS x,
           if(number < 2000, NULL, tuple(x))::Nullable(Tuple(x Nullable(Int64))) AS t,
           tuple(x)::Tuple(x Nullable(Int64)) AS p,
           if(number < 2000, NULL, tuple(if(number < 4000, NULL, tuple(x))))
               ::Nullable(Tuple(inner Nullable(Tuple(x Nullable(Int64))))) AS n,
           if(number < 2000, NULL, tuple(x))::Nullable(Tuple(\`a.b\` Nullable(Int64))) AS d,
           if(number < 2000, NULL, tuple(if(isNull(x), NULL, if(x = 0, '', 'z'))))
               ::Nullable(Tuple(s Nullable(String))) AS strings,
           if(number < 2000, NULL, tuple(toDate32('1970-01-01') + toIntervalDay(x)))
               ::Nullable(Tuple(d Nullable(Date32))) AS dates,
           tupleElement(dates, 'd') AS date
    FROM numbers(10000)
    SETTINGS enable_nullable_tuple_type = 1, engine_file_truncate_on_insert = 1,
             output_format_orc_row_index_stride = 2000"

# Every case is independent, so they run concurrently through `xargs -P`, each writing to its own file under
# `CLICKHOUSE_TMP`, and the outputs are printed in the original order afterwards. The concurrency is capped
# because the test runs in a memory-limited cgroup and a sanitizer client takes hundreds of megabytes of
# resident memory, so starting all clients at once gets them OOM-killed.
OUTPUT_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
MAX_CONCURRENT_CLIENTS=4
export DATA_FILE OUTPUT_PREFIX

# The argument is a tab-separated line: null_in, label, structure, column, default value, other value.
run_case()
{
    local null_in label structure column default_value other_value
    IFS=$'\t' read -r null_in label structure column default_value other_value <<< "$1"
    local predicates=(
        'default|value = default_value'
        'not_default|value != default_value'
        'other|value = other_value'
        'less|value < other_value'
        'greater_equal|value >= other_value'
        'is_null|isNull(value)'
        'is_not_null|isNotNull(value)'
        'in|value IN (default_value, other_value)'
        'not_in|value NOT IN (other_value)'
        'in_null|value IN (NULL)'
        'not_in_null|value NOT IN (NULL)'
        'not_and|NOT (value = other_value AND isNotNull(value))'
        'not_or|NOT (value = other_value OR isNull(value))'
    )
    local queries="SET enable_nullable_tuple_type = 1; SET enable_analyzer = 1;
                   SET allow_nullable_tuple_in_extracted_subcolumns = 1;
                   SET allow_suspicious_low_cardinality_types = 1;
                   SET input_format_null_as_default = 1; SET max_threads = 1;
                   SET transform_null_in = ${null_in};"
    local entry predicate name optimize push_down prefix
    for entry in "${predicates[@]}"; do
        name="${entry%%|*}"
        predicate="${entry#*|}"
        predicate="${predicate//default_value/${default_value}}"
        predicate="${predicate//other_value/${other_value}}"
        predicate="${predicate//value/${column}}"
        for optimize in 0 1; do
            for push_down in 0 1; do
                prefix=""
                if [[ "$optimize" == 0 && "$push_down" == 0 ]]; then
                    prefix="'${name}', "
                fi
                queries+="SELECT ${prefix}count(), sum(id)
                          FROM file('${DATA_FILE}', ORC, 'id Int64, ${structure}')
                          WHERE ${predicate}
                          SETTINGS optimize_functions_to_subcolumns = ${optimize},
                                   input_format_orc_filter_push_down = ${push_down},
                                   input_format_orc_case_insensitive_column_matching = 1;"
            done
        done
    done
    {
        printf '%s | transform_null_in=%s\n' "$label" "$null_in"
        # Both counts and row identities must agree across all optimization combinations.
        ${CLICKHOUSE_CLIENT} --multiquery --query "$queries" | paste - - - -
    } > "${OUTPUT_PREFIX}_${label}_${null_in}.out"
}
export -f run_case

cases=()
outputs=()

add_case()
{
    cases+=("$(printf '%s\t%s\t%s\t%s\t%s\t%s' "$null_in" "$@")")
    outputs+=("${OUTPUT_PREFIX}_$1_${null_in}.out")
}

for null_in in 0 1; do
    add_case scalar 'x Int64' x 0 2
    add_case tuple 'p Tuple(x Int64)' "tupleElement(p, 'x')" 0 2
    add_case nullable_tuple 't Nullable(Tuple(x Int64))' "tupleElement(t, 'x')" 0 2
    add_case nullable_child 't Nullable(Tuple(x Nullable(Int64)))' "tupleElement(t, 'x')" 0 2
    add_case dictionary 't Nullable(Tuple(x LowCardinality(Int64)))' "tupleElement(t, 'x')" 0 2
    add_case nullable_dictionary 't Nullable(Tuple(x LowCardinality(Nullable(Int64))))' "tupleElement(t, 'x')" 0 2
    add_case nested_nullable 'n Nullable(Tuple(inner Nullable(Tuple(x Int64))))' \
        "tupleElement(tupleElement(n, 'inner'), 'x')" 0 2
    add_case nested_default 'n Nullable(Tuple(inner Tuple(x Int64)))' \
        "tupleElement(tupleElement(n, 'inner'), 'x')" 0 2
    add_case nested_nullable_child 'n Nullable(Tuple(inner Tuple(x Nullable(Int64))))' \
        "tupleElement(tupleElement(n, 'inner'), 'x')" 0 2
    add_case dotted_name 'd Nullable(Tuple(`a.b` Int64))' "tupleElement(d, 'a.b')" 0 2
    add_case folded_name 'T Nullable(Tuple(X Int64))' "tupleElement(T, 'X')" 0 2
    add_case string 'strings Nullable(Tuple(s String))' "tupleElement(strings, 's')" "''" "'z'"
    add_case string_dictionary 'strings Nullable(Tuple(s LowCardinality(String)))' \
        "tupleElement(strings, 's')" "''" "'z'"
    # `Date32` uses the epoch as its column default when the reader replaces NULLs.
    add_case date_scalar 'date Date32' date "toDate32('1970-01-01')" "toDate32('1970-01-03')"
    add_case date 'dates Nullable(Tuple(d Date32))' "tupleElement(dates, 'd')" \
        "toDate32('1970-01-01')" "toDate32('1970-01-03')"
    add_case date_nullable 'dates Nullable(Tuple(d Nullable(Date32)))' "tupleElement(dates, 'd')" \
        "toDate32('1970-01-01')" "toDate32('1970-01-03')"
    add_case date_dictionary 'dates Nullable(Tuple(d LowCardinality(Date32)))' "tupleElement(dates, 'd')" \
        "toDate32('1970-01-01')" "toDate32('1970-01-03')"
done

printf '%s\n' "${cases[@]}" | xargs -d '\n' -n 1 -P "${MAX_CONCURRENT_CLIENTS}" bash -c 'set -euo pipefail; run_case "$1"' bash
cat "${outputs[@]}"
rm -f "${outputs[@]}"

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

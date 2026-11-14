#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"

# Separate strides contain parent NULLs, child NULLs, zeroes, and twos.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', ORC)
    SELECT number AS id,
           if(number < 2000, NULL,
              tuple(if(number < 4000, NULL, if(number < 6000, 0, 2)::Int64)))
               ::Nullable(Tuple(x Nullable(Int64))) AS t,
           tupleElement(t, 'x') AS x,
           tuple(x)::Tuple(x Nullable(Int64)) AS p,
           if(number < 2000, NULL,
              if(number < 4000, 'aa', if(number < 6000, 'ab', 'ba'))) AS s
    FROM numbers(8000)
    SETTINGS enable_nullable_tuple_type = 1, engine_file_truncate_on_insert = 1,
             output_format_orc_row_index_stride = 2000"

numeric_predicates=(
    'repeated_in|tuple(value, value) IN ((0, 1))'
    'repeated_not_in|tuple(value, value) NOT IN ((0, 1))'
    'repeated_not|NOT (tuple(value, value) IN ((0, 1)))'
    'repeated_not_not|NOT (tuple(value, value) NOT IN ((0, 1)))'
    'partial_in|tuple(value, value + 1) IN ((0, 2))'
    'partial_not_in|tuple(value, value + 1) NOT IN ((0, 2))'
    'partial_other_column|tuple(value, id % 3) NOT IN ((0, 1))'
    'some_matches|tuple(value, value) NOT IN ((0, 0), (2, 1))'
    'conjunction|tuple(value, value) NOT IN ((0, 1)) AND value = 0'
    'disjunction|tuple(value, value) NOT IN ((0, 1)) OR value = 2'
    'outer_not|NOT (tuple(value, value) IN ((0, 1)) OR isNull(value))'
    'null_member|tuple(value, value) NOT IN ((0, 1), (NULL, NULL))'
    'empty_set|tuple(value, value) NOT IN (SELECT tuple(number, number + 1) FROM numbers(0))'
)
string_predicates=(
    "like|value LIKE 'a%b'"
    "not_like|value NOT LIKE 'a%b'"
    "not_like_and|value NOT LIKE 'a%b' AND value = 'aa'"
    "not_like_or|value NOT LIKE 'a%b' OR value = 'ab'"
    "not_like_outer_not|NOT (value LIKE 'a%b' OR isNull(value))"
)

# Every case is independent, so they run concurrently and their outputs are printed in order afterwards.
OUTPUT_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
outputs=()

run_case()
{
    local output="${OUTPUT_PREFIX}_$1_${null_in}.out"
    outputs+=("$output")
    run_case_queries "$@" > "$output" &
}

run_case_queries()
{
    local label="$1" column="$2"
    shift 2
    local queries="SET enable_nullable_tuple_type = 1; SET enable_analyzer = 1;
                   SET transform_null_in = ${null_in}; SET max_threads = 1;"
    local entry predicate name optimize push_down prefix
    for entry in "$@"; do
        name="${entry%%|*}"
        predicate="${entry#*|}"
        predicate="${predicate//value/${column}}"
        for optimize in 0 1; do
            for push_down in 0 1; do
                prefix=""
                if [[ "$optimize" == 0 && "$push_down" == 0 ]]; then
                    prefix="'${name}', "
                fi
                queries+="SELECT ${prefix}count(), sum(id) FROM file('${DATA_FILE}', ORC)
                          WHERE ${predicate}
                          SETTINGS optimize_functions_to_subcolumns = ${optimize},
                                   input_format_orc_filter_push_down = ${push_down};"
            done
        done
    done
    printf '%s | transform_null_in=%s\n' "$label" "$null_in"
    # Each row compares all four combinations of subcolumn rewriting and predicate pushdown.
    ${CLICKHOUSE_CLIENT} --multiquery --query "$queries" | paste - - - -
}

for null_in in 0 1; do
    run_case scalar x "${numeric_predicates[@]}"
    run_case tuple "tupleElement(p, 'x')" "${numeric_predicates[@]}"
    run_case nullable_tuple "tupleElement(t, 'x')" "${numeric_predicates[@]}"
done
run_case string s "${string_predicates[@]}"
wait
cat "${outputs[@]}"
rm -f "${outputs[@]}"

# A relaxed positive condition can still prune strides that cannot contain a match.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM file('${DATA_FILE}', ORC)
    WHERE tuple(tupleElement(t, 'x'), tupleElement(t, 'x')) IN ((0, 0))
    SETTINGS enable_nullable_tuple_type = 1, enable_analyzer = 1, transform_null_in = 1,
             optimize_functions_to_subcolumns = 1, input_format_orc_filter_push_down = 1, max_threads = 1
    FORMAT JSON" | jq -c '{result: .data, rows_read: .statistics.rows_read}'

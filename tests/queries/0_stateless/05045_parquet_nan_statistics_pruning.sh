#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}"
NAN="${PREFIX}_nan.parquet"
NAN_F32="${PREFIX}_nan_f32.parquet"
NAN_NULLABLE="${PREFIX}_nan_nullable.parquet"
NAN_WITH_NULL="${PREFIX}_nan_with_null.parquet"
FINITE="${PREFIX}_finite.parquet"
INTEGER="${PREFIX}_integer.parquet"
MANY="${PREFIX}_many.parquet"

# A float column holding a NaN next to finite values advertises min = max = 5, because Parquet
# statistics are computed from non-NaN values only.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${NAN}', Parquet, 'val Float64')
    SELECT arrayJoin([toFloat64(5), toFloat64(5), nan]) SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${NAN_F32}', Parquet, 'val Float32')
    SELECT arrayJoin([toFloat32(5), toFloat32(5), toFloat32(nan)]) SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${NAN_NULLABLE}', Parquet, 'val Nullable(Float64)')
    SELECT arrayJoin([toFloat64(5), toFloat64(5), nan]) SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${NAN_WITH_NULL}', Parquet, 'val Nullable(Float64)')
    SELECT arrayJoin([toFloat64(5), toFloat64(5), nan, NULL]) SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${FINITE}', Parquet, 'val Float64')
    SELECT arrayJoin([toFloat64(5), toFloat64(5)]) SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${INTEGER}', Parquet, 'i Int64')
    SELECT arrayJoin([toInt64(5), toInt64(5)]) SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${MANY}', Parquet, 'val Float64')
    SELECT toFloat64(number) FROM numbers(30000)
    SETTINGS engine_file_truncate_on_insert = 1,
             output_format_parquet_row_group_size = 10000, output_format_parquet_data_page_size = 1024"

# The statistics the pruning decisions are taken from, and the fact that ClickHouse does not write
# nan_count, so the reader has to assume a NaN may be present.
echo '-- statistics: min and max exclude the NaN'
${CLICKHOUSE_CLIENT} --query "
    SELECT row_groups[1].columns[1].statistics FROM file('${NAN}', ParquetMetadata)"

# Both arms of every correctness check are printed. Pushdown off is the oracle, so a fixture that
# stopped holding a readable NaN would change the reference instead of silently passing.
arm() {
    local label="$1"
    local file="$2"
    local predicate="$3"
    local extra="$4"
    local structure="$5"
    local source="file('${file}', Parquet)"
    [ -n "${structure}" ] && source="file('${file}', Parquet, '${structure}')"

    local on off
    on=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM ${source} WHERE ${predicate}
        SETTINGS input_format_parquet_filter_push_down = 1, max_threads = 1,
                 input_format_parquet_page_filter_push_down = 1${extra:+, ${extra}}")
    off=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM ${source} WHERE ${predicate}
        SETTINGS input_format_parquet_filter_push_down = 0, max_threads = 1,
                 input_format_parquet_page_filter_push_down = 0${extra:+, ${extra}}")
    echo "${label} on=${on} off=${off}"
}

# Same, with row group pushdown disabled, so only the page statistics can prune.
arm_page_only() {
    local label="$1"
    local file="$2"
    local predicate="$3"

    local on off
    on=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM file('${file}', Parquet) WHERE ${predicate}
        SETTINGS input_format_parquet_filter_push_down = 0, max_threads = 1,
                 input_format_parquet_page_filter_push_down = 1")
    off=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM file('${file}', Parquet) WHERE ${predicate}
        SETTINGS input_format_parquet_filter_push_down = 0, max_threads = 1,
                 input_format_parquet_page_filter_push_down = 0")
    echo "${label} on=${on} off=${off}"
}

echo '-- a negated range predicate must still see the NaN row'
arm not_equals "${NAN}" 'val != 5.'
arm not_equals_negated "${NAN}" 'NOT (val = 5.)'
arm not_equals_float32 "${NAN_F32}" 'val != 5.'
arm not_equals_nullable "${NAN_NULLABLE}" 'val != 5.'
arm not_equals_low_cardinality "${NAN}" 'val != 5.' 'allow_suspicious_low_cardinality_types = 1' 'val LowCardinality(Float64)'

echo '-- and so must a negated set predicate'
arm not_in_one "${NAN}" 'val NOT IN (5.)'
arm not_in_two "${NAN}" 'val NOT IN (5., 6.)'
arm not_in_three "${NAN}" 'val NOT IN (5., 6., 7.)'

# Set membership treats NaN as equal to NaN (nan IN (nan) is 1 while nan = nan is 0), so a NaN
# outside the bounds can match the set. This is the direction that needs can_be_true, not can_be_false.
echo '-- a set holding a NaN must match the NaN row the bounds hide'
arm in_nan "${NAN}" 'val IN (nan)'
arm in_finite_and_nan "${NAN}" 'val IN (1., nan)'
arm in_nan_float32 "${NAN_F32}" 'val IN (toFloat32(nan))'
arm in_nan_and_null "${NAN}" 'val IN (nan, NULL)'
arm in_nan_and_null_nullable "${NAN_WITH_NULL}" 'val IN (nan, NULL)'
arm in_null_and_nan_nullable "${NAN_WITH_NULL}" 'val IN (NULL, nan)'

# With transform_null_in the NULL stays in the set and sorts after the NaN, so finding the NaN
# means looking past it.
echo '-- a set ending in NULL still has to find the NaN'
arm in_nan_null_transform "${NAN_WITH_NULL}" 'val IN (nan, NULL)' 'transform_null_in = 1'
arm in_null_nan_transform "${NAN_WITH_NULL}" 'val IN (NULL, nan)' 'transform_null_in = 1'

echo '-- page statistics prune the same way, with row group pushdown disabled'
arm_page_only page_not_equals "${NAN}" 'val != 5.'
arm_page_only page_not_in "${NAN}" 'val NOT IN (5.)'
arm_page_only page_in_nan "${NAN}" 'val IN (nan)'

echo '-- already correct before, must stay correct'
arm equals "${NAN}" 'val = 5.'
arm in_finite "${NAN}" 'val IN (5., 6.)'
arm in_nan_and_present_finite "${NAN}" 'val IN (nan, 5.)'
arm not_in_finite_and_nan "${NAN}" 'val NOT IN (1., nan)'
arm equals_nan "${NAN}" 'val = nan'
arm is_nan "${NAN}" 'isNaN(val)'
arm self_inequality "${NAN}" 'val != val'
arm in_finite_and_null "${NAN_WITH_NULL}" 'val IN (5., NULL)'
arm is_null "${NAN_WITH_NULL}" 'val IS NULL'

# rows_read is the pruning oracle: a pruned row group or page is never read, so a file that is
# still pruned reports fewer rows than it holds. Every arm below must keep pruning everything,
# which is what makes this a check on lost pruning rather than on wrong results.
# input_format_parquet_dictionary_filter_push_down is pinned because the test runner randomizes it.
prune() {
    local label="$1"
    local file="$2"
    local predicate="$3"
    local row_group="${4:-1}"

    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM file('${file}', Parquet) WHERE ${predicate}
        SETTINGS input_format_parquet_filter_push_down = ${row_group}, max_threads = 1,
                 input_format_parquet_page_filter_push_down = 1,
                 input_format_parquet_dictionary_filter_push_down = 1048576
        FORMAT JSON" \
        | jq -c --arg name "${label}" '{label: $name, result: .data, rows_read: .statistics.rows_read}'
}

echo '-- pruning is preserved: nothing below may read a row'
prune float_above_max "${MANY}" 'val > 1e9'
prune float_above_max_pages "${MANY}" 'val > 1e9' 0
prune integer_not_equals "${INTEGER}" 'i != 5'
prune integer_not_in "${INTEGER}" 'i NOT IN (5, 6)'
prune finite_float_in "${FINITE}" 'val IN (6.)'

rm -f "${USER_FILES_PATH}/${PREFIX}"_*.parquet

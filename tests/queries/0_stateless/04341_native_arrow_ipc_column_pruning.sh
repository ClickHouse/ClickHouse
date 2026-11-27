#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests subset-of-columns reading with the native Arrow IPC reader: when a query requests only some
# columns, the native reader must skip (prune) the others while keeping the requested columns correct,
# for both the Arrow (file) and ArrowStream formats. The data is written once with diverse column types (numeric, string,
# fixed_string, list, tuple/struct, map, LowCardinality/dictionary, nullable) so the skip covers many
# Arrow buffer layouts.

write_data() {
    local file="$1" format="$2"
    ${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${file}', '${format}')
    SELECT
        toInt32(number) AS c_i32,
        toString(number) AS c_str,
        toFixedString(toString(number), 4) AS c_fs,
        range(number % 4) AS c_list,
        tuple(number, toString(number)) AS c_tuple,
        map('k', number) AS c_map,
        toLowCardinality(toString(number % 3)) AS c_lc,
        if(number % 2 = 0, NULL, number)::Nullable(UInt64) AS c_null,
        toFloat64(number / 7) AS c_f64
    FROM numbers(20)
    SETTINGS output_format_arrow_string_as_string = 1,
             output_format_arrow_fixed_string_as_fixed_byte_array = 1,
             output_format_arrow_low_cardinality_as_dictionary = 1,
             output_format_arrow_compression_method = 'none',
             engine_file_truncate_on_insert = 1
    "
}

# Reading a subset of columns (which prunes the rest) must return exactly what reading every column and
# then projecting the same subset returns. The reference side materializes `SELECT *` into a temporary
# table first: a subquery would be shrunk back to the requested columns by
# `RemoveUnusedProjectionColumnsPass`, which would compare the pruned reader against itself, while an
# `INSERT` of every column is a barrier the analyzer cannot see through. Any pruning bug therefore shows
# up as a difference between the two.
check_subset() {
    local file="$1" format="$2" cols="$3"
    local pruned full
    pruned=$(${CLICKHOUSE_LOCAL} --query "SELECT ${cols} FROM file('${file}', '${format}') ORDER BY c_i32")
    full=$(${CLICKHOUSE_LOCAL} --query "
    CREATE TEMPORARY TABLE all_columns ENGINE = Memory AS SELECT * FROM file('${file}', '${format}');
    SELECT ${cols} FROM all_columns ORDER BY c_i32;
    ")
    if [ "$pruned" = "$full" ]; then echo "OK   ${format}: ${cols}"; else echo "MISMATCH   ${format}: ${cols}"; fi
}

for FMT in ArrowStream Arrow; do
    DATA_FILE="${CLICKHOUSE_TMP}/04341_prune.${FMT}"
    write_data "${DATA_FILE}" "${FMT}"

    # A single middle column (forces skipping columns on both sides), small subsets covering each layout,
    # a reordered subset, and the full set.
    check_subset "${DATA_FILE}" "${FMT}" "c_lc"
    check_subset "${DATA_FILE}" "${FMT}" "c_i32, c_f64"
    check_subset "${DATA_FILE}" "${FMT}" "c_list, c_map"
    check_subset "${DATA_FILE}" "${FMT}" "c_tuple, c_fs"
    check_subset "${DATA_FILE}" "${FMT}" "c_null"
    check_subset "${DATA_FILE}" "${FMT}" "c_f64, c_str, c_i32"
    check_subset "${DATA_FILE}" "${FMT}" "c_i32, c_str, c_fs, c_list, c_tuple, c_map, c_lc, c_null, c_f64"

    rm -f "${DATA_FILE}"
done

echo "--- values of a pruned subset (native) ---"
DATA_FILE="${CLICKHOUSE_TMP}/04341_prune.ArrowStream"
write_data "${DATA_FILE}" "ArrowStream"
${CLICKHOUSE_LOCAL} --query "SELECT c_i32, c_lc, c_list FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY c_i32 LIMIT 5"

echo "--- count() only (native, no columns decoded) ---"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${DATA_FILE}', 'ArrowStream')"
rm -f "${DATA_FILE}"

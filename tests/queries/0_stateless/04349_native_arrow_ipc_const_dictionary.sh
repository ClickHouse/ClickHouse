#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Dictionary substitution in the native Arrow writer runs before the encoder materializes constants, so a
# constant `LowCardinality` column (or a constant container with a nested `LowCardinality`) must be
# materialized rather than reaching the `ColumnLowCardinality` cast as a `ColumnConst`. With
# output_format_arrow_low_cardinality_as_dictionary = 1 the native writer must produce valid output for such
# columns (it used to throw a bad-cast exception), and the file must read back identically through the
# native and the Apache Arrow library reader, for both the Arrow and ArrowStream formats.

DATA_FILE="${CLICKHOUSE_TMP}/04349_const_dict"

for FMT in Arrow ArrowStream; do
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DATA_FILE}.${FMT}', '${FMT}')
        SELECT
            'x'::LowCardinality(String) AS lc,
            [toLowCardinality('y'), toLowCardinality('z')]::Array(LowCardinality(String)) AS arr_lc,
            tuple(toLowCardinality('t'), number)::Tuple(a LowCardinality(String), b UInt64) AS tup_lc
        FROM numbers(4)
        SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1,
                 output_format_arrow_use_native_writer = 1,
                 output_format_arrow_string_as_string = 1,
                 output_format_arrow_compression_method = 'none',
                 engine_file_truncate_on_insert = 1
    "

    native=$(${CLICKHOUSE_LOCAL}  --query "SELECT lc, arr_lc, tup_lc FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY tup_lc.2 SETTINGS input_format_arrow_use_native_reader = 1")
    library=$(${CLICKHOUSE_LOCAL} --query "SELECT lc, arr_lc, tup_lc FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY tup_lc.2 SETTINGS input_format_arrow_use_native_reader = 0")
    if [ "$native" = "$library" ]; then echo "OK native==library | ${FMT}"; echo "$native"; else echo "MISMATCH | ${FMT}"; fi

    rm -f "${DATA_FILE}.${FMT}"
done

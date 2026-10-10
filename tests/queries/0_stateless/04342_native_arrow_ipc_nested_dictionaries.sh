#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native Arrow writer encoding nested LowCardinality columns as Arrow dictionaries
# (output_format_arrow_low_cardinality_as_dictionary=1): a LowCardinality inside Array/Tuple/Map must be
# written as a nested Arrow Dictionary (its own id + DictionaryBatch), not materialized to plain values.
# The native-written file must read back identically through the native reader and the Apache Arrow
# library reader, for both the Arrow (file) and ArrowStream formats.

DATA_FILE="${CLICKHOUSE_TMP}/04342_nested_dict"

for FMT in ArrowStream Arrow; do
    ${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}.${FMT}', '${FMT}')
    SELECT
        toLowCardinality(toString(number % 3)) AS lc,
        [toLowCardinality(toString(number % 4)), toLowCardinality(toString(number % 2))] AS arr_lc,
        tuple(toLowCardinality(toString(number % 5)), number) AS tup_lc,
        map(toLowCardinality(toString(number % 6)), number) AS map_lc,
        if(number % 2, NULL, toLowCardinality(toString(number % 7)))::LowCardinality(Nullable(String)) AS lc_null
    FROM numbers(30)
    SETTINGS output_format_arrow_string_as_string = 1,
             output_format_arrow_low_cardinality_as_dictionary = 1,
             output_format_arrow_compression_method = 'none',
             engine_file_truncate_on_insert = 1
    "

    # Compare the row multiset (sort the rows): both readers see the same file, so they must return the
    # same rows, but the row order within equal `(lc, arr_lc)` groups is not deterministic under the
    # randomized thread/block settings the test runner injects, so do not depend on it.
    NATIVE=$(${CLICKHOUSE_LOCAL}  --query "SELECT * FROM file('${DATA_FILE}.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 1" | sort)
    LIBRARY=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 0" | sort)
    [ "$NATIVE" = "$LIBRARY" ] && echo "OK   ${FMT}: native == library" || echo "MISMATCH   ${FMT}"

    rm -f "${DATA_FILE}.${FMT}"
done

echo "--- inferred schema of native-written nested dictionaries (native reader) ---"
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}.ArrowStream', 'ArrowStream')
SELECT
    toLowCardinality(toString(number)) AS lc,
    [toLowCardinality(toString(number))] AS arr_lc
FROM numbers(3)
SETTINGS output_format_arrow_string_as_string = 1, output_format_arrow_low_cardinality_as_dictionary = 1,
         output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1
"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}.ArrowStream', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- values (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}.ArrowStream', 'ArrowStream') ORDER BY lc SETTINGS input_format_arrow_use_native_reader = 1"

rm -f "${DATA_FILE}.ArrowStream"

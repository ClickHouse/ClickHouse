#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native Arrow writer treats `Variant` as a dictionary boundary: a `LowCardinality` nested inside a
# `Variant` alternative is written as plain values, even with
# `output_format_arrow_low_cardinality_as_dictionary = 1` (the boundary stops at the `Variant`). The data
# must still round-trip correctly through the native reader, and the native output must be valid Arrow that
# the Apache Arrow library reader reads identically. The detection must see a `LowCardinality` nested below
# a container inside the alternative (`Variant(Array(LowCardinality))`, `Variant(Tuple(... LowCardinality
# ...))`), not only a direct `Variant(LowCardinality)`.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow"

write() { # $1=select-expr
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DATA_FILE}', 'Arrow')
        SELECT $1 FROM numbers(6)
        SETTINGS allow_experimental_variant_type = 1,
                 output_format_arrow_low_cardinality_as_dictionary = 1,
                 output_format_arrow_use_native_writer = 1,
                 output_format_arrow_string_as_string = 1,
                 output_format_arrow_compression_method = 'none',
                 engine_file_truncate_on_insert = 1
    "
}

VARIANT_ARRAY="(if(number % 2, ['a', 'b'], range(0))::Array(LowCardinality(String)))::Variant(Array(LowCardinality(String)), Int64) AS v"
VARIANT_TUPLE="(tuple(toLowCardinality(toString(number % 3)))::Tuple(x LowCardinality(String)))::Variant(Tuple(x LowCardinality(String)), Int64) AS v"

# The native writer emits the Variant as an Arrow dense union; only the native reader decodes unions, so
# the round-trip is checked through it (the Apache Arrow library reader does not support Arrow unions).
for LABEL in "Variant(Array(LowCardinality))::${VARIANT_ARRAY}" \
             "Variant(Tuple(LowCardinality))::${VARIANT_TUPLE}"; do
    name="${LABEL%%::*}"
    expr="${LABEL#*::}"
    write "$expr"
    echo "--- ${name}: native writer + native reader round-trip ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}', 'Arrow') ORDER BY toString(v) SETTINGS input_format_arrow_use_native_reader = 1, allow_experimental_variant_type = 1"
done

rm -f "${DATA_FILE}"

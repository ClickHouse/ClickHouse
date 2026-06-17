#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native Arrow writer treats `Variant` as a dictionary boundary: it cannot dictionary-encode a
# `LowCardinality` nested inside a `Variant` alternative. With output_format_arrow_low_cardinality_as_dictionary=1
# such a column must be deferred to the Apache Arrow library writer so the union child keeps its
# dictionary-encoded schema, instead of being silently materialized to plain values. The detection has to
# see a `LowCardinality` nested below a container inside the alternative (`Variant(Array(LowCardinality))`,
# `Variant(Tuple(... LowCardinality ...))`), not only a direct `Variant(LowCardinality)`.
#
# Because the whole block then uses the library writer, writing with output_format_arrow_use_native_writer=1
# must produce a byte-identical file to writing with it disabled. A `LowCardinality` that is NOT under a
# `Variant` is still encoded by the native writer (no over-fallback), so there the two writers differ.

DATA_FILE="${CLICKHOUSE_TMP}/04347_variant_lc"

write() { # $1=native_writer $2=outfile $3=select-expr
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('$2', 'Arrow')
        SELECT $3 FROM numbers(6)
        SETTINGS allow_experimental_variant_type = 1,
                 output_format_arrow_low_cardinality_as_dictionary = 1,
                 output_format_arrow_use_native_writer = $1,
                 output_format_arrow_string_as_string = 1,
                 output_format_arrow_compression_method = 'none',
                 engine_file_truncate_on_insert = 1
    "
}

VARIANT_ARRAY="(if(number % 2, ['a', 'b'], range(0))::Array(LowCardinality(String)))::Variant(Array(LowCardinality(String)), Int64) AS v"
VARIANT_TUPLE="(tuple(toLowCardinality(toString(number % 3)))::Tuple(x LowCardinality(String)))::Variant(Tuple(x LowCardinality(String)), Int64) AS v"
PLAIN_ARRAY="[toLowCardinality(toString(number % 3))] AS arr"

for LABEL in "Variant(Array(LowCardinality))::${VARIANT_ARRAY}" \
             "Variant(Tuple(LowCardinality))::${VARIANT_TUPLE}"; do
    name="${LABEL%%::*}"
    expr="${LABEL#*::}"
    write 1 "${DATA_FILE}.native" "$expr"
    write 0 "${DATA_FILE}.library" "$expr"
    if cmp -s "${DATA_FILE}.native" "${DATA_FILE}.library"; then
        echo "OK   ${name}: native writer deferred to the library writer (identical, dictionary schema kept)"
    else
        echo "FAIL ${name}: native writer output differs from the library writer"
    fi
    # The data must still read back correctly through the native reader.
    ${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}.native', 'Arrow') ORDER BY toString(v) SETTINGS input_format_arrow_use_native_reader = 1, allow_experimental_variant_type = 1"
done

# Control: a LowCardinality NOT under a Variant is dictionary-encoded by the native writer itself, so the
# native and library writers produce different bytes (no over-fallback).
write 1 "${DATA_FILE}.native" "$PLAIN_ARRAY"
write 0 "${DATA_FILE}.library" "$PLAIN_ARRAY"
if cmp -s "${DATA_FILE}.native" "${DATA_FILE}.library"; then
    echo "FAIL Array(LowCardinality): native writer unexpectedly deferred to the library writer"
else
    echo "OK   Array(LowCardinality): encoded by the native writer (no over-fallback)"
fi

rm -f "${DATA_FILE}.native" "${DATA_FILE}.library"

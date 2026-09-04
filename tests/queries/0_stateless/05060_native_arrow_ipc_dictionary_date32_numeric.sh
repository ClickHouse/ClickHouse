#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Nested file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `date32` read into a numeric target returns the raw day number without the `Date32` range check; a
# dictionary-encoded `date32` — what `output_format_arrow_low_cardinality_as_dictionary` writes for a
# `LowCardinality(Date32)` column — must do the same. The dictionary batch is decoded before any record
# batch, so the reader carries the encoding field's requested type to it. ClickHouse itself writes the
# out-of-range day number below: `Date32` arithmetic does not clamp, so the stored value cannot be read
# back as `Date32` at all, while a numeric target must return it verbatim.
#
# Covers the top-level dictionary under plain and `LowCardinality` numeric targets and under a `Decimal`
# target (the Int -> Decimal cast needs the raw read too), dictionaries nested in Array/Tuple/Map, a
# `Nested` subcolumn target resolved through the dotted column name — with the dictionary inside the struct
# and with the whole `Nested` column dictionary-encoded — and that a `Date32` target still rejects the value.

PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

for FORMAT in Arrow ArrowStream; do
    FILE="${PREFIX}_lc.${FORMAT}"
    ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 --query "
        INSERT INTO FUNCTION file('${FILE}', '${FORMAT}')
        SELECT
            toLowCardinality(d) AS d,
            [toLowCardinality(d), toLowCardinality(toDate32('2020-01-02'))] AS arr,
            tuple(toLowCardinality(d))::Tuple(x LowCardinality(Date32)) AS tup,
            map('k', toLowCardinality(d)) AS m
        FROM (SELECT toDate32('9999-12-31') + 100 AS d)
        SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1, engine_file_truncate_on_insert = 1"

    echo "--- ${FORMAT}: numeric targets return the raw day number ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT * FROM file('${FILE}', '${FORMAT}', 'd Int32, arr Array(Int32), tup Tuple(x Int32), m Map(String, Int32)')"
    ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 --query "
        SELECT * FROM file('${FILE}', '${FORMAT}', 'd LowCardinality(Int32), arr Array(LowCardinality(Int32))')"
    echo "--- ${FORMAT}: Decimal target ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT d FROM file('${FILE}', '${FORMAT}', 'd Decimal(10, 0)')"
    echo "--- ${FORMAT}: Date32 target still rejects the out-of-range day ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT d FROM file('${FILE}', '${FORMAT}', 'd Date32')" 2>&1 | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -1
    rm -f "${FILE}"

    python3 - "${PREFIX}" "${FORMAT}" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

prefix, fmt = sys.argv[1], sys.argv[2]
new_writer = ipc.new_file if fmt == "Arrow" else ipc.new_stream

# list<struct<d: dictionary<date32>>>: the `Nested` shape, read through the `n.d` subcolumn name. One day
# far outside the `Date32` range and one ordinary day.
days = pa.array([3000000, 19000], type=pa.date32())
d = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], type=pa.int32()), days)
n = pa.ListArray.from_arrays(pa.array([0, 2, 3], type=pa.int32()), pa.StructArray.from_arrays([d], names=["d"]))
schema = pa.schema([pa.field("n", n.type)])
with new_writer(f"{prefix}_nested.{fmt}", schema) as w:
    w.write_batch(pa.record_batch([n], schema=schema))

# The whole `Nested` column dictionary-encoded: dictionary<list<struct<d: date32>>>, again read through `n.d`.
inner = pa.StructArray.from_arrays([pa.array([3000000, 19000, 3000000], type=pa.date32())], names=["d"])
lists = pa.ListArray.from_arrays(pa.array([0, 2, 3], type=pa.int32()), inner)
dictionary_nested = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], type=pa.int32()), lists)
schema = pa.schema([pa.field("n", dictionary_nested.type)])
with new_writer(f"{prefix}_dictionary_nested.{fmt}", schema) as w:
    w.write_batch(pa.record_batch([dictionary_nested], schema=schema))
PY
    echo "--- ${FORMAT}: Nested subcolumn target n.d Array(Int32) ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${PREFIX}_nested.${FORMAT}', '${FORMAT}', '\`n.d\` Array(Int32)')"
    rm -f "${PREFIX}_nested.${FORMAT}"
    echo "--- ${FORMAT}: dictionary-encoded Nested column, subcolumn target n.d Array(Int32) ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${PREFIX}_dictionary_nested.${FORMAT}', '${FORMAT}', '\`n.d\` Array(Int32)')"
    rm -f "${PREFIX}_dictionary_nested.${FORMAT}"
done

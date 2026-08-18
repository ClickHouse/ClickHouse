#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `output_format_arrow_unsupported_types` decides what the Arrow writer does with a column whose type has
# no first-class Arrow mapping. The values are written through `ISerialization`, not through
# `IColumn::getDataAt`: `JSON`, `Dynamic` and `QBit` have no contiguous in-memory representation and used to
# fail with "Method getDataAt is not supported", while `AggregateFunction` used to export the raw
# `AggregateDataPtr` - heap addresses instead of the aggregate state.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"

write() {
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') $1
        SETTINGS output_format_arrow_compression_method = 'none',
                 engine_file_truncate_on_insert = 1
                 ${2:+, $2}" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -1
}
read_back() { ${CLICKHOUSE_LOCAL} --query "SELECT toTypeName(x), $1 FROM file('${DATA_FILE}', 'ArrowStream')"; }

for query in "SELECT '{\"a\":1,\"b\":\"s\"}'::JSON AS x" \
             "SELECT 42::Dynamic AS x" \
             "SELECT [1,2]::Array(Dynamic) AS x" \
             "SELECT sumState(number) AS x FROM numbers(3)"
do
    echo "=== ${query}"

    echo "--- text ---"
    write "${query}" "output_format_arrow_unsupported_types = 'text'"
    read_back "x"

    echo "--- binary: round-trips back into the original type ---"
    write "${query}" "output_format_arrow_unsupported_types = 'binary'"
    read_back "length(x) > 0"

    echo "--- throw ---"
    write "${query}" "output_format_arrow_unsupported_types = 'throw'"
done

# `binary` is the default and matches what the old boolean did, so an unset `output_format_arrow_unsupported_types`
# keeps honouring `output_format_arrow_unsupported_types_as_binary`. An explicit mode wins over the boolean
# whichever order the two are given in.
echo "=== old boolean still works ==="
write "SELECT 42::Dynamic AS x" "output_format_arrow_unsupported_types_as_binary = 0"
write "SELECT 42::Dynamic AS x" "output_format_arrow_unsupported_types_as_binary = 1"
read_back "length(x) > 0"

echo "=== explicit mode wins over the boolean ==="
write "SELECT 42::Dynamic AS x" "output_format_arrow_unsupported_types_as_binary = 0, output_format_arrow_unsupported_types = 'text'"
read_back "x"
write "SELECT 42::Dynamic AS x" "output_format_arrow_unsupported_types = 'throw', output_format_arrow_unsupported_types_as_binary = 1"

# An aggregate state written in `binary` mode is the same encoding `RowBinary` uses, so it deserializes back
# into the original `AggregateFunction` type. Before this went through `ISerialization` the column carried
# `AggregateDataPtr` values, which are heap addresses and cannot be read back at all.
echo "=== AggregateFunction binary round-trip ==="
write "SELECT sumState(number) AS x FROM numbers(11)" "output_format_arrow_unsupported_types = 'binary'"
${CLICKHOUSE_LOCAL} --query "
    SELECT finalizeAggregation(CAST(unhex(hex(x)) AS AggregateFunction(sum, UInt64)))
    FROM file('${DATA_FILE}', 'ArrowStream')"

# The opaque column is tagged as an Arrow extension type carrying the original ClickHouse type name, so a
# consumer can tell it apart from a genuine string or binary column. A reader that does not know the
# extension name (as here, `pyarrow`) sees the plain storage type.
echo "=== pyarrow: storage type and clickhouse.opaque tag ==="
for mode in text binary; do
    write "SELECT '{\"a\":1}'::JSON AS x" "output_format_arrow_unsupported_types = '${mode}'"
    python3 - "${DATA_FILE}" "${mode}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    schema = pa.ipc.open_stream(source).schema

field = schema.field("x")
metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
print(sys.argv[2], field.type, metadata.get("ARROW:extension:name"), metadata.get("ARROW:extension:metadata"), sep="\t")
PY
done

rm -f "${DATA_FILE}"

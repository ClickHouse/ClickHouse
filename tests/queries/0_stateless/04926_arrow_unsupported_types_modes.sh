#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `output_format_arrow_unsupported_types` decides what the Arrow writer does with a column whose type has
# no first-class Arrow mapping. The values are written through `ISerialization`, not through
# `IColumn::getDataAt`, which `JSON`, `Dynamic` and `QBit` do not implement and which yields the
# `AggregateDataPtr` (a heap address, not the state) for `AggregateFunction`.
#
# Process startup dominates the runtime of this test, so statements are batched into as few
# `clickhouse-local` invocations as possible. A rejected write has to stay on its own, because the error
# ends the batch and `--ignore-error` hides it.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"

insert() {
    echo "INSERT INTO FUNCTION file('${2:-$DATA_FILE}', 'ArrowStream') $1
          SETTINGS output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1, $3;"
}

# Prints the error code, so that a rejection is visible in the reference rather than being an empty line.
rejected() { ${CLICKHOUSE_LOCAL} --query "$1" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -1; }

# Every value is printed hex-encoded: an aggregate state and the binary encodings contain NUL bytes.
check() {
    local query="$1" display="$2"
    echo "=== ${query}"
    ${CLICKHOUSE_LOCAL} --multiquery --query "
        $(insert "${query}" "" "output_format_arrow_unsupported_types = 'text'")
        SELECT 'text', toTypeName(x), ${display} FROM file('${DATA_FILE}', 'ArrowStream');
        $(insert "${query}" "" "output_format_arrow_unsupported_types = 'binary'")
        SELECT 'binary', toTypeName(x), ${display} FROM file('${DATA_FILE}', 'ArrowStream');"
    printf 'throw\t'
    rejected "$(insert "${query}" "" "output_format_arrow_unsupported_types = 'throw'")"
}

check "SELECT '{\"a\":1,\"b\":\"s\"}'::JSON AS x" "hex(x)"
check "SELECT 42::Dynamic AS x" "hex(x)"
check "SELECT [1,2]::Array(Dynamic) AS x" "arrayMap(v -> hex(v), x)"
check "SELECT sumState(number) AS x FROM numbers(3)" "hex(x)"
# `QBit` reaches the same path through its own `SerializationQBit` rather than the JSON/Dynamic ones.
check "SELECT [1,2,3]::QBit(BFloat16, 3) AS x" "hex(x)"

# `binary` is the default and matches what the old boolean did, so an unset `output_format_arrow_unsupported_types`
# keeps honouring `output_format_arrow_unsupported_types_as_binary`. An explicit mode wins over the boolean
# whichever order the two are given in.
echo "=== the old boolean, and precedence over it ==="
DYNAMIC="SELECT 42::Dynamic AS x"
printf 'boolean=0\t'; rejected "$(insert "${DYNAMIC}" "" "output_format_arrow_unsupported_types_as_binary = 0")"
printf 'throw wins over boolean=1\t'
rejected "$(insert "${DYNAMIC}" "" "output_format_arrow_unsupported_types = 'throw', output_format_arrow_unsupported_types_as_binary = 1")"
${CLICKHOUSE_LOCAL} --multiquery --query "
    $(insert "${DYNAMIC}" "" "output_format_arrow_unsupported_types_as_binary = 1")
    SELECT 'boolean=1', hex(x) FROM file('${DATA_FILE}', 'ArrowStream');
    $(insert "${DYNAMIC}" "" "output_format_arrow_unsupported_types_as_binary = 0, output_format_arrow_unsupported_types = 'text'")
    SELECT 'text wins over boolean=0', hex(x) FROM file('${DATA_FILE}', 'ArrowStream');"

# The opaque column is tagged as an Arrow extension type carrying the original ClickHouse type name, so a
# consumer can tell it apart from a genuine string or binary column. A reader that does not know the
# extension name (as here, `pyarrow`) sees the plain storage type. An aggregate state stays `binary` even in
# `text` mode: `serializeText` writes its raw state bytes, and an Arrow `string` column must hold valid UTF-8.
# A map's key is tagged too - `Map(JSON, ...)` is a legal type. 128 puts the high bit in the first byte of
# the state, so that payload is not valid UTF-8.
echo "=== pyarrow: storage type and clickhouse.opaque tag ==="
JSON_Q="SELECT '{\"a\":1}'::JSON AS x"
AGG_Q="SELECT sumState(toUInt64(128)) AS x"
MAP_Q="SELECT CAST(map('{\"a\":1}', 1), 'Map(JSON, UInt8)') AS x"

${CLICKHOUSE_LOCAL} --multiquery --query "
    $(insert "${JSON_Q}" "${DATA_FILE}.json_text"    "output_format_arrow_unsupported_types = 'text'")
    $(insert "${JSON_Q}" "${DATA_FILE}.json_binary"  "output_format_arrow_unsupported_types = 'binary'")
    $(insert "${AGG_Q}"  "${DATA_FILE}.agg_text"     "output_format_arrow_unsupported_types = 'text'")
    $(insert "${AGG_Q}"  "${DATA_FILE}.agg_binary"   "output_format_arrow_unsupported_types = 'binary'")
    $(insert "${MAP_Q}"  "${DATA_FILE}.map_text"     "output_format_arrow_unsupported_types = 'text'")
    $(insert "${MAP_Q}"  "${DATA_FILE}.map_binary"   "output_format_arrow_unsupported_types = 'binary'")"

python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa


def dump(label, field, path):
    metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
    print(label, path, field.type, metadata.get("ARROW:extension:name"),
          metadata.get("ARROW:extension:metadata"), sep="\t")
    for i in range(field.type.num_fields):
        dump(label, field.type.field(i), f"{path}.{field.type.field(i).name}")


for label in ["json/text", "json/binary", "aggregate/text", "aggregate/binary", "map/text", "map/binary"]:
    suffix = label.replace("json/", "json_").replace("aggregate/", "agg_").replace("map/", "map_")
    with pa.OSFile(f"{sys.argv[1]}.{suffix}", "rb") as source:
        schema = pa.ipc.open_stream(source).schema
    for f in schema:
        dump(label, f, f.name)
PY

# A text payload is declared `utf8`, the Arrow type a `String` column uses, and follows the same setting: a
# `Dynamic` holding a `String` is only as valid UTF-8 as that string is, exactly like a `String` column, so
# `output_format_arrow_string_as_string = 0` drops the claim for both while keeping the payload text.
echo "=== text payload under output_format_arrow_string_as_string ==="
${CLICKHOUSE_LOCAL} --multiquery --query "
    $(insert "SELECT '{\"a\":1}'::JSON AS x" "${DATA_FILE}.utf8_on"  "output_format_arrow_unsupported_types = 'text', output_format_arrow_string_as_string = 1")
    $(insert "SELECT '{\"a\":1}'::JSON AS x" "${DATA_FILE}.utf8_off" "output_format_arrow_unsupported_types = 'text', output_format_arrow_string_as_string = 0")"
python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa

for label, suffix in [("string_as_string=1", "utf8_on"), ("string_as_string=0", "utf8_off")]:
    with pa.OSFile(f"{sys.argv[1]}.{suffix}", "rb") as source:
        table = pa.ipc.open_stream(source).read_all()
    field = table.schema.field("x")
    metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
    print(label, field.type, metadata.get("ARROW:extension:name"),
          table.column("x").cast(pa.binary()).to_pylist()[0].decode(), sep="\t")
PY

# An aggregate state written in either mode is the encoding `RowBinary` uses, so it deserializes back into
# the original `AggregateFunction` type.
echo "=== AggregateFunction round-trip ==="
${CLICKHOUSE_LOCAL} --multiquery --query "
    SELECT 'text', finalizeAggregation(CAST(x AS AggregateFunction(sum, UInt64)))
    FROM file('${DATA_FILE}.agg_text', 'ArrowStream');
    SELECT 'binary', finalizeAggregation(CAST(x AS AggregateFunction(sum, UInt64)))
    FROM file('${DATA_FILE}.agg_binary', 'ArrowStream');"

rm -f "${DATA_FILE}" "${DATA_FILE}".*

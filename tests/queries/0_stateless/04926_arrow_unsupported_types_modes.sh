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
# Starting `clickhouse-local` costs far more than any query here, and under a sanitizer build enough to
# blow the test's time limit, so the whole matrix is written and read back by as few invocations as
# possible: all five types travel as columns of one row. Only a rejected write needs its own invocation,
# because the error ends the batch and `--ignore-error` hides it.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
COMMON="output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"

# 128 puts the high bit in the first byte of the aggregate state, so that payload is not valid UTF-8.
ALL_TYPES="SELECT '{\"a\":1,\"b\":\"s\"}'::JSON AS j,
                  42::Dynamic AS d,
                  [1,2]::Array(Dynamic) AS a,
                  (SELECT sumState(toUInt64(128))) AS s,
                  [1,2,3]::QBit(BFloat16, 3) AS q"
MAP_TYPE="SELECT CAST(map('{\"a\":1}', 1), 'Map(JSON, UInt8)') AS m"

insert() { echo "INSERT INTO FUNCTION file('$1', 'ArrowStream') $2 SETTINGS ${COMMON}, $3;"; }
# Every value is printed hex-encoded: an aggregate state and the binary encodings contain NUL bytes.
read_all() {
    echo "SELECT '$1' AS mode, hex(j) AS json, hex(d) AS dynamic, arrayMap(v -> hex(v), a) AS array_dynamic,
                 hex(s) AS aggregate, hex(q) AS qbit,
                 finalizeAggregation(CAST(s AS AggregateFunction(sum, UInt64))) AS state
          FROM file('$2', 'ArrowStream') FORMAT Vertical;"
}
rejected() { ${CLICKHOUSE_LOCAL} --query "$1" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -1; }

echo "=== text and binary, all five types, with the aggregate state read back ==="
${CLICKHOUSE_LOCAL} --multiquery --query "
    $(insert "${FILE}.text"     "${ALL_TYPES}" "output_format_arrow_unsupported_types = 'text'")
    $(insert "${FILE}.binary"   "${ALL_TYPES}" "output_format_arrow_unsupported_types = 'binary'")
    $(insert "${FILE}.map_text" "${MAP_TYPE}"  "output_format_arrow_unsupported_types = 'text'")
    $(insert "${FILE}.map_bin"  "${MAP_TYPE}"  "output_format_arrow_unsupported_types = 'binary'")
    $(insert "${FILE}.utf8_off" "${ALL_TYPES}" "output_format_arrow_unsupported_types = 'text', output_format_arrow_string_as_string = 0")
    $(read_all text "${FILE}.text")
    $(read_all binary "${FILE}.binary")"

echo "=== throw ==="
rejected "$(insert "${FILE}.throw" "${ALL_TYPES}" "output_format_arrow_unsupported_types = 'throw'")"

# `binary` is the default and matches what the old boolean did, so an unset `output_format_arrow_unsupported_types`
# keeps honouring `output_format_arrow_unsupported_types_as_binary`. An explicit mode wins over the boolean
# whichever order the two are given in.
echo "=== the old boolean, and precedence over it ==="
DYNAMIC="SELECT 42::Dynamic AS x"
printf 'boolean=0\t'; rejected "$(insert "${FILE}.b" "${DYNAMIC}" "output_format_arrow_unsupported_types_as_binary = 0")"
printf 'throw wins over boolean=1\t'
rejected "$(insert "${FILE}.b" "${DYNAMIC}" "output_format_arrow_unsupported_types = 'throw', output_format_arrow_unsupported_types_as_binary = 1")"
${CLICKHOUSE_LOCAL} --multiquery --query "
    $(insert "${FILE}.b" "${DYNAMIC}" "output_format_arrow_unsupported_types_as_binary = 1")
    SELECT 'boolean=1' AS mode, hex(x) FROM file('${FILE}.b', 'ArrowStream');
    $(insert "${FILE}.b" "${DYNAMIC}" "output_format_arrow_unsupported_types_as_binary = 0, output_format_arrow_unsupported_types = 'text'")
    SELECT 'text wins over boolean=0' AS mode, hex(x) FROM file('${FILE}.b', 'ArrowStream');"

# The opaque column is tagged as an Arrow extension type carrying the original ClickHouse type name, so a
# consumer can tell it apart from a genuine string or binary column. A reader that does not know the
# extension name (as here, `pyarrow`) sees the plain storage type. An aggregate state stays `binary` even in
# `text` mode, because `serializeText` writes its raw state bytes and an Arrow `string` column must hold
# valid UTF-8; a text payload otherwise follows `output_format_arrow_string_as_string`, exactly as a
# `String` column does. A map's key is tagged too - `Map(JSON, ...)` is a legal type.
echo "=== pyarrow: storage type and clickhouse.opaque tag ==="
python3 - "${FILE}" <<'PY'
import sys
import pyarrow as pa


def dump(label, field, path):
    metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
    print(label, path, field.type, metadata.get("ARROW:extension:name"),
          metadata.get("ARROW:extension:metadata"), sep="\t")
    for i in range(field.type.num_fields):
        dump(label, field.type.field(i), f"{path}.{field.type.field(i).name}")


for label, suffix in [("text", "text"), ("binary", "binary"), ("map/text", "map_text"),
                      ("map/binary", "map_bin"), ("text,string_as_string=0", "utf8_off")]:
    with pa.OSFile(f"{sys.argv[1]}.{suffix}", "rb") as source:
        schema = pa.ipc.open_stream(source).schema
    for f in schema:
        dump(label, f, f.name)
PY

rm -f "${FILE}".*

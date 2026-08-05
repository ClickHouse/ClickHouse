#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native ClickHouse Arrow IPC reader and writer on nested types (Array, Tuple, Map, nested Array)
# and LowCardinality (Arrow dictionary) columns, including LowCardinality(Nullable(...)).
#
# The same logical data is exercised twice, so that a bug shared by the native writer and the native reader
# cannot hide: once as a native round-trip, and once from a fixture produced by `pyarrow`, an independent
# Arrow implementation. Both must read back identically. Additionally, the natively written file is read
# with `pyarrow` so that the bytes the native writer emits are validated by an outside consumer too.

DATA_FILE="${CLICKHOUSE_TMP}/04315_nested.arrows"
PYARROW_FILE="${CLICKHOUSE_TMP}/04315_nested_pyarrow.arrows"

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream')
SELECT
    range(number) AS arr,
    arrayMap(x -> if(x % 2 = 0, NULL, x), range(number))::Array(Nullable(UInt32)) AS arr_nullable,
    (number, toString(number)) AS tup,
    map(toString(number), number, 'k', number * 2) AS m,
    toLowCardinality(toString(number % 3)) AS lc,
    toLowCardinality(if(number % 2 = 0, NULL, toString(number)))::LowCardinality(Nullable(String)) AS lc_null,
    [[toUInt8(1), 2], [3]]::Array(Array(UInt8)) AS nested_arr
FROM numbers(5)
SETTINGS output_format_arrow_string_as_string = 1,
         output_format_arrow_compression_method = 'none',
         output_format_arrow_low_cardinality_as_dictionary = 1,
         engine_file_truncate_on_insert = 1
"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream')"

echo "--- data (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY arr"

# An independent Arrow implementation must be able to decode what the native writer produced.
echo "--- natively written file read by pyarrow ---"
python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    table = pa.ipc.open_stream(source).read_all()

for field in table.schema:
    print(field.name, field.type, sep="\t")
for row in table.to_pylist():
    print(row)
PY

# The same data produced by pyarrow: the native reader must return exactly the rows printed above.
python3 - "${PYARROW_FILE}" <<'PY'
import sys
import pyarrow as pa

n = 5
schema = pa.schema([
    pa.field("arr", pa.list_(pa.field("item", pa.uint64(), nullable=False)), nullable=False),
    pa.field("arr_nullable", pa.list_(pa.field("item", pa.uint32(), nullable=True)), nullable=False),
    pa.field("tup", pa.struct([
        pa.field("1", pa.uint64(), nullable=False),
        pa.field("2", pa.string(), nullable=False),
    ]), nullable=False),
    pa.field("m", pa.map_(pa.string(), pa.field("value", pa.uint64(), nullable=False)), nullable=False),
    pa.field("lc", pa.dictionary(pa.int32(), pa.string()), nullable=False),
    pa.field("lc_null", pa.dictionary(pa.int32(), pa.string()), nullable=True),
    pa.field("nested_arr", pa.list_(pa.field(
        "item", pa.list_(pa.field("item", pa.uint8(), nullable=False)), nullable=False)), nullable=False),
])

columns = [
    pa.array([list(range(i)) for i in range(n)], type=schema.field("arr").type),
    pa.array([[None if x % 2 == 0 else x for x in range(i)] for i in range(n)],
             type=schema.field("arr_nullable").type),
    pa.array([{"1": i, "2": str(i)} for i in range(n)], type=schema.field("tup").type),
    pa.array([[(str(i), i), ("k", i * 2)] for i in range(n)], type=schema.field("m").type),
    pa.array([str(i % 3) for i in range(n)], type=schema.field("lc").type),
    pa.array([None if i % 2 == 0 else str(i) for i in range(n)], type=schema.field("lc_null").type),
    pa.array([[[1, 2], [3]] for _ in range(n)], type=schema.field("nested_arr").type),
]

batch = pa.record_batch(columns, schema=schema)
with pa.OSFile(sys.argv[1], "wb") as sink:
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
PY

echo "--- schema (native reader, pyarrow-produced fixture) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${PYARROW_FILE}', 'ArrowStream')"

echo "--- data (native reader, pyarrow-produced fixture) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${PYARROW_FILE}', 'ArrowStream') ORDER BY arr"

rm -f "${DATA_FILE}" "${PYARROW_FILE}"

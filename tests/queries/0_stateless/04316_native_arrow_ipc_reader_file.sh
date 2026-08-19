#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native ClickHouse Arrow IPC reader on the file format (Arrow), which uses the footer for
# random access to record batches and dictionary batches.
#
# The same logical data is exercised twice, so that a bug shared by the native writer and the native
# reader cannot hide: once as a native round-trip, and once from a file produced by `pyarrow`, an
# independent Arrow implementation, which writes multiple record batches plus a dictionary batch of
# its own layout. Both must read back identically, including the metadata-only count path.

DATA_FILE="${CLICKHOUSE_TMP}/04316_file.arrow"
PYARROW_FILE="${CLICKHOUSE_TMP}/04316_file_pyarrow.arrow"

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}', 'Arrow')
SELECT
    toInt32(number) AS i,
    toString(number) AS s,
    range(number % 4) AS arr,
    toLowCardinality(toString(number % 3)) AS lc
FROM numbers(10)
SETTINGS output_format_arrow_string_as_string = 1,
         output_format_arrow_compression_method = 'none',
         output_format_arrow_low_cardinality_as_dictionary = 1,
         max_block_size = 3
"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'Arrow')"

echo "--- data (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'Arrow') ORDER BY i"

echo "--- count only (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${DATA_FILE}', 'Arrow')"

# An independent Arrow implementation must be able to decode what the native writer produced.
echo "--- natively written file read by pyarrow ---"
python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    reader = pa.ipc.open_file(source)
    table = reader.read_all()

print("record batches:", reader.num_record_batches > 1)
for field in table.schema:
    print(field.name, field.type, sep="\t")
for row in table.to_pylist():
    print(row)
PY

# The same data written as an Arrow file by pyarrow, in several record batches with a dictionary batch
# of its own: the native reader must traverse that footer and return exactly the same rows.
python3 - "${PYARROW_FILE}" <<'PY'
import sys
import pyarrow as pa

n = 10
schema = pa.schema([
    pa.field("i", pa.int32(), nullable=False),
    pa.field("s", pa.string(), nullable=False),
    pa.field("arr", pa.list_(pa.field("item", pa.uint8(), nullable=False)), nullable=False),
    pa.field("lc", pa.dictionary(pa.int32(), pa.string()), nullable=False),
])

# One dictionary shared by every batch: the Arrow file format forbids dictionary replacement.
dictionary = pa.array([str(v) for v in range(3)], type=pa.string())

with pa.OSFile(sys.argv[1], "wb") as sink:
    with pa.ipc.new_file(sink, schema) as writer:
        for start in range(0, n, 3):
            rows = range(start, min(start + 3, n))
            batch = pa.record_batch([
                pa.array([i for i in rows], type=pa.int32()),
                pa.array([str(i) for i in rows], type=pa.string()),
                pa.array([list(range(i % 4)) for i in rows], type=schema.field("arr").type),
                pa.DictionaryArray.from_arrays(
                    pa.array([i % 3 for i in rows], type=pa.int32()), dictionary),
            ], schema=schema)
            writer.write_batch(batch)
PY

echo "--- schema (native reader, pyarrow-produced file) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${PYARROW_FILE}', 'Arrow')"

echo "--- data (native reader, pyarrow-produced file) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${PYARROW_FILE}', 'Arrow') ORDER BY i"

echo "--- count only (native reader, pyarrow-produced file) ---"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${PYARROW_FILE}', 'Arrow')"

rm -f "${DATA_FILE}" "${PYARROW_FILE}"

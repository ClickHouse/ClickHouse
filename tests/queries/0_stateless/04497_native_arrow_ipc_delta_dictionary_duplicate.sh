#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the native Arrow IPC reader: a delta dictionary batch appends to the existing
# dictionary. Each incoming batch is checked for internal duplicates, but a unique base dictionary plus a
# unique delta can still produce a duplicate merged value, which would violate the `LowCardinality`
# dictionary uniqueness invariant the non-delta path enforces. The merged dictionary must be re-validated
# and the duplicate rejected. Here a base dictionary ['AAA', 'BBB'] is followed by a delta whose only value
# is patched from 'CCC' to 'AAA', so the merged dictionary repeats 'AAA'.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"
trap 'rm -f "$DATA_FILE"' EXIT

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

schema = pa.schema([pa.field('d', pa.dictionary(pa.int32(), pa.string()))])

# Batch 1 establishes the base dictionary ['AAA', 'BBB'] for dictionary id 0.
b1 = pa.RecordBatch.from_arrays([pa.array(['AAA', 'BBB', 'AAA']).dictionary_encode()], schema=schema)
# Batch 2's dictionary is the base extended with 'CCC'; with deltas enabled the writer emits a delta batch
# carrying only ['CCC'] for the same id, instead of replacing the whole dictionary.
arr2 = pa.DictionaryArray.from_arrays(pa.array([2, 2, 0], type=pa.int32()), pa.array(['AAA', 'BBB', 'CCC']))
b2 = pa.RecordBatch.from_arrays([arr2], schema=schema)

sink = pa.BufferOutputStream()
w = ipc.new_stream(sink, schema, options=ipc.IpcWriteOptions(emit_dictionary_deltas=True))
w.write_batch(b1)
w.write_batch(b2)
w.close()
data = bytearray(sink.getvalue().to_pybytes())

# Confirm a delta was actually emitted: the base dictionary bytes ('AAABBB') must appear exactly once. A
# replacement (non-delta) batch would rewrite the whole dictionary and repeat them, which would instead be
# caught by the per-batch uniqueness check rather than the merged-dictionary check this test targets.
assert data.count(b'AAABBB') == 1, "expected a delta dictionary batch (base dictionary should not be repeated)"

# Patch the delta value 'CCC' -> 'AAA' so the merged dictionary becomes ['AAA', 'BBB', 'AAA'] (duplicate 'AAA').
i = data.find(b'CCC')
assert i >= 0, "delta value 'CCC' not found (no delta dictionary batch was emitted)"
data[i:i + 3] = b'AAA'

open(path, 'wb').write(bytes(data))
EOF

echo "--- delta dictionary that duplicates a base value is rejected ---"
${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
    --query "SELECT d FROM file('${DATA_FILE}', 'ArrowStream') FORMAT Null" 2>&1 \
    | grep -oF 'duplicate values' | head -1 || echo 'FAIL: expected the duplicate merged dictionary value to be rejected'

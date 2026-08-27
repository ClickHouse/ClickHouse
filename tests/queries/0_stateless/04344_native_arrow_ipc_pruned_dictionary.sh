#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A subset read with the native Arrow IPC reader must skip the `DictionaryBatch` bodies of dictionaries
# referenced only by unrequested columns, instead of decoding (and possibly failing or allocating) on them.
# Column `b`'s dictionary values get their string offsets corrupted, so decoding that dictionary throws;
# reading only `a` must still succeed (the dictionary of `b` is unreachable and skipped).

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.Arrow" "${DATA_FILE}.ArrowStream"' EXIT

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

base = sys.argv[1]
indices = pa.array([0, 1, 2, 0, 1], type=pa.int32())
dictionary = pa.array(["x", "yy", "zzz"])
batch = pa.record_batch(
    [pa.array([10, 20, 30, 40, 50], type=pa.int32()), pa.DictionaryArray.from_arrays(indices, dictionary)],
    names=["a", "b"],
)
# The dictionary values' offsets buffer [0, 1, 3, 6] is a distinctive byte pattern; corrupt offsets[1]
# to -1 so decoding the dictionary body fails, while a pruned read never touches it.
pattern = b''.join(v.to_bytes(4, 'little') for v in (0, 1, 3, 6))
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    sink = pa.BufferOutputStream()
    with opener(sink, batch.schema) as writer:
        writer.write_batch(batch)
    data = bytearray(sink.getvalue().to_pybytes())
    i = data.find(pattern)
    assert i >= 0, "dictionary offsets pattern not found"
    assert data.find(pattern, i + 1) == -1, "dictionary offsets pattern is not unique"
    data[i + 4:i + 8] = b'\xff\xff\xff\xff'
    open(f"{base}.{fmt}", 'wb').write(bytes(data))
PY

for FMT in Arrow ArrowStream; do
    echo "--- ${FMT}: SELECT a (b unrequested) -> b's corrupt dictionary is skipped ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT a FROM file('${DATA_FILE}.${FMT}', '${FMT}', 'a Int32')"
    echo "--- ${FMT}: SELECT b (requested) -> corrupt dictionary rejected ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT b FROM file('${DATA_FILE}.${FMT}', '${FMT}', 'a Int32, b LowCardinality(String)')" 2>&1 | grep -o "INCORRECT_DATA" | head -1

    rm -f "${DATA_FILE}.${FMT}"
done

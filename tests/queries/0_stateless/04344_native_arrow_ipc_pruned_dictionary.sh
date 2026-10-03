#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A subset read with the native Arrow IPC reader must skip the `DictionaryBatch` bodies of dictionaries
# referenced only by unrequested columns, instead of decoding (and possibly failing or allocating) on them.
# The file has a plain column `a` and a dictionary-encoded column `b` whose dictionary contains a duplicate
# value, which the reader rejects when it decodes the dictionary. Reading only `a` must therefore succeed
# (the dictionary of `b` is unreachable and skipped); reading `b` must fail with `INCORRECT_DATA`. The file
# is built with pyarrow because ClickHouse's writer never emits a dictionary with duplicate values.

DATA_FILE="${CLICKHOUSE_TMP}/04344_pruned_dict"

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

base = sys.argv[1]
indices = pa.array([0, 1, 2, 0, 1], type=pa.int32())
dictionary = pa.array(["x", "x", "y"])  # duplicate "x": decoding this dictionary must be rejected
batch = pa.record_batch(
    [pa.array([10, 20, 30, 40, 50], type=pa.int32()), pa.DictionaryArray.from_arrays(indices, dictionary)],
    names=["a", "b"],
)
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, batch.schema) as writer:
            writer.write_batch(batch)
PY

for FMT in Arrow ArrowStream; do
    echo "--- ${FMT}: SELECT a (b unrequested) -> b's corrupt dictionary is skipped ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT a FROM file('${DATA_FILE}.${FMT}', '${FMT}', 'a Int32')
        SETTINGS input_format_arrow_use_native_reader = 1
    "
    echo "--- ${FMT}: SELECT b (requested) -> duplicate dictionary values rejected ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT b FROM file('${DATA_FILE}.${FMT}', '${FMT}', 'a Int32, b LowCardinality(String)')
        SETTINGS input_format_arrow_use_native_reader = 1
    " 2>&1 | grep -o "INCORRECT_DATA" | head -1

    rm -f "${DATA_FILE}.${FMT}"
done

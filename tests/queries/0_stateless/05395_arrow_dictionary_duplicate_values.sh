#!/usr/bin/env bash
# Tags: no-fasttest
# Arrow doesn't require the values of a dictionary to be unique. The native Arrow IPC reader must read a
# dictionary with repeated values (and repeated nulls) by resolving each index, both for a top-level column
# (read as `LowCardinality`) and for a dictionary nested in a list (read as a plain column).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.Arrow" "${DATA_FILE}.ArrowStream"' EXIT

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa

base = sys.argv[1]
s = pa.DictionaryArray.from_arrays(
    pa.array([0, 1, 2, 3, 4, 5, 2, 0], type=pa.int8()), pa.array(["x", "y", "x", None, None, "z"]))
n = pa.DictionaryArray.from_arrays(
    pa.array([0, 1, 2, 1, 0, 2, 1, 0], type=pa.int32()), pa.array([7, 7, 8], type=pa.int64()))
l = pa.ListArray.from_arrays(
    pa.array([0, 2, 2, 3, 4, 5, 6, 7, 8], type=pa.int32()),
    pa.DictionaryArray.from_arrays(pa.array([0, 1, 2, 1, 0, 2, 1, 0], type=pa.int16()), pa.array(["p", "p", "q"])))
batch = pa.record_batch([pa.array(range(1, 9), type=pa.int32()), s, n, l], names=["id", "s", "n", "l"])
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, batch.schema) as writer:
            writer.write_batch(batch)
EOF

for FMT in Arrow ArrowStream; do
    echo "--- ${FMT} ---"
    ${CLICKHOUSE_LOCAL} --query "
        DESC file('${DATA_FILE}.${FMT}', '${FMT}');
        SELECT * FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY id;
        SELECT s, count() FROM file('${DATA_FILE}.${FMT}', '${FMT}') GROUP BY s ORDER BY s;
        SELECT n, count() FROM file('${DATA_FILE}.${FMT}', '${FMT}') GROUP BY n ORDER BY n;"
done

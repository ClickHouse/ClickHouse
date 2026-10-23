#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Subset-of-columns reads with the native Arrow IPC reader must keep working when an *unrequested* column has a
# type with its own buffer layout (`ListView`, `LargeListView`, `RunEndEncoded`): the reader advances the
# node/buffer cursors past such a column. The column `b` is placed between two readable columns `a` and `c`, so
# an inexact skip would corrupt `c`. Requesting `b` reads the list views as `Array` and `RunEndEncoded` as its
# values. The files are built with pyarrow because ClickHouse cannot write these Arrow types.

DATA_FILE="${CLICKHOUSE_TMP}/04345_skip_unsupported"

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

base = sys.argv[1]
a = pa.array([10, 20, 30], type=pa.int32())
c = pa.array([100, 200, 300], type=pa.int64())
unsupported = {
    "list_view": pa.array([[1, 2], [3], []], type=pa.list_view(pa.int32())),
    "large_list_view": pa.array([[1, 2], [3], []], type=pa.large_list_view(pa.int32())),
    "run_end_encoded": pa.RunEndEncodedArray.from_arrays(pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])),
}
for name, b in unsupported.items():
    batch = pa.record_batch([a, b, c], names=["a", "b", "c"])
    for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
        with pa.OSFile(f"{base}.{name}.{fmt}", "wb") as sink:
            with opener(sink, batch.schema) as writer:
                writer.write_batch(batch)
PY

for NAME in list_view large_list_view run_end_encoded; do
    for FMT in Arrow ArrowStream; do
        echo "--- ${NAME} / ${FMT}: SELECT a, c (unsupported middle column 'b' skipped) ---"
        ${CLICKHOUSE_LOCAL} --query "SELECT a, c FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}', 'a Int32, c Int64')"

        if [ "${NAME}" = "run_end_encoded" ]; then
            echo "--- ${NAME} / ${FMT}: SELECT b (requested) -> read as its values ---"
        else
            echo "--- ${NAME} / ${FMT}: SELECT b (requested) -> read as Array ---"
        fi
        ${CLICKHOUSE_LOCAL} --query "SELECT b FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}')"

        rm -f "${DATA_FILE}.${NAME}.${FMT}"
    done
done

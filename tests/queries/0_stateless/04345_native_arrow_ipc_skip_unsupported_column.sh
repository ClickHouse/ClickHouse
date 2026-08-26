#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Subset-of-columns reads with the native Arrow IPC reader must keep working even when an *unrequested*
# column has a valid Arrow type the reader cannot decode (`ListView`, `LargeListView`, `RunEndEncoded`).
# The reader cannot materialize these types, but it knows their physical buffer layout, so it advances the
# node/buffer cursors past such an unrequested column instead of failing the whole query. The unsupported
# column `b` is placed between two readable columns `a` and `c`, so an inexact skip would corrupt `c`.
# Requesting the unsupported column itself must still fail with a clear error. The files are built with
# pyarrow because ClickHouse cannot write these Arrow types.
#
# This matches the Apache Arrow library reader, which also cannot convert these types to ClickHouse columns
# (it throws the same error when one is requested) and only succeeds on a subset read because it does not
# convert unrequested columns — so the native reader is verified to be exactly as complete here: each read
# must give the same result through both readers.

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
        echo "--- ${NAME} / ${FMT}: SELECT a, c (unsupported middle column 'b' skipped) — native == library ---"
        native=$(${CLICKHOUSE_LOCAL}  --query "SELECT a, c FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}', 'a Int32, c Int64') SETTINGS input_format_arrow_use_native_reader = 1")
        library=$(${CLICKHOUSE_LOCAL} --query "SELECT a, c FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}', 'a Int32, c Int64') SETTINGS input_format_arrow_use_native_reader = 0")
        if [ "$native" = "$library" ]; then echo "OK"; echo "$native"; else echo "MISMATCH"; fi

        echo "--- ${NAME} / ${FMT}: SELECT b (requested) -> both readers reject the type ---"
        for READER in 1 0; do
            ${CLICKHOUSE_LOCAL} --query "
                SELECT b FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}')
                SETTINGS input_format_arrow_use_native_reader = ${READER},
                         input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference = 0
            " 2>&1 | grep -o "UNKNOWN_TYPE" | head -1
        done

        rm -f "${DATA_FILE}.${NAME}.${FMT}"
    done
done

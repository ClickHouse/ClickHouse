#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A valid Arrow `Utf8`/`LargeUtf8` array with rows > 0 but every value empty has a zero-length data buffer,
# which the reader exposes with a null pointer. The native string decoder must not form `data_ptr + offset`
# in that case (undefined pointer arithmetic on null, tripping sanitizers) — it must insert the empty values
# without touching the data pointer. The result must match the Apache Arrow library reader, for top-level
# strings and strings nested in a list, in both the Arrow and ArrowStream formats. Built with pyarrow
# because the layout (a non-empty validity/offsets buffer with an empty data buffer) is what matters.

DATA_FILE="${CLICKHOUSE_TMP}/04348_empty_str"

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

base = sys.argv[1]
arrays = {
    "utf8": pa.array(["", "", ""], type=pa.utf8()),
    "large_utf8": pa.array(["", "", ""], type=pa.large_utf8()),
    "list_utf8": pa.array([["", ""], [""], []], type=pa.list_(pa.utf8())),
}
for name, arr in arrays.items():
    batch = pa.record_batch([arr], names=["c"])
    for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
        with pa.OSFile(f"{base}.{name}.{fmt}", "wb") as sink:
            with opener(sink, batch.schema) as writer:
                writer.write_batch(batch)
PY

for NAME in utf8 large_utf8 list_utf8; do
    for FMT in Arrow ArrowStream; do
        native=$(${CLICKHOUSE_LOCAL}  --query "SELECT c FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 1")
        library=$(${CLICKHOUSE_LOCAL} --query "SELECT c FROM file('${DATA_FILE}.${NAME}.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 0")
        if [ "$native" = "$library" ]; then echo "OK native==library | ${NAME} ${FMT}"; echo "$native"; else echo "MISMATCH | ${NAME} ${FMT}"; fi
        rm -f "${DATA_FILE}.${NAME}.${FMT}"
    done
done

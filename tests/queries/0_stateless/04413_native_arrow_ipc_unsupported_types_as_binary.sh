#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A type the native Arrow IPC writer has no first-class Arrow mapping for (here `BFloat16`) is written as
# an Arrow `Binary` column when `output_format_arrow_unsupported_types_as_binary = 1` (the default); with the
# setting disabled the native writer rejects it. The binary column reads back as `String` (raw value bytes).

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"
GEN="SELECT number::BFloat16 AS b FROM numbers(4)"

write() {
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') ${GEN}
        SETTINGS output_format_arrow_compression_method = 'none',
                 engine_file_truncate_on_insert = 1"
}
read_back() { ${CLICKHOUSE_LOCAL} --query "SELECT toTypeName(b), hex(b) FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY b"; }

echo "--- native writer: BFloat16 -> Arrow binary, read back ---"
write
read_back

# Cross-check with `pyarrow`, an independent Arrow implementation: the field must really be an Arrow
# `Binary` column carrying the raw `BFloat16` bytes, so that a bug shared by the native writer and the
# native reader cannot pass unnoticed.
echo "--- pyarrow reader: field type and raw payload bytes ---"
python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    table = pa.ipc.open_stream(source).read_all()

field = table.schema.field("b")
print(field.name, field.type, sep="\t")
# Print the storage bytes, so that the output does not depend on how a given `pyarrow` release boxes
# the values into Python objects.
for value in table.column("b").cast(pa.binary()).to_pylist():
    print(value.hex().upper())
PY

echo "--- with output_format_arrow_unsupported_types_as_binary = 0 the native writer rejects it ---"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') ${GEN}
    SETTINGS output_format_arrow_unsupported_types_as_binary = 0,
             engine_file_truncate_on_insert = 1" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -1

rm -f "${DATA_FILE}"

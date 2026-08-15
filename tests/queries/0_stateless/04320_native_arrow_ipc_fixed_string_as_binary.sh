#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: with output_format_arrow_fixed_string_as_fixed_byte_array=0 the schema advertises a
# variable-width Utf8/Binary, so the native writer must emit the offsets+data buffers (not a single
# fixed-width buffer). Otherwise it writes invalid Arrow that a reader misinterprets. Verify the
# native-written data reads back identically with the native reader and with `pyarrow`, an independent
# Arrow implementation: an offsets+data layout that the native reader happens to tolerate must not pass.

DATA_FILE="${CLICKHOUSE_TMP}/04320_fs_binary.arrows"

for as_string in 0 1; do
    ${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream')
    SELECT toFixedString(repeat('x', number), 5) AS fs FROM numbers(5)
    SETTINGS output_format_arrow_fixed_string_as_fixed_byte_array = 0,
             output_format_arrow_string_as_string = ${as_string},
             engine_file_truncate_on_insert = 1"

    echo "string_as_string=${as_string} native reader:"
    ${CLICKHOUSE_LOCAL} --query "SELECT hex(fs) FROM file('${DATA_FILE}', 'ArrowStream')"

    echo "string_as_string=${as_string} pyarrow reader:"
    python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    table = pa.ipc.open_stream(source).read_all()

field = table.schema.field("fs")
print(field.name, field.type, sep="\t")
# Print the storage bytes, so that the output does not depend on how a given `pyarrow` release boxes
# the values into Python objects.
for value in table.column("fs").cast(pa.binary()).to_pylist():
    print(value.hex().upper())
PY
done

rm -f "${DATA_FILE}"

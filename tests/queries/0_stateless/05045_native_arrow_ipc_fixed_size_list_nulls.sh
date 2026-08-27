#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format and pyarrow are not available in fasttest builds

# A null slot of an Arrow fixed_size_list still spans `list_size` positions in the child array, and
# Arrow leaves the values at those positions unspecified. ClickHouse's Array cannot be Nullable, so
# the slot itself cannot stay NULL — but it must read back as an EMPTY array (matching how a null
# slot of the variable-size List reads), never as whatever bytes the writer happened to leave in the
# child buffer. The file is built with pyarrow because the ClickHouse writer never produces a
# fixed_size_list with null slots.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.Arrow" "${DATA_FILE}.ArrowStream"' EXIT

python3 - "$DATA_FILE" <<'PY'
import sys

import pyarrow as pa

base = sys.argv[1]
# Build the fixed_size_list from raw buffers so live child values (the 900s) sit under the null
# slots — exactly the layout where a reader that ignores the list's validity leaks them as data.
child = pa.array([1, 2, None, 4, 900, 901, 5, 6, 902, 903, 7, 8], type=pa.int32())
fsl_type = pa.list_(pa.field('item', pa.int32()), 2)
validity = pa.py_buffer(bytes([0b00101011]))  # slots 2 and 4 are null
fsl = pa.Array.from_buffers(fsl_type, 6, [validity], children=[child])
assert fsl.null_count == 2
assert fsl.to_pylist() == [[1, 2], [None, 4], None, [5, 6], None, [7, 8]]
table = pa.table({'k': pa.array(range(6), type=pa.int64()), 'v': fsl})
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, table.schema) as writer:
            writer.write_table(table)
PY

for FMT in Arrow ArrowStream
do
    echo "--- ${FMT}: null slots read as empty arrays, valid slots keep their values ---"
    ${CLICKHOUSE_LOCAL} -q "SELECT k, v FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY k"
done

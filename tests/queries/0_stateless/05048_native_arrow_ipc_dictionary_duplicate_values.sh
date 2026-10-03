#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format and pyarrow are not available in fasttest builds

# Arrow allows a dictionary to contain duplicate values, including duplicate nulls (the official Arrow
# gold files do). Rows referencing any copy must decode to the same value: a top-level dictionary
# dedups into LowCardinality, a nested one materializes to a plain column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.Arrow" "${DATA_FILE}.ArrowStream"' EXIT

python3 - "$DATA_FILE" <<'PY'
import sys

import pyarrow as pa

base = sys.argv[1]
# 'a' appears at dictionary positions 0 and 3, null at positions 1 and 4.
dict_vals = pa.array(['a', None, 'b', 'a', None], type=pa.string())
top = pa.DictionaryArray.from_arrays(pa.array([0, 3, 2, 1, 4, 2], type=pa.int8()), dict_vals)
inner = pa.DictionaryArray.from_arrays(pa.array([0, 3, 1, 2], type=pa.int8()), dict_vals)
nested = pa.ListArray.from_arrays(pa.array([0, 2, 2, 4, 4, 4, 4], type=pa.int32()), inner)
table = pa.table({'k': pa.array(range(6), type=pa.int64()), 'd': top, 'ld': nested})
assert table.column('d').to_pylist() == ['a', 'a', 'b', None, None, 'b']
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, table.schema) as writer:
            writer.write_table(table)
PY

for FMT in Arrow ArrowStream
do
    echo "--- ${FMT}: duplicate dictionary entries decode to the same value ---"
    ${CLICKHOUSE_LOCAL} -q "SELECT k, d, toTypeName(d), ld FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY k"
done

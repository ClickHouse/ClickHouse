#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for accepting empty String/Binary columns whose offsets buffer
# is 0 bytes.  Apache Arrow Java < 19.0.0 (bundled with Apache Spark) emits a
# 0-byte offsets buffer when a String/Binary child column has zero elements
# (e.g. every map is empty, so the key child has length=0).  Every other Arrow
# implementation (arrow-cpp, pyarrow, arrow-rs) accepts this; ClickHouse must
# too.  The fix: when chunk.length() == 0, no offsets are ever accessed, so
# 0 bytes are required.
#
# Covers three shapes of empty String column:
#   (a) standalone String column, length=0
#   (b) Map(String, Int32) where all entries are empty -> key child length=0
#   (c) Array(String) where all arrays are empty -> string child length=0

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import io, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def write_arrow(tbl):
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return buf.getvalue()

def zero_byte_string_array():
    """Build a String array with length=0 and a 0-byte offsets buffer,
    as Apache Arrow Java < 19.0.0 produces for empty collections."""
    return pa.Array.from_buffers(
        pa.string(), 0,
        [None, pa.py_buffer(b""), pa.py_buffer(b"")]
    )

# (a) Standalone String column, 0 rows, 0-byte offsets buffer.
tbl_a = pa.table({"s": zero_byte_string_array()})
open(f"{out}/empty_string.arrow", "wb").write(write_arrow(tbl_a))

# (b) Map(String, Int32) with 2 rows of empty maps -> key child length=0.
offsets = pa.array([0, 0, 0], type=pa.int32())   # two empty maps
items   = pa.array([], type=pa.int32())
map_col = pa.MapArray.from_arrays(offsets, zero_byte_string_array(), items)
tbl_b = pa.table({"id": pa.array([1, 2], type=pa.int32()), "m": map_col})
open(f"{out}/empty_map_keys.arrow", "wb").write(write_arrow(tbl_b))

# (c) Array(String) with 2 rows of empty arrays -> string child length=0.
list_offsets = pa.array([0, 0, 0], type=pa.int32())
list_col = pa.ListArray.from_arrays(list_offsets, zero_byte_string_array())
tbl_c = pa.table({"id": pa.array([1, 2], type=pa.int32()), "a": list_col})
open(f"{out}/empty_list_strings.arrow", "wb").write(write_arrow(tbl_c))
PYEOF

# (a) Should return 0 rows with no exception.
$CLICKHOUSE_LOCAL --query \
    "SELECT count() FROM file('${TMP_DIR}/empty_string.arrow', Arrow)"

# (b) Should return the two empty-map rows.
$CLICKHOUSE_LOCAL --query \
    "SELECT id, m FROM file('${TMP_DIR}/empty_map_keys.arrow', Arrow) ORDER BY id"

# (c) Should return the two empty-array rows.
$CLICKHOUSE_LOCAL --query \
    "SELECT id, a FROM file('${TMP_DIR}/empty_list_strings.arrow', Arrow) ORDER BY id"

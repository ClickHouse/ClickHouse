#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# The Arrow spec leaves the contents of unobservable slot ranges undefined, and Array/Map cannot be
# inside Nullable in ClickHouse, so a NULL list/map slot becomes a visible row when its null map is
# dropped. It decodes as the type default — the empty array/map — the same way the native Parquet
# reader materializes a null list slot. The rows below hide well-formed values in those ranges and read
# them with a target that has to parse the child text: the read must return the empty range for the
# NULL slot and the parsed values for the visible ones, not fail on a substitute value (the way an
# empty string would fail an Int32 parse).
#
#   list_string_under_null:        list<utf8> whose NULL slot spans '123'; the visible slot holds '456'
#   large_list_string_under_null:  as above with large_list (64-bit offsets)
#   fixed_size_list_string_under_null: fixed_size_list<utf8, 1>, same rows
#   map_string_under_null:         map<utf8, utf8> whose NULL slot spans {'k1': '123'}
#   map_key_string_under_null:     as above with numeric-text keys, so the key cast is the parsing one
#   list_dict_string_under_null:   list<dictionary<utf8>> whose NULL slot references index 0 ('123')
#   struct_list_string_under_null: struct<a: list<utf8>>, the list row is hidden by the NULL struct row
#                                  (composed invisibility, not the list's own validity)
#   struct_time32_under_null:      struct<t: time32[s]>, the NULL row's slot holds 12345 seconds; the
#                                  time32 leaf decodes invisible rows as 00:00:00 like every other leaf

python3 - "$TMP_DIR" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def write(name, arr, field_name):
    tbl = pa.table([arr], schema=pa.schema([pa.field(field_name, arr.type, nullable=True)]))
    with ipc.new_file(f"{out}/{name}.arrow", tbl.schema) as w:
        w.write_table(tbl)

def null_first_slot(arr):
    """The same array with slot 0 nulled at the top level only: the offsets keep spanning its range."""
    buffers = arr.buffers()
    return pa.Array.from_buffers(
        arr.type, len(arr), [pa.py_buffer(bytes([0b10]))] + buffers[1:2], children=[arr.values])

values = pa.array(["123", "456"], type=pa.utf8())
offsets = pa.array([0, 1, 2], type=pa.int32())
write("list_string_under_null", null_first_slot(pa.ListArray.from_arrays(offsets, values)), "a")

large_offsets = pa.array([0, 1, 2], type=pa.int64())
write("large_list_string_under_null", null_first_slot(pa.LargeListArray.from_arrays(large_offsets, values)), "a")

fsl = pa.FixedSizeListArray.from_arrays(values, 1)
write("fixed_size_list_string_under_null",
      pa.Array.from_buffers(fsl.type, 2, [pa.py_buffer(bytes([0b10]))], children=[values]), "a")

def null_slot_map(name, keys, vals):
    entries = pa.StructArray.from_arrays(
        [keys, vals],
        fields=[pa.field("key", pa.utf8(), nullable=False), pa.field("value", pa.utf8(), nullable=False)])
    map_arr = pa.Array.from_buffers(
        pa.map_(pa.utf8(), pa.utf8()), 2,
        [pa.py_buffer(bytes([0b10])), offsets.buffers()[1]], children=[entries])
    write(name, map_arr, "m")

null_slot_map("map_string_under_null", pa.array(["k1", "k2"], type=pa.utf8()), values)
null_slot_map("map_key_string_under_null", values, pa.array(["v1", "v2"], type=pa.utf8()))

dict_values = pa.array(["123", "456"], type=pa.utf8())
dict_child = pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int32()), dict_values)
dict_list = pa.ListArray.from_arrays(offsets, dict_child)
write("list_dict_string_under_null",
      pa.Array.from_buffers(dict_list.type, 2, [pa.py_buffer(bytes([0b10])), dict_list.buffers()[1]],
                            children=[dict_child]), "a")

inner_list = pa.ListArray.from_arrays(offsets, values)
struct_list = pa.Array.from_buffers(
    pa.struct([pa.field("a", inner_list.type, nullable=False)]), 2,
    [pa.py_buffer(bytes([0b10]))], children=[inner_list])
write("struct_list_string_under_null", struct_list, "s")

t32 = pa.Array.from_buffers(
    pa.time32("s"), 2, [None, pa.py_buffer((12345).to_bytes(4, 'little') + (3661).to_bytes(4, 'little'))])
struct_t32 = pa.Array.from_buffers(
    pa.struct([pa.field("t", pa.time32("s"), nullable=False)]), 2,
    [pa.py_buffer(bytes([0b10]))], children=[t32])
write("struct_time32_under_null", struct_t32, "s")
PYEOF

for f in list_string_under_null large_list_string_under_null fixed_size_list_string_under_null list_dict_string_under_null; do
    echo "=== $f as Array(Int32)"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/$f.arrow', 'Arrow', 'a Array(Int32)')"
done
echo "=== map_string_under_null as Map(String, Int32)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/map_string_under_null.arrow', 'Arrow', 'm Map(String, Int32)')"
echo "=== map_key_string_under_null as Map(Int32, String)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/map_key_string_under_null.arrow', 'Arrow', 'm Map(Int32, String)')"
echo "=== struct_list_string_under_null as Tuple(a Array(Int32))"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/struct_list_string_under_null.arrow', 'Arrow', 's Tuple(a Array(Int32))')"
echo "=== struct_time32_under_null as Tuple(t Time64(0))"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/struct_time32_under_null.arrow', 'Arrow', 's Tuple(t Time64(0))')"

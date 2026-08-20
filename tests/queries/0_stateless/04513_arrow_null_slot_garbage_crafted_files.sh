#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Arrow format is not available in fasttest builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# The Arrow spec leaves the value bytes of unobservable slots undefined: slots that are null at any
# ancestor level (a struct's nulls do not have to be repeated in its children's validity), slot
# ranges no list offset references, and union child slots not selected by their row's type id. These
# files (generated with pyarrow) hide invalid values — an out-of-range date32 day number or an
# out-of-bounds dictionary index — where only that composed visibility reveals they can never be
# observed. Each file pairs the hidden garbage with at least one valid, observable row.
#
#   struct_date32_garbage_under_null:            struct<d: date32>, garbage in the child of the NULL row
#   struct_dict_garbage_index_under_null:        struct<d: dictionary>, out-of-bounds index under NULL
#   struct_dict_default_garbage_index_under_null: as above, but the child field is non-nullable and the
#                                                dictionary holds its default value at index 0 — the
#                                                default-valued key fallback
#   struct_struct_date32_garbage_under_null:     struct<inner: struct<d>>, two levels of composition
#   struct_list_date32_garbage_under_null:       struct<l: list<date32>>, the NULL row's valid list slot
#                                                spans a range holding the garbage
#   struct_large_list_date32_garbage_under_null: as above with large_list
#   struct_fixed_size_list_date32_garbage_under_null: as above with fixed_size_list
#   struct_map_date32_garbage_under_null:        struct<m: map<string, date32>>, garbage map value
#   struct_string_view_garbage_view_under_null:  struct<v: string_view>, the NULL row's 16-byte view
#                                                struct holds garbage (length -1); the child's own
#                                                validity marks the slot valid
#   struct_json_garbage_bytes_under_null:        struct<j: string> read as Nullable(Tuple(j JSON));
#                                                the NULL row's bytes are not valid JSON
#   struct_ipv6_binary_garbage_under_null:       struct<v: binary> read as Nullable(Tuple(v IPv6));
#                                                the NULL row's value is not 16 bytes, which must not
#                                                force the whole column down the text-parsed String
#                                                fallback (that would corrupt the visible rows too)
#   struct_int128_binary_garbage_under_null:     as above for Nullable(Tuple(n Int128))
#   struct_array_ipv6_binary_garbage_under_null: struct<a: list<binary>> read as
#                                                Nullable(Tuple(a Array(IPv6))); the NULL row's list
#                                                element is not 16 bytes (the width sniff must see the
#                                                invisibility through the list offsets)
#
# A NULL list/map slot whose offsets span a non-empty range is spec-legal too: the offsets stay
# monotonic and the range's bytes are undefined. Array/Map cannot be inside Nullable in ClickHouse, so
# the slot's own null map is dropped and the row decodes as the type default — the empty array/map —
# the same way the native Parquet reader materializes a null list slot.
#   list_date32_garbage_in_null_slot_range
#   fixed_size_list_date32_garbage_in_null_slot_range
#   map_date32_garbage_in_null_slot_range
#
# For unions the undefined bytes come from two more sources besides validity: child slots not selected
# by their row's type id (sparse layout, no nulls involved at all), and — under a NULL ancestor row —
# the union's own type-id/offset bytes:
#   sparse_union_date32_garbage_in_unselected:       non-selected sparse slots hold garbage
#   sparse_union_dict_garbage_index_in_unselected:   a dictionary child's non-selected slot holds an
#                                                    out-of-bounds index
#   struct_sparse_union_date32_garbage_under_null:   selected slot's garbage hidden by a NULL struct row
#   struct_dense_union_date32_garbage_under_null:    same for the dense layout (offsets-remapped)
#   struct_union_garbage_type_id_under_null:         the type-id byte itself is garbage (99) under NULL
#   struct_dense_union_garbage_offset_under_null:    the dense offset itself is garbage (999) under NULL

SELF_DESCRIBING_FILES="struct_date32_garbage_under_null \
    struct_dict_garbage_index_under_null \
    struct_dict_default_garbage_index_under_null \
    struct_struct_date32_garbage_under_null \
    struct_list_date32_garbage_under_null \
    struct_large_list_date32_garbage_under_null \
    struct_fixed_size_list_date32_garbage_under_null \
    struct_map_date32_garbage_under_null \
    struct_string_view_garbage_view_under_null \
    list_date32_garbage_in_null_slot_range \
    fixed_size_list_date32_garbage_in_null_slot_range \
    map_date32_garbage_in_null_slot_range \
    sparse_union_date32_garbage_in_unselected \
    sparse_union_dict_garbage_index_in_unselected \
    struct_sparse_union_date32_garbage_under_null \
    struct_dense_union_date32_garbage_under_null \
    struct_union_garbage_type_id_under_null \
    struct_dense_union_garbage_offset_under_null"

for f in $SELF_DESCRIBING_FILES; do
    echo "=== $f"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/$f.arrow', 'Arrow')
        SETTINGS allow_experimental_nullable_tuple_type = 1"
done

# The raw-byte targets need an explicit structure: `binary` carries no type of its own.
echo "=== struct_json_garbage_bytes_under_null"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_json_garbage_bytes_under_null.arrow', 'Arrow', 's Nullable(Tuple(j JSON))')
    SETTINGS allow_experimental_nullable_tuple_type = 1, enable_json_type = 1"
echo "=== struct_ipv6_binary_garbage_under_null"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_ipv6_binary_garbage_under_null.arrow', 'Arrow', 's Nullable(Tuple(v IPv6))')
    SETTINGS allow_experimental_nullable_tuple_type = 1"
echo "=== struct_int128_binary_garbage_under_null"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_int128_binary_garbage_under_null.arrow', 'Arrow', 's Nullable(Tuple(n Int128))')
    SETTINGS allow_experimental_nullable_tuple_type = 1"
echo "=== struct_array_ipv6_binary_garbage_under_null"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_array_ipv6_binary_garbage_under_null.arrow', 'Arrow', 's Nullable(Tuple(a Array(IPv6)))')
    SETTINGS allow_experimental_nullable_tuple_type = 1"

# Without the nullable-tuple setting the struct is read as a plain Tuple: its null map is dropped and
# the NULL row becomes a visible one, which must show type defaults, not the hidden bytes. The numeric
# type hint additionally selects the raw date32 read that skips the range check.
echo "=== struct_date32_garbage_under_null as Tuple(d Int32)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_date32_garbage_under_null.arrow', 'Arrow', 's Tuple(d Int32)')"
echo "=== struct_date32_garbage_under_null as Tuple(d Date32)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_date32_garbage_under_null.arrow', 'Arrow', 's Tuple(d Date32)')"
echo "=== struct_ipv6_binary_garbage_under_null as Tuple(v IPv6)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_ipv6_binary_garbage_under_null.arrow', 'Arrow', 's Tuple(v IPv6)')"
echo "=== struct_int128_binary_garbage_under_null as Tuple(n Int128)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_int128_binary_garbage_under_null.arrow', 'Arrow', 's Tuple(n Int128)')"

# The `fixed_size_binary` leaves the ClickHouse writer uses for UUID, IPv6 and the 128/256-bit
# integers. Their raw bytes are reinterpreted verbatim, so the hidden bytes of a NULL struct row would
# surface as a value of the target type; and the self-describing Arrow `uuid` extension type decodes
# straight into UUID, with no later rewrite that could default them. Crafted here instead of shipping
# more files: `Array.from_buffers` puts the garbage under the struct validity in a few readable lines.
python3 - "$TMP_DIR" <<'PYEOF'
import io, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def write(name, arr):
    tbl = pa.table([arr], schema=pa.schema([pa.field("s", arr.type, nullable=True)]))
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    open(f"{out}/{name}.arrow", "wb").write(buf.getvalue())

def struct_with_garbage_under_null(name, width, storage_type):
    """A struct whose row 0 is NULL while its non-nullable fixed_size_binary child marks every slot
    valid: only the struct's validity says the garbage of row 0 can never be observed."""
    child = pa.Array.from_buffers(
        pa.binary(width), 2, [None, pa.py_buffer(bytes([0xAA]) * width + bytes(range(width)))])
    if storage_type != pa.binary(width):
        child = pa.ExtensionArray.from_storage(storage_type, child)
    struct = pa.Array.from_buffers(
        pa.struct([pa.field("b", storage_type, nullable=False)]), 2,
        [pa.py_buffer(bytes([0b10]))], children=[child])
    write(name, struct)

struct_with_garbage_under_null("struct_fixed_binary_garbage_under_null", 16, pa.binary(16))
struct_with_garbage_under_null("struct_fixed_binary32_garbage_under_null", 32, pa.binary(32))
struct_with_garbage_under_null("struct_uuid_garbage_under_null", 16, pa.uuid())
PYEOF

for type_name in UUID IPv6 Int128 UInt128; do
    echo "=== struct_fixed_binary_garbage_under_null as Tuple(b $type_name)"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/struct_fixed_binary_garbage_under_null.arrow', 'Arrow', 's Tuple(b $type_name)')"
done
for type_name in Int256 UInt256; do
    echo "=== struct_fixed_binary32_garbage_under_null as Tuple(b $type_name)"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/struct_fixed_binary32_garbage_under_null.arrow', 'Arrow', 's Tuple(b $type_name)')"
done

echo "=== struct_uuid_garbage_under_null"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/struct_uuid_garbage_under_null.arrow', 'Arrow')
    SETTINGS allow_experimental_nullable_tuple_type = 1"
# The same file as a plain Tuple: the struct null map is dropped, so row 0 becomes visible and the
# self-describing UUID leaf must show the default instead of the hidden bytes.
echo "=== struct_uuid_garbage_under_null as a plain Tuple"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/struct_uuid_garbage_under_null.arrow', 'Arrow')"

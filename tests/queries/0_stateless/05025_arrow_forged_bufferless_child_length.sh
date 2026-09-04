#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A buffer-less Arrow subtree (a `null`-typed field, or a struct/fixed-size-list tree of them) carries
# no buffers whose validated size could bound its declared FieldNode length, so its decoded size derives
# from that length alone. The native reader must therefore never let such a length drive an allocation:
# the child of a list is built for the rows the list references only, and every other position requires
# the length its parent implies BEFORE decoding the child.
#
# Each shape below has positive parent rows and a buffer-less child whose length is a distinctive value.
# For every aligned int64 in the message equal to that value, one forged variant patches it to 2^40:
# every variant must be either rejected as bad data or read successfully, without attempting a huge
# allocation. Where the position fixes the length, the pre-decode check must fire for at least one
# variant; a list child declaring more rows than referenced is legal and simply not materialized.
#
#   list_null:        list<null>, offsets [0, 3, 7] -> null child of length 7
#   large_list_null:  as above with 64-bit offsets
#   map_string_null:  map<utf8, null>, same offsets -> entries struct of length 7 whose `value`
#                     field is buffer-less; forging the entries length itself must hit the physical
#                     body bound (the entries struct is not buffer-less, its key is buffered)
#   fsl_null:         fixed_size_list<null, 2> with 3 rows -> null child of length 6
#   struct_null:      struct<a: null> with 5 rows -> null field of length 5
#   list_struct_null_int: list<struct<n: null, v: int32>>, the null field is declared before its
#                     buffered sibling, so only the parent-derived bounds (not the sibling's
#                     buffer-size check) can reject its forged length before the allocation

python3 - "$TMP_DIR" <<'PYEOF'
import io, struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
HUGE = 1 << 40

def write_and_forge(name, arr, field_name, child_len):
    tbl = pa.table([arr], schema=pa.schema([pa.field(field_name, arr.type, nullable=True)]))
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    data = bytearray(buf.getvalue())
    open(f"{out}/{name}.arrow", "wb").write(data)
    count = 0
    for i in range(0, len(data) - 8, 8):
        if struct.unpack_from("<q", data, i)[0] == child_len:
            patched = bytearray(data)
            patched[i:i + 8] = struct.pack("<q", HUGE)
            open(f"{out}/{name}_forged_{i}.arrow", "wb").write(patched)
            count += 1
    assert count > 0, f"{name}: no int64 equal to {child_len} found to forge"

def null_array(n):
    return pa.nulls(n, type=pa.null())

offsets32 = pa.array([0, 3, 7], type=pa.int32())

write_and_forge("list_null", pa.ListArray.from_arrays(offsets32, null_array(7)), "a", 7)

write_and_forge(
    "large_list_null",
    pa.LargeListArray.from_arrays(pa.array([0, 3, 7], type=pa.int64()), null_array(7)), "a", 7)

keys = pa.array(["k1", "k2", "k3", "k4", "k5", "k6", "k7"], type=pa.utf8())
entries = pa.StructArray.from_arrays(
    [keys, null_array(7)],
    fields=[pa.field("key", pa.utf8(), nullable=False), pa.field("value", pa.null(), nullable=True)])
map_arr = pa.Array.from_buffers(
    pa.map_(pa.utf8(), pa.null()), 2, [None, offsets32.buffers()[1]], children=[entries])
write_and_forge("map_string_null", map_arr, "m", 7)

write_and_forge(
    "fsl_null",
    pa.Array.from_buffers(pa.list_(pa.null(), 2), 3, [None], children=[null_array(6)]), "a", 6)

struct_null = pa.Array.from_buffers(
    pa.struct([pa.field("a", pa.null(), nullable=True)]), 5, [None], children=[null_array(5)])
write_and_forge("struct_null", struct_null, "s", 5)

inner = pa.StructArray.from_arrays(
    [null_array(7), pa.array([1, 2, 3, 4, 5, 6, 7], type=pa.int32())],
    fields=[pa.field("n", pa.null(), nullable=True), pa.field("v", pa.int32(), nullable=False)])
write_and_forge("list_struct_null_int", pa.ListArray.from_arrays(offsets32, inner), "a", 7)
PYEOF

read_query()
{
    echo "SELECT * FROM file('$1', 'Arrow', '$2')"
}

structure()
{
    case "$1" in
        list_null|large_list_null|fsl_null) echo "a Array(Nullable(Nothing))" ;;
        map_string_null)                    echo "m Map(String, Nullable(Nothing))" ;;
        struct_null)                        echo "s Tuple(a Nullable(Nothing))" ;;
        list_struct_null_int)               echo "a Array(Tuple(n Nullable(Nothing), v Int32))" ;;
    esac
}

# The check each shape's forged length must trip at least once: the exact expected length for a
# fixed-size-list child or a struct field (the struct shape can also trip the top-level batch-length
# check, when the struct's own node is the patched one), and the physical body bound for a forged buffered
# child of a map or list (the entries struct has a buffered key, the inner struct a buffered int32 field,
# so neither is buffer-less). The buffer-less child of a list may declare any length past the referenced
# rows, so the list shapes require no rejection.
guard_pattern()
{
    case "$1" in
        fsl_null)                              echo "fixed-size-list child declares" ;;
        struct_null)                           echo "declares .* rows, expected" ;;
        map_string_null|list_struct_null_int)  echo "more than the .* message body can hold" ;;
        *)                                     echo "" ;;
    esac
}

for name in list_null large_list_null map_string_null fsl_null struct_null list_struct_null_int; do
    echo "=== $name"
    $CLICKHOUSE_LOCAL -q "$(read_query "$TMP_DIR/$name.arrow" "$(structure "$name")")"

    oom=0
    guard=0
    for f in "$TMP_DIR/${name}"_forged_*.arrow; do
        err=$($CLICKHOUSE_LOCAL --max_memory_usage=1G \
            -q "$(read_query "$f" "$(structure "$name")") FORMAT Null" 2>&1)
        case "$err" in
            *CANNOT_ALLOCATE_MEMORY*|*"bad_alloc"*|*MEMORY_LIMIT_EXCEEDED*) oom=$((oom + 1)) ;;
        esac
        if [[ "$err" =~ $(guard_pattern "$name") ]]; then guard=$((guard + 1)); fi
    done
    echo "forged variants that drove an allocation: $oom"
    if [ -n "$(guard_pattern "$name")" ]; then
        [ "$guard" -ge 1 ] && echo "pre-decode length check active" || echo "pre-decode length check NOT triggered"
    fi
done

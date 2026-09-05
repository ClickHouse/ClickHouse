#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A buffer-less child (a `null` array, or a struct / fixed-size list of them) whose nodes declare no nulls
# is determined by its size alone, so the reader builds only the rows the visible slots reference. The
# range under a NULL list slot, the unreferenced head of a sliced list and the elements of NULL fixed-size
# list slots are never materialized, however large the offsets make them: a few hundred bytes of metadata
# can declare 2^31-1 hidden elements, and the read must not allocate for them. A visible slot spanning that
# many elements is an honest declaration of a huge value and is still bounded only by the memory limit.
#
#   list_null_hidden_slot:      list<null>, one NULL slot spanning 2^31-1 elements -> []
#   list_null_visible_slot:     the same slot visible: a list of 2^31-1 NULLs, rejected by the memory limit
#   list_null_hidden_head:      list<null> sliced to its last two elements (a huge unreferenced head)
#   large_list_null_hidden_slot: as list_null_hidden_slot with 64-bit offsets
#   list_null_mixed:            a visible slot of three elements followed by a NULL slot spanning the rest
#   fsl_null_hidden:            fixed_size_list<null, 2^20> with 2047 rows, only row 5 visible
#   list_struct_null_hidden:    list<struct<a: null, b: null>>, one NULL slot spanning 2^31-1 rows
#   list_struct_nulls:          list<struct<a: null>> whose struct declares a NULL row: the child carries a
#                               validity bitmap and decodes on the ordinary path (visible and hidden slots)
#   list_null_tail:             list<null> whose child declares one row more than the offsets reference —
#                               the untruncated tail of a sliced list — read as the referenced rows
#   list_struct_null_tail:      the same tail under list<struct<a: null>>
#   list_struct_nulls_tail:     the same tail when the struct declares a NULL row, so the child carries a
#                               validity bitmap and decodes on the ordinary path
#   list_null_short:            list<null> whose child declares fewer rows than the offsets reference: rejected
#   list_struct_null_small:     list<struct<a: null, b: null>> read as a Nullable tuple and as a plain one;
#                               its int64 metadata equal to the element count is then forged one value at a
#                               time to 2^40: every variant is rejected or read, none drives an allocation,
#                               and the nested length check fires for at least one of them

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
N = (1 << 31) - 1

def write(name, arr, field_name="a"):
    tbl = pa.table([arr], schema=pa.schema([pa.field(field_name, arr.type, nullable=True)]))
    with ipc.new_file(f"{out}/{name}.arrow", tbl.schema) as w:
        w.write_table(tbl)
    return open(f"{out}/{name}.arrow", "rb").read()

def list_of_nulls(offsets, validity, large=False, child_len=None):
    child_len = offsets[-1] if child_len is None else child_len
    offsets_type = pa.int64() if large else pa.int32()
    list_type = pa.large_list(pa.null()) if large else pa.list_(pa.null())
    return pa.Array.from_buffers(
        list_type, len(offsets) - 1,
        [pa.py_buffer(bytes([validity])), pa.array(offsets, type=offsets_type).buffers()[1]],
        children=[pa.nulls(child_len, type=pa.null())])

write("list_null_hidden_slot", list_of_nulls([0, N], 0b0))
write("list_null_visible_slot", list_of_nulls([0, N], 0b1))
write("list_null_hidden_head", list_of_nulls([N - 2, N], 0b1, child_len=N))
write("large_list_null_hidden_slot", list_of_nulls([0, N], 0b0, large=True))
write("list_null_mixed", list_of_nulls([0, 3, N], 0b01))

fsl_rows, fsl_size = 2047, 1 << 20
fsl_validity = bytearray((fsl_rows + 7) // 8)
fsl_validity[5 // 8] |= 1 << (5 % 8)
write("fsl_null_hidden", pa.Array.from_buffers(
    pa.list_(pa.null(), fsl_size), fsl_rows, [pa.py_buffer(bytes(fsl_validity))],
    children=[pa.nulls(fsl_rows * fsl_size, type=pa.null())]))

def struct_of_nulls(n, names, validity=None):
    fields = pa.struct([pa.field(name, pa.null(), nullable=True) for name in names])
    return pa.Array.from_buffers(
        fields, n, [None if validity is None else pa.py_buffer(validity)],
        children=[pa.nulls(n, type=pa.null()) for _ in names])

def list_of(child, offsets, validity):
    return pa.Array.from_buffers(
        pa.list_(child.type), len(offsets) - 1,
        [pa.py_buffer(bytes([validity])), pa.array(offsets, type=pa.int32()).buffers()[1]],
        children=[child])

write("list_struct_null_hidden", list_of(struct_of_nulls(N, ["a", "b"]), [0, N], 0b0))
# Struct rows 0 and 2 are valid, row 1 is NULL; list slot 0 holds row 0, the NULL slot 1 holds row 1, slot 2 holds row 2.
write("list_struct_nulls", list_of(struct_of_nulls(3, ["a"], validity=bytes([0b101])), [0, 1, 2, 3], 0b101))

def with_child_length(name, arr, referenced, declared):
    """Writes `arr` and then declares `declared` rows for its child (and that child's own subtree) instead
    of the `referenced` rows pyarrow normalizes it to on write; a larger count is the untruncated tail a
    writer keeps for a sliced list. The FieldNode lengths are the only int64 values equal to `referenced`.
    When the result is a valid Arrow file, pyarrow reads it back to confirm that."""
    data = bytearray(write(name, arr))
    for i in range(0, len(data) - 7, 8):
        if struct.unpack_from("<q", data, i)[0] == referenced:
            struct.pack_into("<q", data, i, declared)
    open(f"{out}/{name}.arrow", "wb").write(data)
    if declared >= referenced:
        chunk = ipc.open_file(f"{out}/{name}.arrow").read_all().column("a").chunk(0)
        assert len(chunk.values) == declared, len(chunk.values)

with_child_length("list_null_tail", list_of_nulls([0, 2], 0b1), 2, 3)
with_child_length("list_struct_null_tail", list_of(struct_of_nulls(2, ["a"]), [0, 2], 0b1), 2, 3)
# Struct row 0 is valid and row 1 is NULL, so the struct carries a validity bitmap.
with_child_length("list_struct_nulls_tail", list_of(struct_of_nulls(2, ["a"], validity=bytes([0b01])), [0, 2], 0b1), 2, 3)
with_child_length("list_null_short", list_of_nulls([0, 2], 0b1), 2, 1)

data = bytearray(write("list_struct_null_small", list_of(struct_of_nulls(7, ["a", "b"]), [0, 3, 7], 0b11)))
count = 0
for i in range(0, len(data) - 8, 8):
    if struct.unpack_from("<q", data, i)[0] == 7:
        patched = bytearray(data)
        patched[i:i + 8] = struct.pack("<q", 1 << 40)
        open(f"{out}/list_struct_null_small_forged_{i}.arrow", "wb").write(patched)
        count += 1
assert count > 0
PYEOF

read_column()
{
    ${CLICKHOUSE_LOCAL} --max_memory_usage=1G --query "SELECT $2 FROM file('${TMP_DIR}/$1.arrow', 'Arrow', '$3')" 2>&1 \
        | grep -o "MEMORY_LIMIT_EXCEEDED\|^[^C].*" | head -n 5
}

NULLS='a Array(Nullable(Nothing))'
echo "--- list_null_hidden_slot ---";        read_column list_null_hidden_slot "a" "$NULLS"
echo "--- list_null_visible_slot ---";       read_column list_null_visible_slot "length(a)" "$NULLS"
echo "--- list_null_hidden_head ---";        read_column list_null_hidden_head "a" "$NULLS"
echo "--- large_list_null_hidden_slot ---";  read_column large_list_null_hidden_slot "a" "$NULLS"
echo "--- list_null_mixed ---";              read_column list_null_mixed "a" "$NULLS"
echo "--- fsl_null_hidden: rows, empty rows, longest row ---"
read_column fsl_null_hidden "count(), countIf(empty(a)), max(length(a))" "$NULLS"
echo "--- list_struct_null_hidden ---"
read_column list_struct_null_hidden "a" "a Array(Tuple(a Nullable(Nothing), b Nullable(Nothing)))"
echo "--- list_struct_nulls as plain tuples ---"
read_column list_struct_nulls "a" "a Array(Tuple(a Nullable(Nothing)))"
echo "--- list_null_tail ---";         read_column list_null_tail "a" "$NULLS"
echo "--- list_struct_null_tail ---";  read_column list_struct_null_tail "a" "a Array(Tuple(a Nullable(Nothing)))"
echo "--- list_struct_nulls_tail ---"; read_column list_struct_nulls_tail "a" "a Array(Tuple(a Nullable(Nothing)))"
echo "--- list_null_short ---"
${CLICKHOUSE_LOCAL} --query "SELECT a FROM file('${TMP_DIR}/list_null_short.arrow', 'Arrow', '${NULLS}')" 2>&1 | grep -o "INCORRECT_DATA" | head -n 1
echo "--- list_struct_null_small as Nullable tuples, then as plain tuples ---"
${CLICKHOUSE_LOCAL} --allow_experimental_nullable_tuple_type=1 --query \
    "SELECT a FROM file('${TMP_DIR}/list_struct_null_small.arrow', 'Arrow', 'a Array(Nullable(Tuple(a Nullable(Nothing), b Nullable(Nothing))))')"
read_column list_struct_null_small "a" "a Array(Tuple(a Nullable(Nothing), b Nullable(Nothing)))"

oom=0
guard=0
for f in "${TMP_DIR}"/list_struct_null_small_forged_*.arrow; do
    err=$(${CLICKHOUSE_LOCAL} --max_memory_usage=1G \
        --query "SELECT a FROM file('${f}', 'Arrow', 'a Array(Tuple(a Nullable(Nothing), b Nullable(Nothing)))') FORMAT Null" 2>&1)
    case "$err" in
        *CANNOT_ALLOCATE_MEMORY*|*"bad_alloc"*|*MEMORY_LIMIT_EXCEEDED*) oom=$((oom + 1)) ;;
    esac
    case "$err" in
        *"struct field '"*"' declares"*) guard=$((guard + 1)) ;;
    esac
done
echo "forged variants that drove an allocation: $oom"
[ "$guard" -ge 1 ] && echo "nested length check active" || echo "nested length check NOT triggered"

#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow IPC streams.
#
# The native Arrow IPC reader maps a union to a `Variant` built from the Arrow schema alone, and a
# `Variant` -> `Variant` cast can only append alternatives, never substitute one, so an explicit structure
# naming a different alternative for a branch was rejected on a readable file. That included what the
# ClickHouse Arrow writer itself emits: it stores `IPv6` as `fixed_size_binary(16)`, so a file written from
# `Variant(IPv6, String)` could not be read back with `Variant(IPv6, String)`. Each branch now takes the
# alternative the request forces on it, resolved the way the native ORC reader resolves union branch hints;
# a branch whose correspondence stays ambiguous keeps its decoded type, so no guess is ever made.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"' EXIT
DATA="${CLICKHOUSE_TEST_UNIQUE_NAME}"

echo "--- the union the ClickHouse Arrow writer emits for Variant(IPv6, String) reads back as itself ---"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA}/rt.arrow', Arrow, 'v Variant(IPv6, String)')
        SETTINGS engine_file_truncate_on_insert = 1
        SELECT if(number = 0, CAST(toIPv6('::1'), 'Variant(IPv6, String)'), CAST('text', 'Variant(IPv6, String)'))
        FROM numbers(2)"
${CLICKHOUSE_CLIENT} --query "DESC file('${DATA}/rt.arrow', Arrow)"
${CLICKHOUSE_CLIENT} --query \
    "SELECT v, toTypeName(v) FROM file('${DATA}/rt.arrow', Arrow, 'v Variant(IPv6, String)') ORDER BY toString(v)"

echo "--- for Variant(IntervalDay, String), which it stores as a signed int64 ---"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA}/rt_interval.arrow', Arrow, 'v Variant(IntervalDay, String)')
        SETTINGS engine_file_truncate_on_insert = 1
        SELECT if(number = 0,
                  CAST(toIntervalDay(3), 'Variant(IntervalDay, String)'),
                  CAST('text', 'Variant(IntervalDay, String)'))
        FROM numbers(2)"
${CLICKHOUSE_CLIENT} --query "DESC file('${DATA}/rt_interval.arrow', Arrow)"
${CLICKHOUSE_CLIENT} --query \
    "SELECT v, toTypeName(v) FROM file('${DATA}/rt_interval.arrow', Arrow, 'v Variant(IntervalDay, String)') ORDER BY toString(v)"

echo "--- and for Variant(Date, String) under output_format_arrow_date_as_uint16 ---"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA}/rt_date.arrow', Arrow, 'v Variant(Date, String)')
        SETTINGS engine_file_truncate_on_insert = 1, output_format_arrow_date_as_uint16 = 1
        SELECT if(number = 0,
                  CAST(toDate('2022-01-08'), 'Variant(Date, String)'),
                  CAST('text', 'Variant(Date, String)'))
        FROM numbers(2)"
${CLICKHOUSE_CLIENT} --query "DESC file('${DATA}/rt_date.arrow', Arrow)"
${CLICKHOUSE_CLIENT} --query \
    "SELECT v, toTypeName(v) FROM file('${DATA}/rt_date.arrow', Arrow, 'v Variant(Date, String)') ORDER BY toString(v)"

python3 - "${CLICKHOUSE_USER_FILES_UNIQUE}" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
tids = pa.array([0, 1, 0, 1], type=pa.int8())
offs = pa.array([0, 0, 1, 1], type=pa.int32())
fsb = pa.array([bytes(range(16)), b"\x00" * 15 + b"\x01"], type=pa.binary(16))


def dense(children, names, offsets=offs):
    return pa.UnionArray.from_dense(tids, offsets, children, names)


def dense_not_null(children, names, type_ids, offsets, length):
    # A dense union whose children are declared non-nullable, which `UnionArray.from_dense` cannot
    # express: it makes every child nullable, and a nullable child decodes with a null map, which
    # makes the decoder gather every element. Only non-nullable children reach the layout below.
    union_type = pa.dense_union(
        [pa.field(name, child.type, nullable=False) for name, child in zip(names, children)])
    value_buffers = [pa.array(type_ids, type=pa.int8()).buffers()[1],
                     pa.array(offsets, type=pa.int32()).buffers()[1]]
    # A union has no validity bitmap, but some pyarrow versions still count a leading slot for it.
    padding = [None] * (union_type.num_buffers - len(value_buffers))
    return pa.Array.from_buffers(union_type, length, padding + value_buffers, children=children)


def bool_int8():
    return dense([pa.array([True, False]), pa.array([7, -8], type=pa.int8())], ["b", "i"])


columns = {
    # Substitution across signedness, plus the `Bool` / `UInt8` asymmetry: the `int8` branch is the only one
    # that can take `UInt8`, which forces `Bool` onto the `bool` branch.
    "bool_int8": bool_int8(),
    # `Bool` is a custom-named `UInt8`, so the two compare equal and only their names tell them apart.
    "bool_utf8": dense([pa.array([True, False]), pa.array(["x", "y"])], ["b", "s"]),
    # An `int8` branch is never re-declared as `Bool`, although `Bool` is a custom-named `UInt8` and the
    # two compare equal: a value outside 0/1 would come back as `true`.
    "int8_utf8": dense([pa.array([-8, 7], type=pa.int8()), pa.array(["x", "y"])], ["i", "s"]),
    # A raw-byte branch: `fixed_size_binary(16)` reinterpreted as a big integer.
    "fsb_utf8": dense([fsb, pa.array(["x", "y"])], ["f", "s"]),
    # The same, dictionary-encoded, so the branch's values arrive from a DictionaryBatch.
    "dict_fsb_utf8": dense(
        [pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int32()), fsb), pa.array(["x", "y"])],
        ["d", "s"]),
    # A `binary` branch holding text: the raw-byte width sniff declines and the cast text-parses it.
    "text_binary_int32": dense(
        [pa.array([b"::1", b"1.2.3.4"], type=pa.binary()), pa.array([7, 8], type=pa.int32())], ["b", "i"]),
    # The same shape holding a value that is neither 16 raw bytes nor `IPv6` text. Every branch takes an
    # alternative, so the read reaches the value conversion and fails there.
    "unparseable_binary_int32": dense(
        [pa.array([b"not-an-ip", b"1.2.3.4"], type=pa.binary()), pa.array([7, 8], type=pa.int32())],
        ["b", "i"]),
    # A `date32` branch is excluded by design: whether a day number is range-checked, saturated or copied
    # verbatim is decided inside the decoder, from a hint this post-decode repair cannot supply.
    "date32_utf8": dense([pa.array([19000, 19001], type=pa.date32()), pa.array(["x", "y"])], ["d", "s"]),
    # `date64` decodes to `DateTime`, whose only targets are `DateTime` and `DateTime64`, so one of them is
    # its single candidate below, while `uint32` admits two. It has to claim its own, or `uint32` stays ambiguous.
    "uint32_date64": dense(
        [pa.array([7, 8], type=pa.uint32()),
         pa.array([19000 * 86400000, 19001 * 86400000], type=pa.date64())], ["i", "d"]),
    # A `uint32` branch beside a branch that cannot take either of its special targets, so the branch
    # must take them itself: the two encodings the ClickHouse Arrow writer uses for IPv4 and DateTime.
    "uint32_utf8": dense([pa.array([7, 8], type=pa.uint32()), pa.array(["x", "y"])], ["i", "s"]),
    # The same second counts outside a union, to compare that branch against.
    "uint32_flat": pa.array([7, 8, 7, 8], type=pa.uint32()),
    # An Arrow timestamp decodes to DateTime64, and the narrowing DateTime target is refused: the cast
    # would drop sub-second precision and wrap anything outside 1970-2106.
    "ts_utf8": dense([pa.array([19000 * 86400000, 19001 * 86400000], type=pa.timestamp("ms", tz="UTC")),
                      pa.array(["x", "y"])], ["t", "s"]),
    # `duration[s]` decodes to `IntervalSecond`, which no conversion admits, while `int64` admits every
    # interval kind, so the duration branch has to claim `IntervalSecond` itself. Every alternative an
    # `int64` branch can take is numeric, hence the sibling and the suspicious-types setting below.
    "int64_duration": dense(
        [pa.array([7, 8], type=pa.int64()), pa.array([5, 6], type=pa.duration("s"))], ["i", "d"]),
    # Claiming its own alternative is not converting into it: a `Date32` branch stays excluded from
    # substitution and keeps the day numbers the flat column path returns, while its sibling widens.
    "uint32_date32": dense(
        [pa.array([7, 8], type=pa.uint32()), pa.array([19000, 19001], type=pa.date32())], ["i", "d"]),
    # The same day numbers outside a union, to compare that branch against.
    "date32_flat": pa.array([19000, 19001, 19000, 19001], type=pa.date32()),
    # Both branches can only take `UInt8`, so any assignment would put two branches on one alternative.
    "bool_uint8": dense([pa.array([True, False]), pa.array([7, 8], type=pa.uint8())], ["b", "i"]),
    # A branch that takes an alternative beside one that takes none and is not requested either, so the
    # repair is abandoned and the request keeps the message it reports today.
    "bool_date32": dense([pa.array([True, False]), pa.array([19000, 19001], type=pa.date32())], ["b", "d"]),
    # A dense branch whose retained slot the decoder gathers away, so the repair never sees the value 9.
    "retained": dense([pa.array([1, 9, 2], type=pa.int8()), pa.nulls(1)], ["i", "n"],
                      offsets=pa.array([0, 0, 2, 0], type=pa.int32())),
    # A dense branch whose retained slot the decoder keeps, which needs all three of: non-nullable
    # children, so no child contributes a null map; a total child row count no larger than the union's;
    # and a slot no row selects. Two rows aliasing slot 0 supply the third row the count needs while
    # leaving slot 1 unreferenced. That 2-byte slot must not decide how the referenced 16-byte values
    # are read: it is outside every row's reach, and the Arrow spec leaves its bytes undefined.
    "aliased_retained": dense_not_null(
        [pa.array([b"\x00" * 15 + b"\x01", b"zz", b"\x00" * 15 + b"\x02"], type=pa.binary()),
         pa.array([42], type=pa.int32())],
        ["b", "i"], [0, 0, 0, 1], [0, 0, 2, 0], 4),
    # A composite branch takes only its own alternative: this walk keeps the decoded names, and a cast to a
    # tuple whose names are disjoint converts positionally, so a field named `b` would come back holding `a`.
    "struct_utf8": dense(
        [pa.StructArray.from_arrays([pa.array([1, 2], type=pa.int32())], fields=[pa.field("a", pa.int32())]),
         pa.array(["x", "y"])], ["t", "s"]),
    # Neither branch prefers a requested alternative and both match both, so nothing is assigned.
    "binary_fsb": dense([pa.array([b"aa", b"bb"], type=pa.binary()), fsb], ["b", "f"]),
    # The Arrow child order differs from the sorted `Variant` order, so the repair must map local to global.
    "utf8_int32": dense([pa.array(["x", "y"]), pa.array([1, 2], type=pa.int32())], ["s", "i"]),
    "sparse": pa.UnionArray.from_sparse(
        tids, [pa.array([True, False, True, False]), pa.array([1, 7, 3, -8], type=pa.int8())], ["b", "i"]),
}
columns["nested"] = pa.StructArray.from_arrays(
    [bool_int8()], fields=[pa.field("v", bool_int8().type)])
columns["listed"] = pa.ListArray.from_arrays(pa.array([0, 2, 3, 4, 4]), bool_int8())

schema = pa.schema([pa.field(name, column.type) for name, column in columns.items()])
batch = pa.record_batch(list(columns.values()), schema=schema)
for fmt, factory in (("Arrow", ipc.new_file), ("ArrowStream", ipc.new_stream)):
    with factory(f"{out}/unions.{fmt}", schema) as writer:
        writer.write_batch(batch)
PY

# usage: read_column <column> <requested type> [format] [settings]
read_column()
{
    ${CLICKHOUSE_CLIENT} --query \
        "SELECT $1, toTypeName($1) FROM file('${DATA}/unions.${3:-Arrow}', '${3:-Arrow}', \$\$$1 $2\$\$)${4:+ SETTINGS $4}"
}

# usage: rejected <column> <requested type>; prints the reported conversion, not merely a failure
rejected()
{
    ${CLICKHOUSE_CLIENT} --query \
        "SELECT $1 FROM file('${DATA}/unions.Arrow', 'Arrow', \$\$$1 $2\$\$)" 2>&1 \
        | grep -oE 'Cannot convert type [^.]+\.' | head -1
}

# usage: named_error <column> <requested type>; prints the reported error name, so the arm pins which
# failure is reported and not merely that the read failed
named_error()
{
    ${CLICKHOUSE_CLIENT} --query \
        "SELECT $1 FROM file('${DATA}/unions.Arrow', 'Arrow', \$\$$1 $2\$\$)" 2>&1 \
        | grep -oE '\([A-Z][A-Z0-9_]+\)' | tail -1
}

echo "--- union<bool, int8> as Variant(Bool, UInt8) ---"
read_column bool_int8 'Variant(Bool, UInt8)'
echo "--- union<bool, utf8> as Variant(UInt8, String): Bool and UInt8 differ only by name ---"
read_column bool_utf8 'Variant(UInt8, String)'
echo "--- union<fixed_size_binary(16), utf8> as Variant(Int128, String) ---"
read_column fsb_utf8 'Variant(Int128, String)'
for FORMAT in Arrow ArrowStream; do
    echo "--- ${FORMAT}: union<dictionary<fixed_size_binary(16)>, utf8> as Variant(IPv6, String) ---"
    read_column dict_fsb_utf8 'Variant(IPv6, String)' "$FORMAT"
done
echo "--- a binary branch holding text falls back to text parsing ---"
read_column text_binary_int32 'Variant(IPv6, UInt32)'
echo "--- a retained slot no row selects must not reach the Enum8 repair ---"
read_column retained "Variant(Enum8('a' = 1, 'b' = 2))"
echo "--- nor decide the width of the values that are selected ---"
read_column aliased_retained 'Variant(IPv6, UInt32)'
# `session_timezone` is randomized in CI and `DateTime` renders in it, so the arm pins the zone it prints in.
echo "--- a branch with a single candidate claims its own alternative, which disambiguates its sibling ---"
read_column uint32_date64 'Variant(UInt64, DateTime)' Arrow "session_timezone = 'UTC'"
echo "--- an explicitly zoned DateTime alternative is a relabel, so the branch takes it ---"
read_column uint32_date64 "Variant(UInt64, DateTime('UTC'))"
echo "--- a DateTime64 alternative is what the flat date64 column reaches, so the branch takes it ---"
read_column uint32_date64 "Variant(UInt64, DateTime64(3, 'UTC'))"
echo "--- a uint32 branch takes IPv4, the encoding the writer uses for it ---"
read_column uint32_utf8 'Variant(IPv4, String)'
echo "--- and DateTime, its other writer encoding ---"
read_column uint32_utf8 'Variant(DateTime, String)' Arrow "session_timezone = 'UTC'"
echo "--- and DateTime64, which the flat uint32 column reaches at any scale ---"
read_column uint32_utf8 "Variant(DateTime64(3, 'UTC'), String)"
echo "--- the same seconds the flat column path returns for that request ---"
read_column uint32_flat "DateTime64(3, 'UTC')"
echo "--- and a duration branch claims the alternative its int64 sibling could otherwise take ---"
read_column int64_duration 'Variant(IntervalSecond, Int128)' Arrow "allow_suspicious_variant_types = 1"
echo "--- claiming it is not converting into it: an excluded branch keeps its own values ---"
read_column uint32_date32 'Variant(UInt64, Date32)'
echo "--- which are the values the flat column path returns for the same days ---"
read_column date32_flat 'Date32'
echo "--- the Arrow child order need not be the sorted Variant order ---"
read_column utf8_int32 'Variant(String, UInt32)'
echo "--- a sparse union ---"
read_column sparse 'Variant(Bool, UInt8)'
echo "--- a union under a struct and under a list ---"
read_column nested 'Tuple(v Variant(Bool, UInt8))'
read_column listed 'Array(Variant(Bool, UInt8))'

echo "--- a date32 branch is excluded by design, so this stays rejected ---"
rejected date32_utf8 'Variant(Int32, String)'
echo "--- and the narrowing DateTime64 -> DateTime is refused, so this stays rejected ---"
rejected ts_utf8 'Variant(DateTime, String)'
echo "--- a uint32 branch offered a wider integer beside DateTime64 takes neither ---"
rejected uint32_utf8 "Variant(Int64, DateTime64(3, 'UTC'), String)"
echo "--- two branches may never end up on one alternative ---"
rejected bool_uint8 'Variant(UInt8, String)'
echo "--- an int8 branch is never re-declared as Bool, so this stays rejected ---"
rejected int8_utf8 'Variant(Bool, String)'
echo "--- a composite branch is never re-declared, so a name-differing field stays rejected ---"
rejected struct_utf8 'Variant(Tuple(b Int32), String)'
echo "--- an ambiguous branch gets no alternative, so this stays rejected ---"
rejected binary_fsb 'Variant(IPv6, Int128)'
echo "--- an unassigned branch the request does not name abandons the repair, keeping the message ---"
rejected bool_date32 'Variant(UInt8, String)'
echo "--- and where every branch is assigned, a value that cannot convert is reported as itself ---"
named_error unparseable_binary_int32 'Variant(IPv6, UInt32)'

echo "--- schema inference has no requested type, so it is unaffected ---"
${CLICKHOUSE_CLIENT} --query "DESC file('${DATA}/unions.Arrow', 'Arrow')"
echo "--- and a request that merely extends the decoded Variant leaves every branch as decoded ---"
read_column bool_int8 'Variant(Bool, Int8, String)'

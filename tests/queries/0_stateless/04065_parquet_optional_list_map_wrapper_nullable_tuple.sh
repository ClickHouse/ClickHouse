#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet format is not available in fasttest builds

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Parquet fixtures that ClickHouse's own current writer cannot produce, checked in under
# data_parquet/. Two groups of them:
#
#  * pyarrow-written: an OPTIONAL LIST/MAP wrapper (ClickHouse only emits REQUIRED wrappers), whose
#    nulls the reader normalizes to empty collections so they never reach the inner tuple null map;
#    and a group with an array between it and its only leaf, the one shape where the leaf's own
#    null_count statistic says nothing about the group.
#
#  * written by a ClickHouse version from before the writer encoded a null Nullable(Tuple(...)) group
#    at the group's own definition level. On every leaf whose path under the group is nullable or
#    repeated, such a file records a null group as a present group with a null element, which no
#    reader can undo. Those files carry no clickhouse.nullable_group_def_levels key, so the reader
#    reads them only through a leaf whose path under the group adds no definition level, and refuses
#    them otherwise rather than returning a present struct where the user wrote NULL.

DATA="$CURDIR/data_parquet"

opts="--enable_nullable_tuple_type=1 --allow_experimental_nullable_tuple_type=1"

echo "-- optional LIST wrapper, REQUIRED element group: Array(Nullable(Tuple)) accepted (always-defined)"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_list_wrapper_required_element.parquet', 'Parquet', 'a Array(Nullable(Tuple(x UInt32)))')"

echo "-- optional MAP wrapper, REQUIRED value group: Map(String, Nullable(Tuple)) accepted"
$CLICKHOUSE_LOCAL $opts -q "SELECT m, toTypeName(m) FROM file('$DATA/04065_optional_map_wrapper_required_value.parquet', 'Parquet', 'm Map(String, Nullable(Tuple(x UInt32)))')"

echo "-- optional element group with all-REQUIRED subtree under a list: struct nulls read at the group's own level"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_struct_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')"

# Elements are [{x: 1}, null element group, {x: null}], so reading x as Nullable is what shows that
# the null element group and the element whose leaf is null stay distinct: NULL versus ((NULL)).
echo "-- optional element group with a NULLABLE leaf under a list: accepted, the group's level is read apart from the leaf's"
$CLICKHOUSE_LOCAL $opts -q "SELECT a FROM file('$DATA/04065_optional_struct_nullable_leaf_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')"
$CLICKHOUSE_LOCAL $opts -q "SELECT a FROM file('$DATA/04065_optional_struct_nullable_leaf_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x Nullable(UInt32)))))')"

# The leaf under this group is REQUIRED, so its own null_count is 0 while the group IS null in one
# row. Only the OUTERMOST tracked group may let that statistic skip deriving a group's map: an array
# sits between the two groups here, so a null group is an absent list element the writer need not
# count, and keying the shortcut on the innermost group instead drops row 2's NULL and returns an
# empty array. ClickHouse's own writer leaves null_count unset for such a leaf, so only an external
# writer produces this case.
echo "-- array between two nullable groups, leaf null_count = 0: the outer group's NULL survives"
$CLICKHOUSE_LOCAL $opts -q "SELECT p, p IS NULL FROM file('$DATA/04065_optional_struct_array_between_nullable_groups.parquet', 'Parquet', 'p Nullable(Tuple(b Array(Nullable(Tuple(c UInt32)))))')"

# Same file, and the reason the trust check fails OPEN on created_by: no leaf of this group is clean,
# so a marker-only rule would turn every correct third-party file into an error.
echo "-- ... and it is accepted although it carries no marker, because its producer is not ClickHouse"
$CLICKHOUSE_LOCAL $opts -q "SELECT count() FROM file('$DATA/04065_optional_struct_array_between_nullable_groups.parquet', 'Parquet', 'p Nullable(Tuple(b Array(Nullable(Tuple(c UInt32)))))')"

# Written by an affected ClickHouse on purpose: p is Nullable(Tuple(a Nullable(Int32), b Int32)) with
# rows (1,2), NULL, (2,3). Leaf b is REQUIRED, so its levels put the null group at 0 and are usable;
# leaf a records that group as def 1, "group present, a is NULL", and pyarrow reads the row as a
# present struct as well, so the file is wrong rather than our decoding of it.
echo "-- affected file, a clean element is among those read: struct NULL recovered through it"
$CLICKHOUSE_LOCAL $opts -q "SELECT p FROM file('$DATA/04065_clickhouse_written_ambiguous_nullable_tuple_levels.parquet', 'Parquet', 'p Nullable(Tuple(a Nullable(Int32), b Int32))')"

# Same file with the non-clean element hinted non-Nullable, where a null must be told from the group's
# own: leaf a's levels call the null group's row "a is NULL", so only the map taken from clean leaf b
# recovers it, and a check keyed on the leaf that saw the null refuses the row instead.
echo "-- affected file, the non-clean element read as non-Nullable at null_as_default = 0: still recovered"
$CLICKHOUSE_LOCAL $opts --input_format_null_as_default=0 -q "SELECT p FROM file('$DATA/04065_clickhouse_written_ambiguous_nullable_tuple_levels.parquet', 'Parquet', 'p Nullable(Tuple(a Int32, b Int32))')"

echo "-- affected file, only the non-clean element read: refused, not answered with a present struct"
$CLICKHOUSE_LOCAL $opts -q "SELECT p FROM file('$DATA/04065_clickhouse_written_ambiguous_nullable_tuple_levels.parquet', 'Parquet', 'p Nullable(Tuple(a Nullable(Int32)))')" 2>&1 | grep -o "TYPE_MISMATCH" | head -1

# Same, with both elements nullable, so no request has a clean leaf.
echo "-- affected file with no clean element at all: refused"
$CLICKHOUSE_LOCAL $opts -q "SELECT p FROM file('$DATA/04065_clickhouse_written_ambiguous_all_nullable.parquet', 'Parquet', 'p Nullable(Tuple(a Nullable(Int32), b Nullable(Int32)))')" 2>&1 | grep -o "TYPE_MISMATCH" | head -1

# Inference names the type and only the read enforces trust. Refusing during inference instead would
# break DESC and reads of unrelated columns of the same file for no gain.
echo "-- affected file: DESC still names Nullable(Tuple(...))"
$CLICKHOUSE_LOCAL $opts -q "DESC file('$DATA/04065_clickhouse_written_ambiguous_all_nullable.parquet', 'Parquet')"

echo "-- affected file: SELECT * of it is refused at read time"
$CLICKHOUSE_LOCAL $opts -q "SELECT * FROM file('$DATA/04065_clickhouse_written_ambiguous_all_nullable.parquet', 'Parquet')" 2>&1 | grep -o "TYPE_MISMATCH" | head -1

echo "-- affected file: with nullable-tuple inference off it is a plain Tuple and reads as one"
$CLICKHOUSE_LOCAL -q "SELECT p, toTypeName(p) FROM file('$DATA/04065_clickhouse_written_ambiguous_all_nullable.parquet', 'Parquet')"

# End to end over the marker: the same no-clean-leaf shape written by this build must read back,
# which fails if the writer stops emitting the key or the reader stops accepting it.
echo "-- the same shape written by this build carries the marker and round-trips"
T="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_04065_marker.parquet"
$CLICKHOUSE_LOCAL $opts --engine_file_truncate_on_insert=1 -m -q "
    INSERT INTO FUNCTION file('$T', Parquet, 'p Nullable(Tuple(a Nullable(Int32), b Nullable(Int32)))')
        SELECT if(number = 1, NULL, tuple(toInt32(number + 1), toInt32(number + 10))) FROM numbers(3);
    SELECT p FROM file('$T', Parquet, 'p Nullable(Tuple(a Nullable(Int32)))');"
rm -f "$T"

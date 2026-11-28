#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in fasttest builds, and pyarrow crafts three fixtures.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference of a Parquet OPTIONAL group with an all-REQUIRED subtree.
# https://github.com/ClickHouse/ClickHouse/issues/112427

T="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_04905"
trap 'rm -f "$T"_*.parquet' EXIT

OPTS="allow_experimental_nullable_tuple_type = 1, engine_file_truncate_on_insert = 1"

# All fixtures are written by ClickHouse itself, so the physical schema is checked in the same run.
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    INSERT INTO FUNCTION file('${T}_top.parquet', Parquet, 'p Nullable(Tuple(Float64, Float64))')
        SELECT (1, 2) UNION ALL SELECT NULL;
    INSERT INTO FUNCTION file('${T}_named.parquet', Parquet, 'p Nullable(Tuple(a UInt8, b String))')
        SELECT (1, 'x') UNION ALL SELECT NULL;
    INSERT INTO FUNCTION file('${T}_arr.parquet', Parquet, 'p Array(Nullable(Tuple(Float64, Float64)))')
        SELECT [(1, 2), NULL] UNION ALL SELECT [];
    INSERT INTO FUNCTION file('${T}_map.parquet', Parquet, 'p Map(String, Nullable(Tuple(z UInt8)))')
        SELECT map('k', tuple(1)) UNION ALL SELECT map('k', NULL);
    INSERT INTO FUNCTION file('${T}_nest.parquet', Parquet, 'p Nullable(Tuple(a UInt8, b Nullable(Tuple(c UInt8))))')
        SELECT (1, tuple(2)) UNION ALL SELECT NULL;
    INSERT INTO FUNCTION file('${T}_optleaf.parquet', Parquet, 'p Nullable(Tuple(a Nullable(UInt8)))')
        SELECT tuple(1) UNION ALL SELECT NULL;
    INSERT INTO FUNCTION file('${T}_req.parquet', Parquet, 'p Tuple(Float64, Float64)')
        SELECT (1, 2);
"

echo '--- top-level: optional group, all-required subtree'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_top.parquet', Parquet);
    SELECT * FROM file('${T}_top.parquet', Parquet) ORDER BY toString(p);"

echo '--- named elements'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_named.parquet', Parquet);
    SELECT * FROM file('${T}_named.parquet', Parquet) ORDER BY toString(p);"

echo '--- Array element: an array level is not an optional struct ancestor'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_arr.parquet', Parquet);
    SELECT * FROM file('${T}_arr.parquet', Parquet) ORDER BY length(p);"

echo '--- Map value struct'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_map.parquet', Parquet);
    SELECT * FROM file('${T}_map.parquet', Parquet) ORDER BY toString(p);"

echo '--- the inferred type carries the struct NULL into a table, no hint anywhere'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    CREATE TABLE t ENGINE = Memory AS SELECT * FROM file('${T}_top.parquet', Parquet);
    SELECT toTypeName(p), isNull(p) FROM t ORDER BY toString(p);"

echo '--- allow_experimental_nullable_tuple_type = 0: the type must stay one CREATE TABLE accepts'
$CLICKHOUSE_LOCAL -m -q "
    SET allow_experimental_nullable_tuple_type = 0;
    DESC file('${T}_top.parquet', Parquet);"

echo '--- schema_inference_make_columns_nullable = 0'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS, schema_inference_make_columns_nullable = 0;
    DESC file('${T}_top.parquet', Parquet);
    SELECT * FROM file('${T}_top.parquet', Parquet) ORDER BY p.1;"

# tupleSubtreeIsAllRequired refuses these two: a descendant OPTIONAL adds its own definition level,
# so a leaf null map is not the group null map. Reading them with a Nullable(Tuple) hint is rejected
# too, so inference must not name a type the read cannot honour.
echo '--- nested optional group: plain Tuple'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_nest.parquet', Parquet);"

echo '--- optional leaf under an optional group: plain Tuple'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_optleaf.parquet', Parquet);"

echo '--- required group: no struct-level NULL exists'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_req.parquet', Parquet);
    SELECT * FROM file('${T}_req.parquet', Parquet);"

echo '--- GeoParquet Point is a two-double group, but resolves as a geo type'
cp "$CUR_DIR"/data_parquet/03445_geoparquet_null_point.parquet "${T}_geo.parquet"
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_geo.parquet', Parquet);" | awk -F'\t' '{print $1, $2}'

echo '--- a single tuple element requested by name is a leaf read'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    SELECT p.1 FROM file('${T}_top.parquet', Parquet) ORDER BY 1;"

echo '--- hinted read with every element missing still rejects: the null map is unrecoverable'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    SELECT * FROM file('${T}_top.parquet', Parquet, 'p Nullable(Tuple(zzz UInt8))')
        SETTINGS input_format_parquet_allow_missing_columns = 1;" 2>&1 | grep -c 'TYPE_MISMATCH'

echo '--- sibling formats infer the same shape from the same data'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    INSERT INTO FUNCTION file('${T}_s.orc', ORC, 'p Nullable(Tuple(Float64, Float64))')
        SELECT (1, 2) UNION ALL SELECT NULL;
    INSERT INTO FUNCTION file('${T}_s.arrow', Arrow, 'p Nullable(Tuple(Float64, Float64))')
        SELECT (1, 2) UNION ALL SELECT NULL;
    SELECT 'ORC', startsWith(toTypeName(p), 'Nullable(Tuple') FROM file('${T}_s.orc', ORC) LIMIT 1;
    SELECT 'Arrow', startsWith(toTypeName(p), 'Nullable(Tuple') FROM file('${T}_s.arrow', Arrow) LIMIT 1;
    SELECT 'Parquet', startsWith(toTypeName(p), 'Nullable(Tuple') FROM file('${T}_top.parquet', Parquet) LIMIT 1;"
rm -f "${T}_s.orc" "${T}_s.arrow"

# The three fixtures below need a schema no compliant writer emits, so pyarrow writes a valid file
# and one field of its Thrift FileMetaData is rewritten. Each anchor spans the element's name, which
# makes it unique within the footer, and each rewrite is asserted to have landed.
python3 - "$T" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.parquet as pq

T = sys.argv[1]

# SchemaElement fields: 1=type 3=repetition_type 4=name 5=num_children. A Thrift compact field
# header is (field_id_delta << 4) | type, with I32 = 5 and BINARY = 8; an I32 value is zigzag, so
# a small n encodes as 2*n. REQUIRED = 0, OPTIONAL = 1; INT32 = 1, DOUBLE = 5.
def patch_footer(path, was, now, what):
    b = bytearray(open(path, "rb").read())
    flen = struct.unpack("<I", b[-8:-4])[0]
    start = len(b) - 8 - flen
    foot = b[start:start + flen]
    assert foot.count(was) == 1, f"{what}: anchor found {foot.count(was)} times, wanted 1"
    patched = foot.replace(was, now)
    assert was not in patched and patched.count(now) == 1, f"{what}: rewrite did not take"
    b[start:start + flen] = patched
    open(path, "wb").write(bytes(b))

# optional group p { required group q { } }. pyarrow refuses to write a childless group, so q gets
# an INT32 child and its num_children is cleared. Zero rows keeps the row group self-consistent:
# a childless group has no column chunk.
p = f"{T}_leafless.parquet"
pq.write_table(pa.Table.from_pylist([], schema=pa.schema([
    pa.field("p", pa.struct([pa.field("q", pa.struct([
        pa.field("c", pa.int32(), nullable=False)]), nullable=False)]), nullable=True)])),
    p, compression="none")
assert str(pq.ParquetFile(p).schema_arrow.field("p").type) \
    == "struct<q: struct<c: int32 not null> not null>", "leafless: unexpected pre-patch schema"
patch_footer(p, b"\x35\x00\x18\x01q\x15\x02", b"\x35\x00\x18\x01q\x15\x00",
             "leafless num_children 1 -> 0")

# A map whose key group is OPTIONAL. The Parquet MAP spec requires a REQUIRED key.
p = f"{T}_mapkey.parquet"
pq.write_table(pa.Table.from_pylist([{"m": [({"kx": 1}, 5)]}], schema=pa.schema([
    pa.field("m", pa.map_(pa.struct([pa.field("kx", pa.int32(), nullable=False)]),
                          pa.int32()), nullable=True)])), p, compression="none")
assert pq.ParquetFile(p).metadata.num_rows == 1, "mapkey: expected one row"
patch_footer(p, b"\x35\x00\x18\x03key\x15\x02", b"\x35\x02\x18\x03key\x15\x02",
             "map key repetition REQUIRED -> OPTIONAL")

# optional group p { required double u }, still annotated INTEGER(8, unsigned). A UInt8 leaf is
# INT32 physical with that annotation, so widening only the physical type leaves the annotation
# behind and this reader rejects the leaf.
p = f"{T}_unsup.parquet"
pq.write_table(pa.Table.from_pylist([{"p": {"u": 1}}], schema=pa.schema([
    pa.field("p", pa.struct([pa.field("u", pa.uint8(), nullable=False)]), nullable=True)])),
    p, compression="none")
col = pq.ParquetFile(p).schema.column(0)
assert col.physical_type == "INT32", f"unsupported: physical {col.physical_type}"
assert str(col.logical_type) == "Int(bitWidth=8, isSigned=false)", \
    f"unsupported: logical {col.logical_type}"
patch_footer(p, b"\x15\x02\x25\x00\x18\x01u", b"\x15\x0a\x25\x00\x18\x01u",
             "unsupported physical INT32 -> DOUBLE")
PYEOF

# A group whose only child is skipped has no leaf to reconstruct the null map from, so it must keep
# inferring a plain empty Tuple rather than a Nullable one.
echo '--- optional group with all children skipped as unsupported'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_unsup.parquet', Parquet)
        SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;"
echo '--- and without the skip setting it is still an error, not a silent empty tuple'
$CLICKHOUSE_LOCAL -q "
    DESC file('${T}_unsup.parquet', Parquet)
        SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 0" 2>&1 | grep -c 'INCORRECT_DATA'

# A childless group is an output column with no primitive below it, so nothing carries the
# definition levels the group null map is reconstructed from.
echo '--- optional group whose only child is a childless group'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_leafless.parquet', Parquet);
    SELECT count() FROM file('${T}_leafless.parquet', Parquet);"

# DataTypeMap rejects a Nullable key, so a Map key group stays a plain Tuple whatever its
# repetition type. The Parquet spec requires a REQUIRED key, so the fixture is non-compliant.
echo '--- optional Map key group'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_mapkey.parquet', Parquet);
    SELECT m FROM file('${T}_mapkey.parquet', Parquet);"

# In read mode a JSON hint on a struct leaves the element hints unset, so a hint-less node is not
# evidence of inference. Nullable(Tuple) named there without the group null map armed reaches the
# Map branch of the output assembly.
echo '--- JSON hint over a struct containing an optional all-required group'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    INSERT INTO FUNCTION file('${T}_json.parquet', Parquet, 'data Tuple(a UInt8, b Nullable(Tuple(c UInt8)))')
        SELECT (1, tuple(2));
    SELECT data FROM file('${T}_json.parquet', Parquet, 'data JSON');"
echo '--- JSON hint over a Map whose value is an optional all-required group'
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    INSERT INTO FUNCTION file('${T}_jsonmap.parquet', Parquet, 'data Map(String, Nullable(Tuple(z UInt8)))')
        SELECT map('k', tuple(1));
    SELECT data FROM file('${T}_jsonmap.parquet', Parquet, 'data JSON');"

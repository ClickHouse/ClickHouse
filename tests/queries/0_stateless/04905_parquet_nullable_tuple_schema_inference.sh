#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet format is not available in fasttest builds

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

# A group whose only child is skipped has no leaf to reconstruct the null map from, so it must keep
# inferring a plain empty Tuple rather than a Nullable one. The fixture is an optional group whose
# only child declares an INTEGER logical type on a DOUBLE physical type, which this reader rejects.
echo '--- optional group with all children skipped as unsupported'
cp "$CUR_DIR"/data_parquet/parquet_optional_struct_unsupported_child.parquet "${T}_unsup.parquet"
$CLICKHOUSE_LOCAL -m -q "
    SET $OPTS;
    DESC file('${T}_unsup.parquet', Parquet)
        SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;"
echo '--- and without the skip setting it is still an error, not a silent empty tuple'
$CLICKHOUSE_LOCAL -q "
    DESC file('${T}_unsup.parquet', Parquet)
        SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 0" 2>&1 | grep -c 'INCORRECT_DATA'

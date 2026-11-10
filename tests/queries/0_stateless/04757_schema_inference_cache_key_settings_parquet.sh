#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs Parquet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each carrier below is probed in BOTH orders, because an order-insensitive probe cannot
# detect a missing cache-key field: whichever query runs first decides the cached type.
# Every order gets its OWN file so the two orders never share a cache entry.
#
# The fixtures are aged with `touch -d`: SchemaCache::tryGetImpl drops an entry when the
# source's mtime is >= the entry's registration time, and both are whole seconds, so a file
# written in the same second as the first query is re-inferred and nothing is cached.

T="${CLICKHOUSE_TEST_UNIQUE_NAME}"
AGE="2000-01-01 00:00:00"

# --- allow_experimental_nullable_tuple_type -----------------------------------------------
# Decides whether an OPTIONAL group with an all-REQUIRED subtree is inferred as Nullable(Tuple(...))
# or as a plain Tuple(...), so each pair must report the type its own query asks for, whichever ran
# first. A stale entry also changes the value read back: the plain Tuple has nowhere to put a
# struct-level NULL and returns a tuple of NULLs instead.
$CLICKHOUSE_LOCAL -q "
    SET allow_experimental_nullable_tuple_type = 1;
    SELECT * FROM values('p Nullable(Tuple(a UInt8, b UInt8))', tuple(1, 2), NULL)
    INTO OUTFILE '${T}_nt_a.parquet' TRUNCATE FORMAT Parquet"
cp "${T}_nt_a.parquet" "${T}_nt_b.parquet"
touch -d "$AGE" "${T}"_nt_*.parquet
echo "-- Parquet nullable_tuple, nt=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_nt_a.parquet', 'Parquet') SETTINGS allow_experimental_nullable_tuple_type = 1;
    DESC file('${T}_nt_a.parquet', 'Parquet') SETTINGS allow_experimental_nullable_tuple_type = 0;" | cut -f2
echo "-- Parquet nullable_tuple, nt=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_nt_b.parquet', 'Parquet') SETTINGS allow_experimental_nullable_tuple_type = 0;
    DESC file('${T}_nt_b.parquet', 'Parquet') SETTINGS allow_experimental_nullable_tuple_type = 1;" | cut -f2
echo "-- Parquet nullable_tuple value, nt=1 first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT isNull(p) FROM file('${T}_nt_a.parquet', 'Parquet') ORDER BY 1 SETTINGS allow_experimental_nullable_tuple_type = 1;
    SELECT isNull(p) FROM file('${T}_nt_a.parquet', 'Parquet') ORDER BY 1 SETTINGS allow_experimental_nullable_tuple_type = 0;"

# --- input_format_parquet_allow_geoparquet_parser -----------------------------------------
# Decides whether a GeoParquet geometry column is inferred as a geo type or as its raw String
# representation, so each pair must report LineString for the =1 query and Nullable(String)
# for the =0 query, whichever ran first.
for suffix in a b; do cp "$CUR_DIR"/data_parquet/03445_geoparquet_null_linestring.parquet "${T}_geo_${suffix}.parquet"; done
touch -d "$AGE" "${T}"_geo_*.parquet
echo "-- Parquet allow_geoparquet_parser, geo=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_geo_a.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 1;
    DESC file('${T}_geo_a.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 0;" | awk -F'\t' '$1 == "geometry" {print $2}'
echo "-- Parquet allow_geoparquet_parser, geo=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_geo_b.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 0;
    DESC file('${T}_geo_b.parquet', 'Parquet') SETTINGS input_format_parquet_allow_geoparquet_parser = 1;" | awk -F'\t' '$1 == "geometry" {print $2}'

# --- input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference ---------
# Decides whether a column of an unsupported type is dropped or the file is rejected, so the
# permissive query must not let a later strict query skip the exception. Only this direction is a
# carrier: the strict query throws, and a throwing inference caches nothing.
# The data file has one VARIANT-typed column `u`, which is a valid Parquet logical type that is not
# implemented here, and one supported Int32 column `id`.
cp "$CUR_DIR"/data_parquet/parquet_variant_logical_type.parquet "${T}_unsup_a.parquet"
touch -d "$AGE" "${T}"_unsup_*.parquet
echo "-- Parquet skip_columns_with_unsupported_types, skip=1 first then strict must throw"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    DESC file('${T}_unsup_a.parquet', 'Parquet') FORMAT Null;" \
    2>&1 | grep -c INCORRECT_DATA
echo "-- Parquet skip_columns_with_unsupported_types, strict alone throws (control)"
$CLICKHOUSE_LOCAL -q "DESC file('${T}_unsup_a.parquet', 'Parquet') FORMAT Null" \
    2>&1 | grep -c INCORRECT_DATA
# Without this the arm above would also pass if nothing had been cached at all: the strict query
# throws either way. This shows the permissive query really did leave an entry, keyed on its value.
echo "-- Parquet skip_columns_with_unsupported_types, the permissive entry exists and is keyed"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    SELECT count(), extract(additional_format_info, 'skip_columns_with_unsupported_types=\w+')
    FROM system.schema_inference_cache WHERE format = 'Parquet' GROUP BY 2 ORDER BY 2;"
# An entry being present still does not prove a later query read it: with cache reads bypassed every
# query re-infers and rewrites the same entry. Repeating one query at unchanged settings must hit.
echo "-- Parquet skip_columns_with_unsupported_types, a repeated query hits the cache"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    DESC file('${T}_unsup_a.parquet', 'Parquet') SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1 FORMAT Null;
    SELECT value > 0 FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits';"

rm -f "${T}"_*

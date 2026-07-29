#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet format is not available in fasttest builds

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Parquet fixtures whose LIST/MAP wrapper is OPTIONAL but the inner element/value group is
# REQUIRED. ClickHouse's own writer only emits REQUIRED list wrappers, so these come from an
# external writer (pyarrow) and are checked in under data_parquet/.
#
# The optional wrapper's nulls are normalized to empty collections by the reader and never reach
# the inner tuple null-map, so an always-defined REQUIRED element/value read as Nullable(Tuple)
# is lossless and must be accepted (issue #109605 follow-up).
#
# Split struct and nullable-leaf null maps at their definition levels. Repeated descendants remain
# unsupported because their null maps have different cardinality.

DATA="$CURDIR/data_parquet"

opts="--enable_nullable_tuple_type=1 --allow_experimental_nullable_tuple_type=1"

echo "-- optional LIST wrapper, REQUIRED element group: Array(Nullable(Tuple)) accepted (always-defined)"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_list_wrapper_required_element.parquet', 'Parquet', 'a Array(Nullable(Tuple(x UInt32)))')"

echo "-- optional MAP wrapper, REQUIRED value group: Map(String, Nullable(Tuple)) accepted"
$CLICKHOUSE_LOCAL $opts -q "SELECT m, toTypeName(m) FROM file('$DATA/04065_optional_map_wrapper_required_value.parquet', 'Parquet', 'm Map(String, Nullable(Tuple(x UInt32)))')"

echo "-- optional element group with all-REQUIRED subtree under a list: struct nulls reconstructed losslessly, accepted"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_struct_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')"

echo "-- optional element group with a NULLABLE leaf under a list: split struct and leaf null maps"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_struct_nullable_leaf_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x Nullable(UInt32)))))')"

echo "-- optional element group with a NULLABLE leaf requested as non-nullable: reject leaf null"
$CLICKHOUSE_LOCAL $opts -q "SELECT a FROM file('$DATA/04065_optional_struct_nullable_leaf_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')" 2>&1 | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

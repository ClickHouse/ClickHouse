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
# An OPTIONAL element/struct group whose own subtree is all-REQUIRED carries genuine struct-level
# nulls, but every leaf's definition-level null map then equals the group null map, so it can be
# reconstructed and the tuple wrapped in Nullable losslessly (#109898) -- accepted. Only when the
# subtree adds another definition level (e.g. a nullable leaf) is the group null map no longer
# recoverable from a leaf, so that case must still be rejected.

DATA="$CURDIR/data_parquet"

opts="--enable_nullable_tuple_type=1 --allow_experimental_nullable_tuple_type=1"

echo "-- optional LIST wrapper, REQUIRED element group: Array(Nullable(Tuple)) accepted (always-defined)"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_list_wrapper_required_element.parquet', 'Parquet', 'a Array(Nullable(Tuple(x UInt32)))')"

echo "-- optional MAP wrapper, REQUIRED value group: Map(String, Nullable(Tuple)) accepted"
$CLICKHOUSE_LOCAL $opts -q "SELECT m, toTypeName(m) FROM file('$DATA/04065_optional_map_wrapper_required_value.parquet', 'Parquet', 'm Map(String, Nullable(Tuple(x UInt32)))')"

echo "-- optional element group with all-REQUIRED subtree under a list: struct nulls reconstructed losslessly, accepted"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_struct_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')"

echo "-- optional element group with a NULLABLE leaf under a list: leaf null map != struct null map, still rejected"
$CLICKHOUSE_LOCAL $opts -q "SELECT a FROM file('$DATA/04065_optional_struct_nullable_leaf_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')" 2>&1 | grep -o "TYPE_MISMATCH" | head -1

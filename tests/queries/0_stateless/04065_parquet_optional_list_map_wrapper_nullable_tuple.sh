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
# is lossless and must be accepted (issue #109605 follow-up). A genuinely OPTIONAL inner group
# carries real struct-level nulls and must still be rejected.

DATA="$CURDIR/data_parquet"

opts="--enable_nullable_tuple_type=1 --allow_experimental_nullable_tuple_type=1"

echo "-- optional LIST wrapper, REQUIRED element group: Array(Nullable(Tuple)) accepted (always-defined)"
$CLICKHOUSE_LOCAL $opts -q "SELECT a, toTypeName(a) FROM file('$DATA/04065_optional_list_wrapper_required_element.parquet', 'Parquet', 'a Array(Nullable(Tuple(x UInt32)))')"

echo "-- optional MAP wrapper, REQUIRED value group: Map(String, Nullable(Tuple)) accepted"
$CLICKHOUSE_LOCAL $opts -q "SELECT m, toTypeName(m) FROM file('$DATA/04065_optional_map_wrapper_required_value.parquet', 'Parquet', 'm Map(String, Nullable(Tuple(x UInt32)))')"

echo "-- optional element group under a list: genuine struct-level nulls, still rejected"
$CLICKHOUSE_LOCAL $opts -q "SELECT a FROM file('$DATA/04065_optional_struct_under_list.parquet', 'Parquet', 'a Array(Nullable(Tuple(inner Tuple(x UInt32))))')" 2>&1 | grep -o "TYPE_MISMATCH" | head -1

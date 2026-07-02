#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# input_format_binary_max_type_complexity guards binary type decoding on input only.

# On input, a type over the limit is rejected (Nullable(Array(UInt8)) has complexity 3 > 1).
$CLICKHOUSE_CLIENT --query "SELECT * FROM format(RowBinaryWithNamesAndTypes, x'010178231e01') SETTINGS input_format_binary_decode_types_in_binary_format=1, input_format_binary_max_type_complexity=1" 2>&1 | grep -o -m1 'Binary type decoding complexity limit exceeded'

# Reading already-stored JSON/Dynamic data is not limited by the setting: a low limit must not break reads.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_binary_type_complexity"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_binary_type_complexity (p JSON(max_dynamic_paths=0)) ENGINE=Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_binary_type_complexity VALUES ('{\"a\":[1,2,3]}')"
$CLICKHOUSE_CLIENT --query "SELECT p FROM t_binary_type_complexity SETTINGS input_format_binary_max_type_complexity=1"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_binary_type_complexity"

# A MergeTree part can hold a stored Dynamic value whose type complexity exceeds the input limit (e.g. data
# written before the guard existed). Reading it back decodes the Dynamic structure from the part and must not
# apply the guard. Tuple with 1001 elements has binary type complexity 1002 > 1000 default.
wide_tuple="tuple($(printf '1,%.0s' $(seq 1 1000))1)"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_binary_type_complexity_mt"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_binary_type_complexity_mt (d Dynamic(max_types=1)) ENGINE=MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_binary_type_complexity_mt SELECT ${wide_tuple}::Dynamic"
# WHERE references d so the Dynamic column is actually read (not answered by a trivial count).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_binary_type_complexity_mt WHERE length(d::String) > 0 SETTINGS input_format_binary_max_type_complexity=1"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_binary_type_complexity_mt"

# An aggregate-function state holding Dynamic (e.g. groupArray(Dynamic)) can be supplied by a client and decodes
# the value type via the arena path, so the guard applies there: a type over the limit is rejected, and the same
# state decodes when the limit is raised (0 = unlimited).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_type_complexity"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_agg_type_complexity ENGINE=Memory AS SELECT groupArrayState(${wide_tuple}::Dynamic) AS s"
$CLICKHOUSE_CLIENT --query "SELECT length(finalizeAggregation(s)) FROM t_agg_type_complexity SETTINGS input_format_binary_max_type_complexity=1" 2>&1 | grep -o -m1 'Binary type decoding complexity limit exceeded'
$CLICKHOUSE_CLIENT --query "SELECT length(finalizeAggregation(s)) FROM t_agg_type_complexity SETTINGS input_format_binary_max_type_complexity=0"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_agg_type_complexity"

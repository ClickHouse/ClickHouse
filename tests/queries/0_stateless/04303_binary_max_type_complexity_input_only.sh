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

# Reading a stored Dynamic value from a MergeTree part must not apply the guard either. max_types=0 keeps the
# value in the shared variant (its type is decoded from the part on read). Array(UInt8) has complexity 2 > 1,
# so a low limit would reject it if the guard applied on read.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_binary_type_complexity_mt"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_binary_type_complexity_mt (d Dynamic(max_types=0)) ENGINE=MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_binary_type_complexity_mt SELECT [1, 2, 3]::Array(UInt8)::Dynamic"
# WHERE references d so the Dynamic column is actually read (not answered by a trivial count).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_binary_type_complexity_mt WHERE length(d::String) > 0 SETTINGS input_format_binary_max_type_complexity=1"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_binary_type_complexity_mt"

# Decoding a stored aggregate-function state holding Dynamic (e.g. groupArray(Dynamic)) goes through
# ColumnDynamic, which is not limited by the setting (it can run in background operations that must not throw).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_type_complexity"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_agg_type_complexity ENGINE=Memory AS SELECT groupArrayState([1, 2, 3]::Array(UInt8)::Dynamic) AS s"
$CLICKHOUSE_CLIENT --query "SELECT length(finalizeAggregation(s)) FROM t_agg_type_complexity SETTINGS input_format_binary_max_type_complexity=1"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_agg_type_complexity"

#!/usr/bin/env bash

# shellcheck disable=SC2154

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT \
  --param_num="42" \
  --param_str="hello" \
  --param_date="2022-08-04 18:30:53" \
  --param_map="{'2b95a497-3a5d-49af-bf85-15763318cde7': [1.2, 3.4]}" \
  -q "select {num:UInt64}, {str:String}, {date:DateTime}, {map:Map(UUID, Array(Float32))}"


$CLICKHOUSE_CLIENT \
  --param_num="42" \
  --param_str="hello" \
  --param_date="2022-08-04 18:30:53" \
  --param_map="{'2b95a497-3a5d-49af-bf85-15763318cde7': [1.2, 3.4]}" \
  -q "select toTypeName({num:UInt64}), toTypeName({str:String}), toTypeName({date:DateTime}), toTypeName({map:Map(UUID, Array(Float32))})"


table_name="t_02377_extend_protocol_with_query_parameters_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT -n -q "
  create table $table_name(
    id Int64,
    arr Array(UInt8),
    map Map(String, UInt8),
    mul_arr Array(Array(UInt8)),
    map_arr Map(UInt8, Array(UInt8)),
    map_map_arr Map(String, Map(String, Array(UInt8))))
  engine = MergeTree
  order by (id)"


$CLICKHOUSE_CLIENT \
  --param_id="42" \
  --param_arr="[1, 2, 3]" \
  --param_map="{'abc': 22, 'def': 33}" \
  --param_mul_arr="[[4, 5, 6], [7], [8, 9]]" \
  --param_map_arr="{10: [11, 12], 13: [14, 15]}" \
  --param_map_map_arr="{'ghj': {'klm': [16, 17]}, 'nop': {'rst': [18]}}" \
  -q "insert into $table_name values({id: Int64}, {arr: Array(UInt8)}, {map: Map(String, UInt8)}, {mul_arr: Array(Array(UInt8))}, {map_arr: Map(UInt8, Array(UInt8))}, {map_map_arr: Map(String, Map(String, Array(UInt8)))})"


$CLICKHOUSE_CLIENT -q "select * from $table_name"


$CLICKHOUSE_CLIENT \
  --param_tbl="numbers" \
  --param_db="system" \
  --param_col="number" \
  -q "select {col:Identifier} from {db:Identifier}.{tbl:Identifier} limit 1 offset 5"


# it is possible to set parameter for the current session
$CLICKHOUSE_CLIENT -n -q "set param_n = 42; select {n: UInt8}"
# and it will not be visible to other sessions
$CLICKHOUSE_CLIENT -n -q "select {n: UInt8} -- { serverError 456 }"


# the same parameter could be set multiple times within one session (new value overrides the previous one)
$CLICKHOUSE_CLIENT -n -q "set param_n = 12; set param_n = 13; select {n: UInt8}"


# but multiple different parameters could be defined within each session
$CLICKHOUSE_CLIENT -n -q "
  set param_a = 13, param_b = 'str';
  set param_c = '2022-08-04 18:30:53';
  set param_d = '{\'10\': [11, 12], \'13\': [14, 15]}';
  select {a: UInt32}, {b: String}, {c: DateTime}, {d: Map(String, Array(UInt8))}"

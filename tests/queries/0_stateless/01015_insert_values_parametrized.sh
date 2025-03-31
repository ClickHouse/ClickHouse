#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS insert_values_parameterized";
$CLICKHOUSE_CLIENT --query="CREATE TABLE insert_values_parameterized (n UInt8, s String, a Array(Float32)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --input_format_values_deduce_templates_of_expressions=1 --input_format_values_interpret_expressions=0 --param_p_n="-1" --param_p_s="param" --param_p_a="[0.2,0.3]" --query="INSERT INTO insert_values_parameterized  VALUES
(1 + {p_n:Int8}, lower(concat('Hello', {p_s:String})), arraySort(arrayIntersect([],            {p_a:Array(Nullable(Float32))}))),\
(2 + {p_n:Int8}, lower(concat('world', {p_s:String})), arraySort(arrayIntersect([0.1,0.2,0.3], {p_a:Array(Nullable(Float32))}))),\
(3 + {p_n:Int8}, lower(concat('TEST',  {p_s:String})), arraySort(arrayIntersect([0.1,0.3,0.4], {p_a:Array(Nullable(Float32))}))),\
(4 + {p_n:Int8}, lower(concat('PaRaM', {p_s:String})), arraySort(arrayIntersect([0.5],         {p_a:Array(Nullable(Float32))})))";

$CLICKHOUSE_CLIENT --input_format_values_deduce_templates_of_expressions=0 --input_format_values_interpret_expressions=1 --param_p_n="-1" --param_p_s="param" --param_p_a="[0.2,0.3]" --query="INSERT INTO insert_values_parameterized  VALUES \
(5 + {p_n:Int8}, lower(concat('Evaluate', {p_s:String})), arrayIntersect([0, 0.2, 0.6], {p_a:Array(Nullable(Float32))}))"

$CLICKHOUSE_CLIENT --param_p_n="5" --param_p_s="param" --param_p_a="[0.2,0.3]" --query="INSERT INTO insert_values_parameterized  VALUES \
({p_n:Int8}, {p_s:String}, {p_a:Array(Nullable(Float32))})"

$CLICKHOUSE_CLIENT --query="SELECT * FROM insert_values_parameterized ORDER BY n";

$CLICKHOUSE_CLIENT --query="DROP TABLE insert_values_parameterized";

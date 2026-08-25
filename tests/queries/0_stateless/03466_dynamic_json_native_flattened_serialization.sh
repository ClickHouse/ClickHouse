#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -m -q "
create table test (d Dynamic(max_types=2)) engine=Memory;
insert into test values (42::Int64), ('hello'), ('2020-01-01'::Date), (43::Int64), ('2020-01-01'::Date), ([1, 2, 3]::Array(Int64)), (map(1::Int64, 2::Int64)), ('world'), ([4, 5]), (44::Int64);
select * from test format Native settings output_format_native_use_flattened_dynamic_and_json_serialization=1;
" | $CLICKHOUSE_LOCAL --table test --input-format Native -q "select d, dynamicType(d), isDynamicElementInSharedData(d) from test"

$CLICKHOUSE_LOCAL -m -q "
create table test (json JSON(a UInt32, max_dynamic_types=2, max_dynamic_paths=2)) engine=Memory;
insert into test values ('{\"a\" : 1, \"b\" : 42, \"c\" : \"2020-01-01\"}'), ('{\"a\" : 2, \"b\" : \"2020-01-01\", \"c\" : 42, \"d\" : 42}'), ('{\"a\" : 3, \"b\" : \"str1\", \"c\" : [1, 2, 3], \"d\" : \"str1\", \"e\" : 42}'), ('{\"a\" : 4, \"b\" : [1, 2, 3], \"c\" : \"str1\", \"d\" : [1, 2, 3], \"e\" : \"str1\"}');
select * from test format Native settings output_format_native_use_flattened_dynamic_and_json_serialization=1;
" | $CLICKHOUSE_LOCAL --table test --input-format Native -q "select json, JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json) from test settings output_format_json_quote_64bit_integers=0"

$CLICKHOUSE_LOCAL -m -q "
create table test (json JSON(max_dynamic_types=2, max_dynamic_paths=4)) engine=Memory;
insert into test values ('{\"a\" : [{\"b\" : 42, \"c\" : \"2020-01-01\"}]}'), ('{\"a\" : [{\"b\" : \"2020-01-01\", \"c\" : 42, \"d\" : 42}]}'), ('{\"a\" : [{\"b\" : \"str1\", \"c\" : [1, 2, 3], \"d\" : [1, 2, 3]}]}'), ('{\"a\" : [{\"b\" : 43, \"c\" : 43}]}');
select * from test format Native settings output_format_native_use_flattened_dynamic_and_json_serialization=1;
" | $CLICKHOUSE_LOCAL --table test --input-format Native -q "select json, JSONDynamicPathsWithTypes(json.a[][1]), JSONSharedDataPathsWithTypes(json.a[][1]) from test settings output_format_json_quote_64bit_integers=0"

$CLICKHOUSE_LOCAL -m -q "
create table test (json JSON(max_dynamic_types=0, max_dynamic_paths=0)) engine=Memory;
insert into test values ('{\"a\" : [{\"b\" : 42, \"c\" : \"2020-01-01\"}]}'), ('{\"a\" : [{\"b\" : \"2020-01-01\", \"c\" : 42, \"d\" : 42}]}'), ('{\"a\" : [{\"b\" : \"str1\", \"c\" : [1, 2, 3], \"d\" : [1, 2, 3]}]}'), ('{\"a\" : [{\"b\" : 43, \"c\" : 43}]}');
select * from test format Native settings output_format_native_use_flattened_dynamic_and_json_serialization=1;
" | $CLICKHOUSE_LOCAL --table test --input-format Native -q "select json, JSONDynamicPathsWithTypes(json.a[][1]), JSONSharedDataPathsWithTypes(json.a[][1]) from test settings output_format_json_quote_64bit_integers=0"

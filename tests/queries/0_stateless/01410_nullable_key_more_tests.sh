#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_func()
{
    engine=$1

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "drop table if exists table_with_nullable_keys"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "create table table_with_nullable_keys (nullable_int Nullable(UInt32), nullable_str Nullable(String), nullable_lc LowCardinality(Nullable(String)), nullable_ints Array(Nullable(UInt32)), nullable_misc Tuple(Nullable(String), Nullable(UInt32)), nullable_val Map(UInt32, Nullable(String)), value UInt8) engine $engine order by (nullable_int, nullable_str, nullable_lc, nullable_ints, nullable_misc, nullable_val) settings allow_nullable_key = 1, index_granularity = 1"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "insert into table_with_nullable_keys select * replace (cast(nullable_val as Map(UInt32, Nullable(String))) as nullable_val) from generateRandom('nullable_int Nullable(UInt32), nullable_str Nullable(String), nullable_lc Nullable(String), nullable_ints Array(Nullable(UInt32)), nullable_misc Tuple(Nullable(String), Nullable(UInt32)), nullable_val Array(Tuple(UInt32, Nullable(String))), value UInt8', 1, 30, 30) limit 1024"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_str = (select randomPrintableASCII(30)) or nullable_str in (select randomPrintableASCII(30) from numbers(3)) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_lc = (select randomPrintableASCII(30)) or nullable_lc in (select randomPrintableASCII(30) from numbers(3)) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_ints = [1, 2, null] or nullable_ints in (select * from generateRandom('nullable_ints Array(Nullable(UInt32))', 1, 30, 30) limit 3) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_misc = (select (randomPrintableASCII(30), rand())) or nullable_misc in (select arrayJoin([(randomPrintableASCII(30), null), (null, rand())]))"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "select * from table_with_nullable_keys where nullable_val = (select map(rand(), randomPrintableASCII(10), rand(2), randomPrintableASCII(20), rand(3), null)) or nullable_val in (select cast(nullable_ints as Map(UInt32, Nullable(String))) from generateRandom('nullable_ints Array(Tuple(UInt32, Nullable(String)))', 1, 30, 30) limit 3) format Null"

    curl -d@- -sS "${CLICKHOUSE_URL}" <<< "drop table table_with_nullable_keys"
}

test_func MergeTree
test_func AggregatingMergeTree
test_func ReplacingMergeTree

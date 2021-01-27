#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "sipHash should be calculated after filtration"
$CLICKHOUSE_CLIENT -q "explain actions = 1 select sum(x), sum(y) from (select sipHash64(number) as x, bitAnd(number, 1024) as y from numbers_mt(1000000000) limit 1000000000) where y = 0" | grep -o "FUNCTION sipHash64\|Filter column: equals"
echo "sorting steps should know about limit"
$CLICKHOUSE_CLIENT -q "explain actions = 1 select number from (select number from numbers(500000000) order by -number) limit 10" | grep -o "MergingSorted\|MergeSorting\|PartialSorting\|Limit 10"

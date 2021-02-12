#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "> sipHash should be calculated after filtration"
$CLICKHOUSE_CLIENT -q "explain actions = 1 select sum(x), sum(y) from (select sipHash64(number) as x, bitAnd(number, 1024) as y from numbers_mt(1000000000) limit 1000000000) where y = 0" | grep -o "FUNCTION sipHash64\|Filter column: equals"
echo "> sorting steps should know about limit"
$CLICKHOUSE_CLIENT -q "explain actions = 1 select number from (select number from numbers(500000000) order by -number) limit 10" | grep -o "MergingSorted\|MergeSorting\|PartialSorting\|Limit 10"

echo "-- filter push down --"
echo "> filter should be pushed down after aggregating"
$CLICKHOUSE_CLIENT -q "
    explain select * from (select sum(x), y from (
        select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0
    settings enable_optimize_predicate_expression=0" | grep -o "Aggregating\|Filter"

echo "> filter should be pushed down after aggregating, column after aggregation is const"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select *, y != 0 from (select sum(x), y from (
        select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0
    settings enable_optimize_predicate_expression=0" | grep -o "Aggregating\|Filter\|COLUMN Const(UInt8) -> notEquals(y, 0)"

echo "> one condition of filter should be pushed down after aggregating, other condition is aliased"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select * from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s != 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: notEquals(y, 0)\|ALIAS notEquals(s, 4) :: 1 -> and(notEquals(y, 0), notEquals(s, 4))"

echo "> one condition of filter should be pushed down after aggregating, other condition is casted"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select * from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s - 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: notEquals(y, 0)\|FUNCTION CAST(minus(s, 4) :: 1, UInt8 :: 3) -> and(notEquals(y, 0), minus(s, 4))"

echo "> one condition of filter should be pushed down after aggregating, other two conditions are ANDed"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select * from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s - 8 and s - 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: notEquals(y, 0)\|FUNCTION and(minus(s, 4) :: 2, minus(s, 8) :: 1) -> and(notEquals(y, 0), minus(s, 8), minus(s, 4))"

echo "> two conditions of filter should be pushed down after aggregating and ANDed, one condition is aliased"
$CLICKHOUSE_CLIENT -q "
    explain optimize = 1, actions = 1 select * from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s != 8 and y - 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: and(minus(y, 4), notEquals(y, 0))\|ALIAS notEquals(s, 8) :: 1 -> and(notEquals(y, 0), notEquals(s, 8), minus(y, 4))"

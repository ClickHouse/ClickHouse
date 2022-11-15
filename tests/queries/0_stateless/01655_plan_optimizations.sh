#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select x + 1 from (select y + 2 as x from (select dummy + 3 as y)) settings query_plan_max_optimizations_to_apply = 1" 2>&1 |
     grep -o "Too many optimizations applied to query plan"

echo "> sipHash should be calculated after filtration"
$CLICKHOUSE_CLIENT -q "explain actions = 1 select sum(x), sum(y) from (select sipHash64(number) as x, bitAnd(number, 1024) as y from numbers_mt(1000000000) limit 1000000000) where y = 0" | grep -o "FUNCTION sipHash64\|Filter column: equals"
echo "> sorting steps should know about limit"
$CLICKHOUSE_CLIENT -q "explain actions = 1 select number from (select number from numbers(500000000) order by -number) limit 10" | grep -o "Sorting\|Limit 10"

echo "-- filter push down --"
echo "> filter should be pushed down after aggregating"
$CLICKHOUSE_CLIENT -q "
    explain select * from (select sum(x), y from (
        select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0
    settings enable_optimize_predicate_expression=0" | grep -o "Aggregating\|Filter"
$CLICKHOUSE_CLIENT -q "
    select s, y from (select sum(x) as s, y from (
        select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 order by s, y
    settings enable_optimize_predicate_expression=0"

echo "> filter should be pushed down after aggregating, column after aggregation is const"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select s, y, y != 0 from (select sum(x) as s, y from (
        select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0
    settings enable_optimize_predicate_expression=0" | grep -o "Aggregating\|Filter\|COLUMN Const(UInt8) -> notEquals(y, 0)"
$CLICKHOUSE_CLIENT -q "
    select s, y, y != 0 from (select sum(x) as s, y from (
        select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 order by s, y, y != 0
    settings enable_optimize_predicate_expression=0"

echo "> one condition of filter should be pushed down after aggregating, other condition is aliased"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s != 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: notEquals(y, 0)\|ALIAS notEquals(s, 4) :: 1 -> and(notEquals(y, 0), notEquals(s, 4))"
$CLICKHOUSE_CLIENT -q "
    select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s != 4 order by s, y
    settings enable_optimize_predicate_expression=0"

echo "> one condition of filter should be pushed down after aggregating, other condition is casted"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s - 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: notEquals(y, 0)\|FUNCTION _CAST(minus(s, 4) :: 1, UInt8 :: 3) -> and(notEquals(y, 0), minus(s, 4))"
$CLICKHOUSE_CLIENT -q "
    select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s - 4 order by s, y
    settings enable_optimize_predicate_expression=0"

echo "> one condition of filter should be pushed down after aggregating, other two conditions are ANDed"
$CLICKHOUSE_CLIENT --convert_query_to_cnf=0 -q "
    explain actions = 1 select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s - 8 and s - 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: notEquals(y, 0)\|FUNCTION and(minus(s, 8) :: 1, minus(s, 4) :: 2) -> and(notEquals(y, 0), minus(s, 8), minus(s, 4))"
$CLICKHOUSE_CLIENT -q "
    select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s - 8 and s - 4 order by s, y
    settings enable_optimize_predicate_expression=0"

echo "> two conditions of filter should be pushed down after aggregating and ANDed, one condition is aliased"
$CLICKHOUSE_CLIENT --convert_query_to_cnf=0 -q "
    explain actions = 1 select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s != 8 and y - 4
    settings enable_optimize_predicate_expression=0" |
    grep -o "Aggregating\|Filter column\|Filter column: and(notEquals(y, 0), minus(y, 4))\|ALIAS notEquals(s, 8) :: 1 -> and(notEquals(y, 0), notEquals(s, 8), minus(y, 4))"
$CLICKHOUSE_CLIENT -q "
    select s, y from (
        select sum(x) as s, y from (select number as x, number + 1 as y from numbers(10)) group by y
    ) where y != 0 and s != 8 and y - 4 order by s, y
    settings enable_optimize_predicate_expression=0"

echo "> filter is split, one part is filtered before ARRAY JOIN"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select x, y from (
        select range(number) as x, number + 1 as y from numbers(3)
    ) array join x where y != 2 and x != 0" |
    grep -o "Filter column: and(notEquals(y, 2), notEquals(x, 0))\|ARRAY JOIN x\|Filter column: notEquals(y, 2)"
$CLICKHOUSE_CLIENT -q "
    select x, y from (
        select range(number) as x, number + 1 as y from numbers(3)
    ) array join x where y != 2 and x != 0 order by x, y"

# echo "> filter is split, one part is filtered before Aggregating and Cube"
# $CLICKHOUSE_CLIENT -q "
#     explain actions = 1 select * from (
#         select sum(x) as s, x, y from (select number as x, number + 1 as y from numbers(10)) group by x, y　with cube
#     ) where y != 0 and s != 4
#     settings enable_optimize_predicate_expression=0" |
#     grep -o "Cube\|Aggregating\|Filter column: notEquals(y, 0)"
# $CLICKHOUSE_CLIENT -q "
#     select s, x, y from (
#         select sum(x) as s, x, y from (select number as x, number + 1 as y from numbers(10)) group by x, y　with cube
#     ) where y != 0 and s != 4 order by s, x, y
#     settings enable_optimize_predicate_expression=0"

echo "> filter is pushed down before Distinct"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select x, y from (
        select distinct x, y from (select number % 2 as x, number % 3 as y from numbers(10))
    ) where y != 2
    settings enable_optimize_predicate_expression=0" |
    grep -o "Distinct\|Filter column: notEquals(y, 2)"
$CLICKHOUSE_CLIENT -q "
    select x, y from (
        select distinct x, y from (select number % 2 as x, number % 3 as y from numbers(10))
    ) where y != 2 order by x, y
    settings enable_optimize_predicate_expression=0"

echo "> filter is pushed down before sorting steps"
$CLICKHOUSE_CLIENT --convert_query_to_cnf=0 -q "
    explain actions = 1 select x, y from (
        select number % 2 as x, number % 3 as y from numbers(6) order by y desc
    ) where x != 0 and y != 0
    settings enable_optimize_predicate_expression = 0" |
    grep -o "Sorting\|Filter column: and(notEquals(x, 0), notEquals(y, 0))"
$CLICKHOUSE_CLIENT -q "
    select x, y from (
        select number % 2 as x, number % 3 as y from numbers(6) order by y desc
    ) where x != 0 and y != 0
    settings enable_optimize_predicate_expression = 0"

echo "> filter is pushed down before TOTALS HAVING and aggregating"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select * from (
        select y, sum(x) from (select number as x, number % 4 as y from numbers(10)) group by y with totals
    ) where y != 2
    settings enable_optimize_predicate_expression=0" |
    grep -o "TotalsHaving\|Aggregating\|Filter column: notEquals(y, 2)"
$CLICKHOUSE_CLIENT -q "
    select * from (
        select y, sum(x) from (select number as x, number % 4 as y from numbers(10)) group by y with totals
    ) where y != 2"

echo "> filter is pushed down before CreatingSets"
$CLICKHOUSE_CLIENT -q "
    explain select number from (
        select number from numbers(5) where number in (select 1 + number from numbers(3))
    ) where number != 2 settings enable_optimize_predicate_expression=0" |
    grep -o "CreatingSets\|Filter"
$CLICKHOUSE_CLIENT -q "
    select number from (
        select number from numbers(5) where number in (select 1 + number from numbers(3))
    ) where number != 2 settings enable_optimize_predicate_expression=0"

echo "> one condition of filter is pushed down before LEFT JOIN"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1
    select number as a, r.b from numbers(4) as l any left join (
        select number + 2 as b from numbers(3)
    ) as r on a = r.b where a != 1 and b != 2 settings enable_optimize_predicate_expression = 0" |
    grep -o "Join\|Filter column: notEquals(number, 1)"
$CLICKHOUSE_CLIENT -q "
    select number as a, r.b from numbers(4) as l any left join (
        select number + 2 as b from numbers(3)
    ) as r on a = r.b where a != 1 and b != 2 settings enable_optimize_predicate_expression = 0" | sort

echo "> one condition of filter is pushed down before INNER JOIN"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1
    select number as a, r.b from numbers(4) as l any inner join (
        select number + 2 as b from numbers(3)
    ) as r on a = r.b where a != 1 and b != 2 settings enable_optimize_predicate_expression = 0" |
    grep -o "Join\|Filter column: notEquals(number, 1)"
$CLICKHOUSE_CLIENT -q "
    select number as a, r.b from numbers(4) as l any inner join (
        select number + 2 as b from numbers(3)
    ) as r on a = r.b where a != 1 and b != 2 settings enable_optimize_predicate_expression = 0"

echo "> filter is pushed down before UNION"
$CLICKHOUSE_CLIENT -q "
    explain select a, b from (
        select number + 1 as a, number + 2 as b from numbers(2) union all select number + 1 as b, number + 2 as a from numbers(2)
    ) where a != 1 settings enable_optimize_predicate_expression = 0" |
    grep -o "Union\|Filter"
$CLICKHOUSE_CLIENT -q "
    select a, b from (
        select number + 1 as a, number + 2 as b from numbers(2) union all select number + 1 as b, number + 2 as a from numbers(2)
    ) where a != 1 settings enable_optimize_predicate_expression = 0"

echo "> function calculation should be done after sorting and limit (if possible)"
echo "> Expression should be divided into two subexpressions and only one of them should be moved after Sorting"
$CLICKHOUSE_CLIENT -q "
    explain actions = 1 select number as n, sipHash64(n) from numbers(100) order by number + 1 limit 5" |
    sed 's/^ *//g' | grep -o "^ *\(Expression (.*Before ORDER BY.*)\|Sorting\|FUNCTION \w\+\)"
echo "> this query should be executed without throwing an exception"
$CLICKHOUSE_CLIENT -q "
    select throwIf(number = 5) from (select * from numbers(10)) order by number limit 1"

-- An aggregation whose aggregate state consumed no rows still emits a row when it has no grouping
-- key, and so does the WITH TOTALS grand total. Every value below is pinned twice, once with
-- optimize_arithmetic_operations_in_aggregate_functions on and once with it off, and the two must
-- agree. The plan assertions pin that the optimization is still applied where it is sound.

SET empty_result_for_aggregation_by_empty_set = 0;
-- Renames the aggregates to minOrNull and friends, which the optimization does not recognise.
SET aggregate_functions_null_for_empty = 0;

SELECT 'plus and minus over an empty aggregation';

SELECT 'min plus', min(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min plus', min(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'max plus', max(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'max plus', max(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min minus', min(number - 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min minus', min(number - 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'max minus', max(number - 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'max minus', max(number - 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min constant on the left', min(1 - number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min constant on the left', min(1 - number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min low cardinality', min(toLowCardinality(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min low cardinality', min(toLowCardinality(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min date', min(toDate('2020-01-01') + number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min date', min(toDate('2020-01-01') + number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'emptied by a filter rather than by an empty table';

SELECT 'min plus filtered', min(number + 1) FROM numbers(10) WHERE number > 100 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min plus filtered', min(number + 1) FROM numbers(10) WHERE number > 100 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'multiply and divide by a degenerate constant';

SELECT 'min divide by zero', min(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min divide by zero', min(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min signed divide by zero', min(toInt64(number) / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min signed divide by zero', min(toInt64(number) / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'max divide by zero', max(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'max divide by zero', max(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'sum divide by zero', sum(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum divide by zero', sum(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min multiply by infinity', min(number * inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min multiply by infinity', min(number * inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min multiply by nan', min(number * nan) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min multiply by nan', min(number * nan) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min decimal divide by zero', min(toDecimal64(number, 2) / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min decimal divide by zero', min(toDecimal64(number, 2) / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'a negative constant produces a negative zero, which the reciprocal tells apart';

SELECT 'multiply reciprocal', 1 / min(toFloat64(number) * -3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'multiply reciprocal', 1 / min(toFloat64(number) * -3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'divide reciprocal', 1 / min(toFloat64(number) / -3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'divide reciprocal', 1 / min(toFloat64(number) / -3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'multiply left reciprocal', 1 / min(-3.5 * toFloat64(number)) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'multiply left reciprocal', 1 / min(-3.5 * toFloat64(number)) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'grouping shapes that still emit a row over empty input';

SELECT 'with totals', min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH TOTALS SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'with totals', min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH TOTALS SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'grouping sets with a keyless member', min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS ((number % 2), ()) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'grouping sets with a keyless member', min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS ((number % 2), ()) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'grouping sets all keyless', min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS (()) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'grouping sets all keyless', min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS (()) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'group by all', min(number + 1) FROM numbers(0) GROUP BY ALL SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'group by all', min(number + 1) FROM numbers(0) GROUP BY ALL SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'constant key literal', min(number + 1) FROM numbers(0) GROUP BY 'x' SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1, empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
SELECT 'constant key literal', min(number + 1) FROM numbers(0) GROUP BY 'x' SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0, empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
SELECT 'constant key folded', min(number + 1) FROM numbers(0) GROUP BY 1 + 1 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1, empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
SELECT 'constant key folded', min(number + 1) FROM numbers(0) GROUP BY 1 + 1 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0, empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;

SELECT 'the aggregate belongs to its own query, not to an enclosing one';

SELECT 'keyless outside a grouped subquery', min(m + 1) FROM (SELECT min(number) AS m FROM numbers(0) GROUP BY number % 2) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'keyless outside a grouped subquery', min(m + 1) FROM (SELECT min(number) AS m FROM numbers(0) GROUP BY number % 2) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'grouped outside a keyless subquery', min(m + 1) FROM (SELECT min(number) AS m FROM numbers(0)) GROUP BY m % 2 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'grouped outside a keyless subquery', min(m + 1) FROM (SELECT min(number) AS m FROM numbers(0)) GROUP BY m % 2 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'the wrong row was observable as a row count through HAVING';

SELECT 'having row count', count() FROM (SELECT min(number + 1) AS c FROM numbers(0) HAVING c > 0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'having row count', count() FROM (SELECT min(number + 1) AS c FROM numbers(0) HAVING c > 0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'aliased having row count', count() FROM (SELECT min((n AS a) + (1 AS b)) AS c FROM (SELECT number AS n FROM numbers(0)) WHERE (a > 0) AND (b > 0) HAVING c > 0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'aliased having row count', count() FROM (SELECT min((n AS a) + (1 AS b)) AS c FROM (SELECT number AS n FROM numbers(0)) WHERE (a > 0) AND (b > 0) HAVING c > 0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'a Variant operand makes the arithmetic Nullable while the aggregated operand is not';

SELECT 'variant constant', min(number + CAST(toUInt64(3) AS Variant(UInt64, String))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'variant constant', min(number + CAST(toUInt64(3) AS Variant(UInt64, String))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'variant constant on the left', min(CAST(toUInt64(3) AS Variant(UInt64, String)) + number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'variant constant on the left', min(CAST(toUInt64(3) AS Variant(UInt64, String)) + number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'variant aggregated operand', min(v + 1) FROM (SELECT CAST(number AS Variant(UInt64, String)) AS v FROM numbers(0)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'variant aggregated operand', min(v + 1) FROM (SELECT CAST(number AS Variant(UInt64, String)) AS v FROM numbers(0)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'nullability introduced by the constant, not by the aggregated operand';

SELECT 'nullable constant multiply', min(number * CAST(3 AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nullable constant multiply', min(number * CAST(3 AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nullable constant multiply on the left', max(CAST(3 AS Nullable(UInt64)) * number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nullable constant multiply on the left', max(CAST(3 AS Nullable(UInt64)) * number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nullable constant divide', min(number / CAST(3 AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nullable constant divide', min(number / CAST(3 AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'variant constant multiply', min(number * CAST(toUInt64(3) AS Variant(UInt64, String))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'variant constant multiply', min(number * CAST(toUInt64(3) AS Variant(UInt64, String))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'variant aggregated operand multiply', min(v * 3) FROM (SELECT CAST(number AS Variant(UInt64, String)) AS v FROM numbers(0)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'variant aggregated operand multiply', min(v * 3) FROM (SELECT CAST(number AS Variant(UInt64, String)) AS v FROM numbers(0)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'the count-scaled sum rewrite is unsound for a non-finite or negatively signed constant';

SELECT 'sum plus infinity', sum(number + inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum plus infinity', sum(number + inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'sum minus infinity', sum(number - inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum minus infinity', sum(number - inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'sum plus nan', sum(number + nan) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum plus nan', sum(number + nan) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'sum negative constant on the left reciprocal', 1 / sum(-3.5 - number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum negative constant on the left reciprocal', 1 / sum(-3.5 - number) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty sum plus infinity', sum(number + inf) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty sum plus infinity', sum(number + inf) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'avg preserves a date-like argument type, so its empty state is the epoch and not nan';

SELECT 'avg date', avg(toDate(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'avg date', avg(toDate(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty avg date', avg(toDate(number) + 1) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty avg date', avg(toDate(number) + 1) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'shapes the optimization must keep: a Nullable aggregate returns NULL over an empty state';

SELECT 'nullable plus', min(toNullable(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nullable plus', min(toNullable(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nullable divide by zero', min(toNullable(number) / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nullable divide by zero', min(toNullable(number) / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nullable multiply by infinity', sum(toNullable(number) * inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nullable multiply by infinity', sum(toNullable(number) * inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'shapes the optimization must keep: zero is preserved by a positive finite constant';

SELECT 'min multiply', min(number * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min multiply', min(number * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min multiply float reciprocal', 1 / min(toFloat64(number) * 3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min multiply float reciprocal', 1 / min(toFloat64(number) * 3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min multiply decimal', min(toDecimal64(number, 2) * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min multiply decimal', min(toDecimal64(number, 2) * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'min divide by infinity', min(number / inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'min divide by infinity', min(number / inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'sum multiply', sum(number * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum multiply', sum(number * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'avg plus', avg(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'avg plus', avg(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'sum plus', sum(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'sum plus', sum(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'totals with multiply', min(number * 3) FROM numbers(0) GROUP BY number % 2 WITH TOTALS SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'totals with multiply', min(number * 3) FROM numbers(0) GROUP BY number % 2 WITH TOTALS SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'why a grouping key set with a non-constant key is safe: it emits no row over empty input';

SELECT 'group by key rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'group by key rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'group by key and constant rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2, 'x') SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1, empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
SELECT 'group by key and constant rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2, 'x') SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0, empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
SELECT 'with rollup rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH ROLLUP) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'with rollup rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH ROLLUP) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'with cube rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH CUBE) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'with cube rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH CUBE) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'grouping sets all keyed rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS ((number % 2), (number % 3))) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'grouping sets all keyed rows', count() FROM (SELECT min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS ((number % 2), (number % 3))) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'non-empty input: the optimization is valid and the values are unchanged';

SELECT 'nonempty keyless', min(number + 1) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty keyless', min(number + 1) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty date', min(toDate('2020-01-01') + number) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty date', min(toDate('2020-01-01') + number) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty nullable', min(toNullable(number) + 1) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty nullable', min(toNullable(number) + 1) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty group by key', number % 2 AS k, min(number + 1) FROM numbers(4) GROUP BY k ORDER BY k SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty group by key', number % 2 AS k, min(number + 1) FROM numbers(4) GROUP BY k ORDER BY k SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty totals', min(number + 1) AS v FROM numbers(4) GROUP BY number % 2 WITH TOTALS ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty totals', min(number + 1) AS v FROM numbers(4) GROUP BY number % 2 WITH TOTALS ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty rollup', min(number + 1) AS v FROM numbers(4) GROUP BY number % 2 WITH ROLLUP ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty rollup', min(number + 1) AS v FROM numbers(4) GROUP BY number % 2 WITH ROLLUP ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty cube', min(number + 1) AS v FROM numbers(4) GROUP BY number % 2 WITH CUBE ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty cube', min(number + 1) AS v FROM numbers(4) GROUP BY number % 2 WITH CUBE ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty grouping sets', min(number + 1) AS v FROM numbers(4) GROUP BY GROUPING SETS ((number % 2), (number % 3)) ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty grouping sets', min(number + 1) AS v FROM numbers(4) GROUP BY GROUPING SETS ((number % 2), (number % 3)) ORDER BY v SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'nonempty multiply', min(number * 3) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'nonempty multiply', min(number * 3) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT 'the aggregate argument in the plan tells a kept hoist from a declined one';

SELECT 'declined: keyless plus', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number + 1)%';
SELECT 'declined: with totals', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH TOTALS SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number + 1)%';
SELECT 'declined: grouping sets with a keyless member', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS ((number % 2), ()) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number + 1)%';
SELECT 'declined: constant key', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY 1 + 1 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number + 1)%';
SELECT 'declined: negative constant', count() FROM (EXPLAIN actions = 1 SELECT min(toFloat64(number) * -3.5) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(toFloat64(number) * -3.5)%';
SELECT 'declined: divide by zero', count() FROM (EXPLAIN actions = 1 SELECT min(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number / 0)%';
SELECT 'declined: sum divide by zero', count() FROM (EXPLAIN actions = 1 SELECT sum(number / 0) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: sum(number / 0)%';
SELECT 'declined: avg plus', count() FROM (EXPLAIN actions = 1 SELECT avg(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: avg(number + 1)%';
SELECT 'declined: variant constant', count() FROM (EXPLAIN actions = 1 SELECT min(number + CAST(toUInt64(3) AS Variant(UInt64, String))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number + 3)%';
SELECT 'declined: nullable constant multiply', count() FROM (EXPLAIN actions = 1 SELECT min(number * CAST(3 AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number * %';
SELECT 'declined: sum plus infinity', count() FROM (EXPLAIN actions = 1 SELECT sum(number + inf) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: sum(number + inf)%';
SELECT 'kept: keyless multiply', count() FROM (EXPLAIN actions = 1 SELECT min(number * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number)%';
SELECT 'kept: keyless nullable plus', count() FROM (EXPLAIN actions = 1 SELECT min(toNullable(number) + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(toNullable(number))%';
SELECT 'kept: group by key', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number)%';
SELECT 'kept: with rollup', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH ROLLUP SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number)%';
SELECT 'kept: with cube', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY number % 2 WITH CUBE SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number)%';
SELECT 'kept: grouping sets all keyed', count() FROM (EXPLAIN actions = 1 SELECT min(number + 1) FROM numbers(0) GROUP BY GROUPING SETS ((number % 2), (number % 3)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain LIKE '%Aggregates: min(number)%';
-- The count-scaled sum rewrite is still applied for a positive finite constant: this prints 1 both
-- before and after the change, so a wholesale disabling of the sibling pass would redden it.
-- The count aggregate is matched case-insensitively, and as a boolean rather than a row count,
-- because the plan spells it `sumCount(number)` or `sum(number), count(number)` depending on
-- optimize_syntax_fuse_functions, which the test runner randomizes.
SELECT 'kept: sum plus finite', count() > 0 FROM (EXPLAIN actions = 1 SELECT sum(number + 1) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1) WHERE explain ILIKE '%count(number)%';
SELECT 'off: nothing is hoisted', count() FROM (EXPLAIN actions = 1 SELECT min(number * 3) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0) WHERE explain LIKE '%Aggregates: min(number)%';

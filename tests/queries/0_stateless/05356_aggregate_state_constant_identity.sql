-- Expressions that differ only in an aggregate function state constant are different expressions.

SELECT concat(initializeAggregation('groupArrayState', 'x'), toString(number)) AS r, concat(initializeAggregation('groupArrayState', 'y'), toString(number)) AS r FROM numbers(2); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM (SELECT concat(initializeAggregation('groupArrayState', 'x'), toString(number)) AS r, concat(initializeAggregation('groupArrayState', 'y'), toString(number)) AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- The same state bytes of different aggregate functions.
SELECT * FROM (SELECT finalizeAggregation(materialize(arrayReduce('quantileState(0.5)', [1, 2, 3, 100]))) + number AS r, finalizeAggregation(materialize(arrayReduce('quantileState(0.9)', [1, 2, 3, 100]))) + number AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT number, a FROM (SELECT number, finalizeAggregation(materialize(arrayReduce('quantileState(0.5)', [1, 2, 3, 100]))) + number AS a FROM numbers(2)) WHERE finalizeAggregation(materialize(arrayReduce('quantileState(0.9)', [1, 2, 3, 100]))) + number > 50 ORDER BY number SETTINGS query_plan_enable_optimizations = 1, query_plan_merge_expressions = 1;
SELECT finalizeAggregation(tupleElement(`(x)`, 1)), finalizeAggregation(tupleElement(`(x)_1`, 1)) FROM (SELECT tuple(arrayReduce('quantileState(0.5)', [1, 2, 3, 100]) AS x), tuple(arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS x));
SELECT count() FROM numbers(100) WHERE number IN (SELECT toUInt64(finalizeAggregation(materialize(arrayReduce('quantileState(0.5)', [1, 2, 3, 100])))) AS v) OR number IN (SELECT toUInt64(finalizeAggregation(materialize(arrayReduce('quantileState(0.9)', [1, 2, 3, 100])))) AS v);
-- The same, with the functions nested in the argument type of another state.
SELECT * FROM (SELECT finalizeAggregation(finalizeAggregation(materialize(arrayReduce('groupArrayState', [arrayReduce('quantileState(0.5)', [1, 2, 3, 100])])))[1]) + number AS r, finalizeAggregation(finalizeAggregation(materialize(arrayReduce('groupArrayState', [arrayReduce('quantileState(0.9)', [1, 2, 3, 100])])))[1]) + number AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT number, a FROM (SELECT number, finalizeAggregation(finalizeAggregation(materialize(arrayReduce('groupArrayState', [arrayReduce('quantileState(0.5)', [1, 2, 3, 100])])))[1]) + number AS a FROM numbers(2)) WHERE finalizeAggregation(finalizeAggregation(materialize(arrayReduce('groupArrayState', [arrayReduce('quantileState(0.9)', [1, 2, 3, 100])])))[1]) + number > 50 ORDER BY number SETTINGS query_plan_enable_optimizations = 1, query_plan_merge_expressions = 1;
SELECT count() FROM numbers(100) WHERE number IN (SELECT toUInt64(finalizeAggregation(finalizeAggregation(materialize(arrayReduce('groupArrayState', [arrayReduce('quantileState(0.5)', [1, 2, 3, 100])])))[1])) AS v) OR number IN (SELECT toUInt64(finalizeAggregation(finalizeAggregation(materialize(arrayReduce('groupArrayState', [arrayReduce('quantileState(0.9)', [1, 2, 3, 100])])))[1])) AS v);
DROP TABLE IF EXISTS t_aggregate_state_key;
CREATE TABLE t_aggregate_state_key (s String) ENGINE = MergeTree ORDER BY concat(arrayReduce('quantileState(0.5)', [1.]), s);
INSERT INTO t_aggregate_state_key VALUES ('a'), ('b');
SELECT s FROM t_aggregate_state_key ORDER BY concat(arrayReduce('quantileState(0.9)', [1.]), s) SETTINGS optimize_read_in_order = 1;
DROP TABLE t_aggregate_state_key;

-- States nested in Array, Map, Tuple, Variant and Nullable(Tuple).
SELECT * FROM (SELECT tuple([initializeAggregation('groupArrayState', 'x')], number) AS r, tuple([initializeAggregation('groupArrayState', 'y')], number) AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM (SELECT tuple(map(1, initializeAggregation('groupArrayState', 'x')), number) AS r, tuple(map(1, initializeAggregation('groupArrayState', 'y')), number) AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM (SELECT tuple(tuple(1, initializeAggregation('groupArrayState', 'x')), number) AS r, tuple(tuple(1, initializeAggregation('groupArrayState', 'y')), number) AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM (SELECT tuple(initializeAggregation('groupArrayState', 'x')::Variant(AggregateFunction(groupArray, String), UInt8), number) AS r, tuple(initializeAggregation('groupArrayState', 'y')::Variant(AggregateFunction(groupArray, String), UInt8), number) AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM (SELECT tuple(toNullable(tuple(initializeAggregation('groupArrayState', 'x'))), number) AS r, tuple(toNullable(tuple(initializeAggregation('groupArrayState', 'y'))), number) AS r FROM numbers(2)) SETTINGS enable_nullable_tuple_type = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- Repeated tuple element names are allowed, and each column keeps its own value.
SELECT * FROM (SELECT tuple(concat(initializeAggregation('groupArrayState', 'x'), toString(number)) AS x), tuple(concat(initializeAggregation('groupArrayState', 'y'), toString(number)) AS x) FROM numbers(2)) FORMAT TSVWithNames;

-- GROUP BY keys.
SELECT concat(initializeAggregation('groupArrayState', 'x'), toString(number)) FROM numbers(2) GROUP BY concat(initializeAggregation('groupArrayState', 'y'), toString(number)); -- { serverError NOT_AN_AGGREGATE }
SELECT concat(initializeAggregation('groupArrayState', 'x'), toString(number)) AS c FROM numbers(2) GROUP BY concat(initializeAggregation('groupArrayState', 'x'), toString(number)) ORDER BY c;

-- The same state is still the same expression.
SELECT * FROM (SELECT concat(initializeAggregation('groupArrayState', 'x'), toString(number)) AS r, concat(initializeAggregation('groupArrayState', 'x'), toString(number)) AS r FROM numbers(2));
SELECT count() FROM (SELECT * FROM (SELECT defaultValueOfTypeName('AggregateFunction(argMax, LowCardinality(String), DateTime)') AS r, defaultValueOfTypeName('AggregateFunction(argMax, String, DateTime)') AS r));
SELECT GROUPING(tuple(initializeAggregation('groupArrayState', 'x'), toDateTime(0))) AS g FROM numbers(1) GROUP BY ROLLUP(tuple(initializeAggregation('groupArrayState', 'x'), toDateTime(0, 'UTC'))) ORDER BY g SETTINGS session_timezone = 'UTC';
SELECT GROUPING((SELECT [tuple(range(1000001), initializeAggregation('countState', 1))])) AS g FROM numbers(1) GROUP BY ROLLUP((SELECT [tuple(range(1000001), initializeAggregation('countState', 1))])) ORDER BY g SETTINGS enable_scalar_subquery_optimization = 0;

-- A state beside Dynamic is compared; one inside Dynamic keeps the column comparison.
SELECT * FROM (SELECT tuple(tuple(initializeAggregation('groupArrayState', 'x'), initializeAggregation('countState', 1)::Dynamic(max_types=0)), number) AS r, tuple(tuple(initializeAggregation('groupArrayState', 'y'), initializeAggregation('countState', 1)::Dynamic(max_types=0)), number) AS r FROM numbers(2)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT count() FROM (SELECT * FROM (SELECT tuple(tuple(initializeAggregation('groupArrayState', 'x'), initializeAggregation('countState', 1)::Dynamic(max_types=0)), number) AS r, tuple(tuple(initializeAggregation('groupArrayState', 'x'), initializeAggregation('countState', 1)::Dynamic(max_types=0)), number) AS r FROM numbers(2)));

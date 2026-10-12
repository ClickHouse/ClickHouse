-- https://github.com/ClickHouse/ClickHouse/issues/85784
-- https://github.com/ClickHouse/ClickHouse/issues/118625

-- At short_circuit_function_evaluation = 'disable' the un-rewritten queries throw as well, so every
-- expected value below requires short-circuit evaluation to be on.
-- optimize_if_chain_to_multiif is pinned off: it flattens the nested `if` below into `multiIf` before
-- this pass runs, and the pass matches only `if`, so the nested witness would stop exercising it.
SET enable_analyzer = 1, optimize_rewrite_aggregate_function_with_if = 1,
    optimize_rewrite_sum_if_to_count_if = 0, short_circuit_function_evaluation = 'enable',
    optimize_if_chain_to_multiif = 0;

SELECT sum(if(number = 0, 0, intDiv(42, number))) FROM numbers(5);
SELECT sum(if(number = 0, 0, intDiv(42, number))) FROM numbers(5) SETTINGS optimize_rewrite_aggregate_function_with_if = 0;
SELECT sum(if(number != 0, intDiv(42, number), 0)) FROM numbers(5);
SELECT avg(if(number = 0, NULL, intDiv(42, number))) FROM numbers(5);
SELECT count(if(number = 0, NULL, intDiv(42, number))) FROM numbers(5);

SELECT sum(if((number = 0) OR ((number % 6) = 0), 0, intDiv(6, number))) FROM numbers(6);
SELECT sum(if(number = 0, 0, intDiv(42, number) + 1)) FROM numbers(5);
SELECT sum(if(number = 0, 0, if(number > 100, 0, intDiv(42, number)))) FROM numbers(5);
-- A conversion is protected at 'enable' too: the guard asks the generic suitability predicate, not a
-- list of arithmetic functions.
SELECT count(if(s = 'bad', NULL, toDate(s))) FROM (SELECT arrayJoin(['2020-01-01', 'bad']) AS s);

-- A subquery on the right of IN is not an expression: it has no result type, and laziness does not
-- reach inside it.
SELECT sum(if(number = 0, 0, number IN (SELECT 1 UNION ALL SELECT 2))) FROM numbers(5);
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, number IN (SELECT intDiv(42, number) FROM numbers(1, 5)))) FROM numbers(5)
) WHERE explain LIKE '%sumIf%';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, number IN (SELECT intDiv(42, number) FROM numbers(1, 5) UNION ALL SELECT 1))) FROM numbers(5)
) WHERE explain LIKE '%sumIf%';

-- The rewrite is still applied when the lifted branch was never evaluated lazily.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, number)) FROM numbers(5)) WHERE explain LIKE '%sumIf%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, number, 0)) FROM numbers(5)) WHERE explain LIKE '%sumIf%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, number * 2)) FROM numbers(5)) WHERE explain LIKE '%sumIf%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, intDiv(42, number))) FROM numbers(5)) WHERE explain LIKE '%sumIf%';

SELECT 'force_enable';
-- addDate reports no short-circuit suitability, so only forced laziness protects it.
SELECT count(if(s = 'bad', NULL, addDate(s, INTERVAL 1 DAY)))
FROM (SELECT arrayJoin(['2020-01-01', 'bad']) AS s)
SETTINGS short_circuit_function_evaluation = 'force_enable';
-- The same query throws at 'enable': nothing was protected there, so the rewrite changes nothing.
SELECT count(if(s = 'bad', NULL, addDate(s, INTERVAL 1 DAY)))
FROM (SELECT arrayJoin(['2020-01-01', 'bad']) AS s); -- { serverError CANNOT_PARSE_DATETIME }
SELECT count() FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, number * 2)) FROM numbers(5))
WHERE explain LIKE '%sumIf%'
SETTINGS short_circuit_function_evaluation = 'force_enable';
-- A bare column is still rewritten at forced laziness: the guard declines only for a function.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, number)) FROM numbers(5))
WHERE explain LIKE '%sumIf%'
SETTINGS short_circuit_function_evaluation = 'force_enable';

SELECT 'disable';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(number = 0, 0, intDiv(42, number))) FROM numbers(5))
WHERE explain LIKE '%sumIf%'
SETTINGS short_circuit_function_evaluation = 'disable';

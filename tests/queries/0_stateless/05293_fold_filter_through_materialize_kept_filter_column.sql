-- A filter predicate that is constant through `materialize` folds also when the filter column stays in the output:
-- the filter reads the folded constant, and the kept column keeps its value and its representation.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'pretty';

-- The kept column is read above the filter.
SELECT 'kept filter column folded', countIf(explain LIKE '%Filter column: 1%')
FROM (EXPLAIN PLAN actions = 1 SELECT c, number FROM (SELECT materialize(1) = 1 AS c, number FROM numbers(3) WHERE c));
SELECT c, isConstant(c), number FROM (SELECT materialize(1) = 1 AS c, number FROM numbers(3) WHERE c);

-- Nothing reads it above the filter, so only the constant is left.
SELECT 'unused filter column folded', countIf(explain LIKE '%Filter column: 1%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM (SELECT materialize(1) = 1 AS c, number FROM numbers(10) WHERE c));
SELECT count() FROM (SELECT materialize(1) = 1 AS c, number FROM numbers(10) WHERE c);

-- A predicate that folds to false keeps no row.
SELECT 'kept filter column folded to false', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1 SELECT c, number FROM (SELECT materialize(1) = 0 AS c, number FROM numbers(3) WHERE c));
SELECT c, number FROM (SELECT materialize(1) = 0 AS c, number FROM numbers(3) WHERE c);

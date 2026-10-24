-- A lambda that captures an `IN` set is wrapped in `ColumnConst` before the set is built, so the higher-order
-- function around it was taken for a constant while the header was being evaluated, and all an unbuilt set can
-- answer with is the dummy `in` reserves for one. `FilterTransform` reads `always_false` off such a constant and
-- closes its input, so a filter that is in fact true dropped every row.

-- Only the analyzer's path folds the capture here; the old analyzer answers correctly either way.
SET enable_analyzer = 1;

-- The filter is true, so the row is returned.
SELECT number FROM numbers(1) WHERE arrayExists(x -> x IN (SELECT 1), [1, 2]);

-- A predicate that is genuinely false still drops the row.
SELECT count() FROM numbers(1) WHERE arrayExists(x -> x IN (SELECT 2), [1, 3]);

-- Folding is deferred, not disabled: once the set is built the expression is folded and is a constant.
SELECT arrayExists(x -> x IN (SELECT 1), [1, 2]) AS e, isConstant(e);

-- A lambda that captures no set is still folded, which is what allows it on the right-hand side of `IN`.
-- `isConstant` is what asserts the folding: the `IN` alone also answers correctly without it, over a row-wise set.
SELECT 3 IN (arrayMap(x -> x + 1, [1, 2])), isConstant(arrayMap(x -> x + 1, [1, 2]));

-- A single constant conjunct used to decide the whole filter.
SELECT count() FROM numbers(1) WHERE arrayExists(x -> x IN (SELECT 1), [1, 2]) AND number >= 0;

-- The value must not depend on being read through a derived table, where the same false constant was
-- consumed by the projection rather than by a filter.
SELECT * FROM (SELECT arrayExists(x -> x IN (SELECT 2), [2]));

-- `NOT IN` reaches the same gate as `IN`: 1 and 3 are not in the set, so the filter is true and the
-- row is returned.
SELECT number FROM numbers(1) WHERE arrayExists(x -> x NOT IN (SELECT 2), [1, 3]);

-- A distinct spelling of the same operator must pass the same readiness gate, so the row is returned.
SELECT number FROM numbers(1) WHERE arrayExists(x -> x GLOBAL IN (SELECT 1), [1, 2]);

-- `HAVING` consumes the same constant above the aggregation, so the group must survive.
SELECT count() FROM numbers(1) GROUP BY number HAVING arrayExists(x -> x IN (SELECT 1), [1, 2]);

-- The `map*` family does not forward the dry-run flag into the lambda, so this spelling aborted with
-- `Not-ready Set` rather than returning a wrong value. The filter is true, so the row is returned.
SELECT number FROM numbers(1) WHERE mapExists((k, v) -> k IN (SELECT 1), map(1, 2));

-- Nesting reaches the gate too: whether the set stays in the outer capture list or the inner lambda is
-- hoisted out of it, what the outer lambda captures is not a foldable constant, so the row is returned.
SELECT number FROM numbers(1) WHERE arrayExists(x -> arrayExists(y -> y IN (SELECT 1), [1]), [1]);

-- `arrayFold` is a separate implementation rather than an `arrayMap` sibling, and it does not forward the
-- dry-run flag into its lambda, so this nesting aborted with `Not-ready Set`. Only 1 is in the set.
SELECT arrayFilter(k -> arrayFold((acc, x) -> x IN (SELECT 1), [k], toUInt8(0)), [1, 2]);

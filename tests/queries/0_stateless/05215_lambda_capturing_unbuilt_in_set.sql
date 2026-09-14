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

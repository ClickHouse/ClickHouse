-- Higher-order functions must forward the dry-run flag into the lambda they evaluate, so that an
-- `IN (SELECT ...)` in the lambda body is not run against a set the plan has not built yet.

SELECT mapFilter((k, v) -> k IN (SELECT 1), map(1, 2, 3, 4));
SELECT mapApply((k, v) -> (k, v IN (SELECT 2)), map(1, 2, 3, 4));
SELECT mapExists((k, v) -> k IN (SELECT 1), map(1, 2, 3, 4));
SELECT mapAll((k, v) -> k IN (SELECT 1), map(1, 2, 3, 4));
SELECT mapSort((k, v) -> k IN (SELECT 1), map(1, 2, 3, 4));
SELECT mapReverseSort((k, v) -> k IN (SELECT 1), map(1, 2, 3, 4));
SELECT mapPartialSort((k, v) -> k IN (SELECT 1), 1, map(1, 2, 3, 4));
SELECT mapPartialReverseSort((k, v) -> k IN (SELECT 1), 1, map(1, 2, 3, 4));
SELECT arrayFilter(k -> arrayFold((acc, x) -> x IN (SELECT 1), [k], toUInt8(0)), [1, 2]);

-- `arrayFilter` already forwarded the flag, so it is the reference behaviour for the cases above.
SELECT arrayFilter(k -> k IN (SELECT 1), [1, 2]);

-- `mapExtractKeyLike`, `mapExtractValueLike`, `mapContainsKeyLike` and `mapContainsValueLike` reach
-- the adapter through the LowCardinality fast-path mixin and inherit its dry run, so neither their
-- value nor their type may change.
SELECT mapExtractKeyLike(m, 'a%'), toTypeName(mapExtractKeyLike(m, 'a%'))
FROM (SELECT map('a'::LowCardinality(String), 'b'::String) AS m);

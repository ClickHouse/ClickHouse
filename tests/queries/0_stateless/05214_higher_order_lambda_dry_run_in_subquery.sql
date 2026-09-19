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

-- `mapContainsKeyLike` and `mapContainsValueLike` return `UInt8`, so with a `LowCardinality`
-- argument of their own and every argument constant their declared type is `LowCardinality(UInt8)`,
-- and a dry run must produce a column of that type rather than a bare `UInt8`.
SELECT mapContainsKeyLike(map('a', 1, 'b', 2), toLowCardinality('a%')),
       toTypeName(mapContainsKeyLike(map('a', 1, 'b', 2), toLowCardinality('a%')));
SELECT mapContainsValueLike(map(1, 'a', 2, 'b'), toLowCardinality('a%')),
       toTypeName(mapContainsValueLike(map(1, 'a', 2, 'b'), toLowCardinality('a%')));

-- `mapExtractKeyLike` and `mapExtractValueLike` return `Map(...)`, which cannot be inside
-- `LowCardinality`, so this pins the value and the type of the path that is never wrapped.
SELECT mapExtractKeyLike(m, 'a%'), toTypeName(mapExtractKeyLike(m, 'a%'))
FROM (SELECT map('a'::LowCardinality(String), 'b'::String) AS m);

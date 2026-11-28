-- `x = ALL (subquery)` keeps the one-element `IN` comparison against `singleValueOrNull` for a non-empty
-- right-hand side: when the subquery returns a NULL or several distinct values, the result is 0, not NULL.
-- An empty right-hand side is TRUE (vacuous truth).
SELECT 1 = ALL (SELECT CAST(NULL AS Nullable(UInt8)));
SELECT 1 = ALL (SELECT number FROM numbers(2));
SELECT 1 = ALL (SELECT 1 FROM numbers(3));
SELECT 1 = ALL (SELECT toNullable(1) FROM numbers(3));
SELECT 1 = ALL (SELECT 1 WHERE 0);
SELECT number, number = ALL (SELECT 1 UNION ALL SELECT 1) FROM numbers(3);

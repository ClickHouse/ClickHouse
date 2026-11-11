-- A table expression is only allowed as the second (right) argument of IN.
-- A table on the left side must produce a clear error instead of an internal
-- "Method getResultType is not supported for TABLE query tree node" exception.

SET enable_analyzer = 1;

SELECT system.numbers IN (1, 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT system.numbers NOT IN (1, 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT system.numbers GLOBAL IN (1, 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT system.numbers GLOBAL NOT IN (1, 2, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Legitimate uses of IN keep working.
SELECT 2 IN (1, 2, 3);
SELECT 'system' IN (SELECT name FROM system.databases) LIMIT 1;
SELECT count() FROM system.one WHERE dummy IN (SELECT number FROM numbers(3));

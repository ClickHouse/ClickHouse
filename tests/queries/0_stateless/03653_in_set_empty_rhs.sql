-- Reproducer for building IN set from an empty RHS subquery
-- These queries should not crash and should evaluate according to SQL semantics.

-- From the original issue: empty tuple membership against a constant empty-tuple set
-- Should not crash; before the fix this produced an unsupported-method error
SELECT () IN (()); -- { serverError UNSUPPORTED_METHOD }

-- Empty scalar RHS
SELECT 1 IN (SELECT number FROM numbers(0));

-- Empty tuple RHS
SELECT (1, 2) IN (SELECT (number, number) FROM numbers(0));

-- NOT IN with empty RHS should be true
SELECT 1 NOT IN (SELECT number FROM numbers(0));
SELECT (1, 2) NOT IN (SELECT (number, number) FROM numbers(0));

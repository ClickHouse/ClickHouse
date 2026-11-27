-- Regression test for a LOGICAL_ERROR "Block structure mismatch in joined block stream: different number
-- of columns" thrown from `ConstantJoin::addBlockToJoin`. A correlated scalar subquery with a constant-true
-- predicate is decorrelated into a join handled by `ConstantJoin`. When the input side projects the same
-- identifier twice (`SELECT number, *`), materializing the `CommonSubplanReferenceStep` projected both
-- same-named columns while the reference promised the deduplicated header, so the join's build-side blocks
-- were wider than the join algorithm's header.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT '-- right';
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE or(173, t.number >= 0)) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 0;

SELECT '-- left';
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE or(173, t.number >= 0)) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_use_in_memory_buffer = 0;

SELECT '-- in-memory buffer';
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE or(173, t.number >= 0)) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 1;

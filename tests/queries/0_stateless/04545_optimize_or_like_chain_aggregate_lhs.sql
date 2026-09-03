-- Aggregate/window functions on the LHS of a LIKE chain must not trip the
-- optimize_or_like_chain non-determinism check (previously threw LOGICAL_ERROR
-- "Function node with name '...' is not resolved as ordinary function").
SET optimize_or_like_chain = 1;
SET enable_analyzer = 1;
-- The default thresholds (4 substrings / 10 patterns) are above the branch counts used
-- here, so pin them to 1: without this the chains are kept as written and the queries
-- below pass even if the rewrite never runs.
SET optimize_or_like_chain_min_substrings = 1;
SET optimize_or_like_chain_min_patterns = 1;

SELECT toString(count()) LIKE '%1%' OR toString(count()) LIKE '%2%' FROM numbers(3);
SELECT toString(count()) LIKE '%3%' OR toString(count()) LIKE '%9%' FROM numbers(3);
SELECT toString(sum(number) OVER ()) LIKE '%1%' OR toString(sum(number) OVER ()) LIKE '%2%' FROM numbers(3);
SELECT max(s) LIKE '%a%' OR max(s) LIKE '%z%' FROM (SELECT 'abc' AS s);
SELECT k FROM (SELECT 1 AS k, 'abc' AS s) GROUP BY k HAVING max(s) LIKE '%a%' OR max(s) LIKE '%b%';

-- The rewrite is reached for an aggregate LHS, not merely skipped.
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT toString(count()) LIKE '%1%' OR toString(count()) LIKE '%2%' FROM numbers(3)
) WHERE explain ILIKE '%multiSearchAny%';

-- Ordinary (deterministic) functions still collapse.
SELECT toString(number + 1) LIKE '%1%' OR toString(number + 1) LIKE '%2%' FROM numbers(3);
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT toString(number + 1) LIKE '%1%' OR toString(number + 1) LIKE '%2%' FROM numbers(3)
) WHERE explain ILIKE '%multiSearchAny%';

-- A non-deterministic LHS is still excluded from the rewrite.
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT toString(rand()) LIKE '%1%' OR toString(rand()) LIKE '%2%' FROM numbers(3)
) WHERE explain ILIKE '%multiSearchAny%';

-- Regression test for a LOGICAL_ERROR "Cannot find input column ... on its position in inputs of
-- expression actions DAG" thrown from buildPhysicalJoinImpl. A correlated
-- scalar subquery is decorrelated into a JOIN; when the input side projects the same identifier twice
-- (SELECT number, *) and correlated_subqueries_default_join_kind = 'left' swaps that side onto the left
-- join child, the join's ActionsDAG had a single input for the duplicated name while the child step
-- still produced two. See STID 2409-5283 (AST fuzzer).

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- Minimal case that previously threw: the duplicated column is NOT projected to the
-- output, so the unreferenced duplicate input was pruned from the join DAG while the left child still
-- produced it. Result is discarded; the test only verifies the query no longer throws.
SELECT '-- minimal';
WITH t AS (SELECT number, * FROM numbers(3))
SELECT (SELECT number) FROM t
FORMAT Null
SETTINGS correlated_subqueries_default_join_kind = 'left';

-- The duplicated column flows to the output: both 'number' columns must be preserved and the result
-- must be identical for both decorrelation join kinds.
SELECT '-- left';
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'left';

SELECT '-- right';
WITH t AS (SELECT number, * FROM numbers(3))
SELECT *, (SELECT t.number WHERE t.number >= 0) AS r FROM t
ORDER BY 1
SETTINGS correlated_subqueries_default_join_kind = 'right';

-- The original AST-fuzzer query: previously threw under left decorrelation.
SELECT '-- fuzzer query runs';
WITH RECURSIVE
    alias2 AS (
        SELECT DISTINCT number FROM test_aliases LIMIT 255, -2147483649
        UNION DISTINCT SELECT number FROM test_aliases LIMIT 10, -2
        UNION DISTINCT SELECT number FROM test_aliases LIMIT 548, 1048576
        UNION DISTINCT SELECT (SELECT number) FROM test_aliases WHERE 2147483648 LIMIT -886, 2
    ),
    test_aliases AS (
        SELECT toFixedString('(', 4), number, isNullable(65537), isNullable(65537), *
        FROM numbers(1025) WHERE 1024 QUALIFY isZeroOrNull(NULL)
    )
SELECT DISTINCT number, toFixedString(materialize('('), 65536), isNullable(1)
FROM alias2
FORMAT Null
SETTINGS correlated_subqueries_default_join_kind = 'left';

SELECT '-- ok';

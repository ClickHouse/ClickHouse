-- Regression test for issue #108518.
--
-- tryOptimizeAndCompareChain appends a derived transitive conjunct to an AND
-- with push_back and was not idempotent. The identifier resolve cache can hand
-- the same resolved node to several use sites (projection / HAVING / GROUP BY),
-- and the pass visitor is not deduplicating, so a shared AND accumulated the
-- same conjunct once per visit (2-arg AND -> 4-arg) while a singly-referenced
-- GROUP BY key copy stayed 3-arg. GROUP BY keys are matched against the
-- projection by formatted name, so the two no longer matched and the query
-- failed with NOT_FOUND_COLUMN_IN_BLOCK. The optimization now skips a conjunct
-- already present, so the result is independent of node sharing.

SET enable_analyzer = 1;
SET enable_identifier_resolve_cache = 1;

SELECT '-- compound AND alias in GROUP BY + HAVING (empty result)';
SELECT a FROM numbers(2) GROUP BY ((number >= materialize(5)) AND (0 >= number)) AS a HAVING a;

SELECT '-- compound AND alias that selects a group (returns 1)';
SELECT a FROM numbers(3) GROUP BY ((number >= materialize(0)) AND (5 >= number)) AS a HAVING a;

SELECT '-- compound OR alias in GROUP BY + HAVING';
SELECT a FROM numbers(5) GROUP BY ((number >= materialize(2)) OR (number <= materialize(0))) AS a HAVING a;

SELECT '-- three-way AND alias';
SELECT a FROM numbers(5) GROUP BY ((number >= materialize(1)) AND (4 >= number) AND (number != materialize(2))) AS a HAVING a ORDER BY a;

SELECT '-- alias referenced in projection, GROUP BY, HAVING and ORDER BY';
SELECT a, count() FROM numbers(6) GROUP BY ((number >= materialize(2)) AND (4 >= number)) AS a HAVING a ORDER BY a;

SELECT '-- Nullable column';
SELECT a FROM (SELECT CAST(number AS Nullable(UInt64)) AS n FROM numbers(4)) GROUP BY ((n >= materialize(1)) AND (2 >= n)) AS a HAVING a ORDER BY a;

SELECT '-- compound alias projected twice';
SELECT (number >= materialize(5)) AND (0 >= number) AS a, a FROM numbers(2) ORDER BY a;

SELECT '-- AndCompareChain is idempotent: shared (cache on) and unshared (cache off) trees match';
SELECT (
    SELECT sum(countSubstrings(explain, 'function_name: lessOrEquals'))
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT a FROM numbers(2) GROUP BY ((number >= materialize(5)) AND (0 >= number)) AS a HAVING a
          SETTINGS enable_identifier_resolve_cache = 1)
) = (
    SELECT sum(countSubstrings(explain, 'function_name: lessOrEquals'))
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT a FROM numbers(2) GROUP BY ((number >= materialize(5)) AND (0 >= number)) AS a HAVING a
          SETTINGS enable_identifier_resolve_cache = 0)
);

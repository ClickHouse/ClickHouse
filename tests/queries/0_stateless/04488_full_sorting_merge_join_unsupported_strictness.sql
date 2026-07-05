-- Regression test for a LOGICAL_ERROR (STID 4488-4e11) when full_sorting_merge was
-- selected for a SEMI/ANTI join. full_sorting_merge is executed by MergeJoinAlgorithm,
-- which only implements Any/All/Asof strictness. FullSortingMergeJoin::isSupported did
-- not reject the other strictnesses, so the join was wrongly built and then raised an
-- exception at pipeline build (LOGICAL_ERROR "Join is supported only for pipelines with
-- one output port") or at execution (NOT_IMPLEMENTED "MergeJoinAlgorithm is not
-- implemented for strictness Semi"). Now full_sorting_merge declines such joins, so the
-- planner either falls back to another enabled algorithm or reports it cleanly as a
-- query exception.

SET enable_analyzer = 1;

-- full_sorting_merge is the only enabled algorithm and cannot do SEMI/ANTI. Assert via
-- EXPLAIN: the fix declines at planning time so EXPLAIN already errors, while the unfixed
-- server builds the plan and only errors later at execution (so its EXPLAIN succeeds).
EXPLAIN SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
SEMI LEFT JOIN (SELECT number AS x FROM numbers(5)) AS b ON a.x = b.x
SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError NOT_IMPLEMENTED }

EXPLAIN SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
ANTI LEFT JOIN (SELECT number AS x FROM numbers(5)) AS b ON a.x = b.x
SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError NOT_IMPLEMENTED }

-- With another algorithm enabled, the planner falls back instead of raising an exception.
SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
SEMI LEFT JOIN (SELECT number AS x FROM numbers(5)) AS b ON a.x = b.x
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
ANTI LEFT JOIN (SELECT number AS x FROM numbers(5)) AS b ON a.x = b.x
SETTINGS join_algorithm = 'full_sorting_merge,hash';

-- The original query exception: an EXISTS correlated subquery decorrelates into a SEMI join.
-- With another algorithm enabled it falls back and runs without raising an exception. The
-- full_sorting_merge-only variant is not asserted: its decorrelated plan either errors
-- cleanly or falls back depending on the plan, neither raising an internal error; the
-- explicit SEMI/ANTI cases above already cover the clean-error path deterministically.
SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
WHERE EXISTS (SELECT 1 FROM (SELECT number AS y FROM numbers(5)) AS b WHERE b.y = a.x)
SETTINGS allow_experimental_correlated_subqueries = 1, join_algorithm = 'full_sorting_merge,hash';

-- Supported strictness/kind still uses full_sorting_merge.
SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
INNER JOIN (SELECT number AS x FROM numbers(5)) AS b ON a.x = b.x
SETTINGS join_algorithm = 'full_sorting_merge';

SELECT count() FROM (SELECT number AS x FROM numbers(10)) AS a
LEFT JOIN (SELECT number AS x FROM numbers(5)) AS b ON a.x = b.x
SETTINGS join_algorithm = 'full_sorting_merge';

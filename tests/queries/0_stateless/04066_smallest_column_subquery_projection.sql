-- Tags: no-replicated-database
-- Test that queries reading from subqueries with tuple/array subcolumn projections
-- work correctly when the outer query doesn't reference specific columns.
--
-- When CountDistinctPass rewrites countDistinct(tup.a) into
-- SELECT count() FROM (SELECT tup.a FROM t GROUP BY tup.a),
-- the outer count() needs no subquery columns, so PlannerJoinTree picks
-- the cheapest projection column via getSmallestColumn(). The fix passes
-- skip_subcolumns=false so that subquery projection entries carrying
-- subcolumn metadata (e.g. tup.a from FunctionToSubcolumnsPass) are not
-- incorrectly skipped as if they were storage meta-subcolumns (.size0/.keys).
--
-- Regression test for STID 3938-33a6: server crash (LOGICAL_ERROR: No available columns)
-- when getSmallestColumn() skipped all projection entries in certain builds.

SET allow_experimental_nullable_tuple_type = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_smallest_col;
CREATE TABLE t_smallest_col (tup Tuple(a UInt64, b String), arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_smallest_col VALUES ((1, 'x'), [1, 2]), ((2, 'y'), [3]), ((3, 'z'), [4, 5, 6]);

-- countDistinct on tuple subcolumn: CountDistinctPass rewrites to
-- SELECT count() FROM (SELECT tup.a FROM t GROUP BY tup.a)
-- The outer count() needs no columns from the subquery, triggering
-- getSmallestColumn() on the subquery projection.
SELECT countDistinct(tup.a) FROM t_smallest_col SETTINGS count_distinct_optimization = 1;
SELECT countDistinct(tup.b) FROM t_smallest_col SETTINGS count_distinct_optimization = 1;

-- Same with array size subcolumn
SELECT countDistinct(length(arr)) FROM t_smallest_col SETTINGS count_distinct_optimization = 1;

-- Verify count on Nullable tuple subcolumns works
DROP TABLE IF EXISTS t_smallest_col_nullable;
CREATE TABLE t_smallest_col_nullable (t Nullable(Tuple(u Nullable(UInt32)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_smallest_col_nullable VALUES ((1)), ((NULL)), (NULL);

SELECT count(t.u) FROM t_smallest_col_nullable;
SELECT countDistinct(t.u) FROM t_smallest_col_nullable SETTINGS count_distinct_optimization = 1;

DROP TABLE t_smallest_col;
DROP TABLE t_smallest_col_nullable;

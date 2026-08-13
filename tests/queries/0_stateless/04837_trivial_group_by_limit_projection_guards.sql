-- The trivial `GROUP BY ... LIMIT` optimization must not fire when something between the
-- aggregation and the LIMIT consumes or filters the groups: cutting the aggregation at
-- `LIMIT + OFFSET` keys would then change the result, not merely pick an unspecified
-- subset of the groups. Each query pins the expected result that a premature cutoff
-- would break.

SET optimize_trivial_group_by_limit_query = 1;
SET max_threads = 16;
SET max_block_size = 100;

-- DISTINCT collapses the projected groups: 1000 groups yield 4 distinct values of
-- `intDiv(k, 250)`. With aggregation cut at 3 keys the distinct set would shrink to 1 value.
SELECT count() FROM (SELECT DISTINCT intDiv(k, 250) AS d FROM (SELECT number AS k FROM numbers_mt(1000)) GROUP BY k LIMIT 3);

-- A window function in the projection is evaluated over all groups: `count() OVER ()`
-- must see all 1000 of them. With aggregation cut at 5 keys it would return 5.
SELECT DISTINCT w FROM (SELECT count() OVER () AS w FROM (SELECT number AS k FROM numbers_mt(1000)) GROUP BY k LIMIT 5);

-- `arrayJoin` in the projection can drop rows (empty arrays): the odd half of 1000 groups
-- survives, so LIMIT 10 must still find 10 rows. With aggregation cut at 10 keys only
-- about half of them would produce a row.
SELECT count() FROM (SELECT arrayJoin(if(k % 2 = 1, [k], [])) FROM (SELECT number AS k FROM numbers_mt(1000)) GROUP BY k LIMIT 10);

-- QUALIFY filters the groups after the aggregation: 500 groups pass the filter, so LIMIT 10
-- must find 10 rows. With aggregation cut at 10 keys only the qualifying subset of those 10
-- would remain.
SELECT count() FROM (SELECT k, count() OVER (PARTITION BY k % 2) AS w FROM (SELECT number AS k FROM numbers_mt(1000)) GROUP BY k QUALIFY k >= 500 LIMIT 10);

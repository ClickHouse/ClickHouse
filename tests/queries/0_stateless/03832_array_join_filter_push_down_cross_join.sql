-- The partial filter pushdown optimization (tryToExtractPartialPredicate) could
-- push a filter containing ARRAY_JOIN nodes below a JOIN. removeUnusedActions
-- unconditionally keeps ARRAY_JOIN nodes (they change row count), which pulls
-- in INPUT nodes from the other side of the JOIN that are not available in the
-- target stream, causing a LOGICAL_ERROR:
-- "In Filter cannot be more inputs in the DAG than columns in the input header"

SET enable_analyzer = 1;

-- Original fuzzer-found query
SELECT DISTINCT anyLastDistinct(r.number), l.number, grouping(l.number)
FROM numbers(1) AS l INNER JOIN numbers(2, assumeNotNull(isNull(13))) AS r
ON not(l.number < r.number)
WHERE and(equals(toNullable(2), arrayJoin([13, 13, 13, isZeroOrNull(materialize(13)), *, toLowCardinality(materialize(13)), 13, 13])), 13, 13, 13)
GROUP BY ALL WITH TOTALS;

-- Simplified reproducer: cross join with arrayJoin([*, 1]) in WHERE
SELECT 1 FROM numbers(1) AS l, numbers(2) AS r WHERE and(arrayJoin([*, 1]), 1);

-- arrayJoin referencing both sides in WHERE
SELECT 1 FROM numbers(1) AS l, numbers(2) AS r WHERE arrayJoin([l.number, r.number, 1]);

-- arrayJoin with expression combining both sides
SELECT 1 FROM numbers(1) AS l, numbers(2) AS r WHERE and(arrayJoin([l.number + r.number]), 1);

-- arrayJoin with additional single-side filter
SELECT 1 FROM numbers(1) AS l, numbers(2) AS r WHERE and(arrayJoin([*, 1]), l.number = 0);

-- arrayJoin with product and constant in WHERE
SELECT 1 FROM numbers(1) AS l, numbers(2) AS r WHERE and(arrayJoin([l.number * r.number, 1]), 1, 1);

-- count with arrayJoin across both sides
SELECT count() FROM numbers(1) AS l, numbers(2) AS r WHERE arrayJoin([*, 1]);

-- Three-way cross join with arrayJoin from all sides
SELECT 1 FROM numbers(1) AS l, numbers(1) AS r, numbers(1) AS t WHERE and(arrayJoin([l.number, r.number, t.number, 1]), 1);

-- INNER JOIN with condition and arrayJoin in WHERE
SELECT 1 FROM numbers(1) AS l INNER JOIN numbers(2) AS r ON l.number = r.number WHERE arrayJoin([l.number, r.number]);

-- LEFT JOIN with arrayJoin referencing left side
SELECT 1 FROM numbers(1) AS l LEFT JOIN numbers(2) AS r ON l.number = r.number WHERE arrayJoin([l.number, 1]);

-- Cross join with arrayJoin and single-side filters
SELECT 1 FROM numbers(3) AS l, numbers(3) AS r WHERE and(arrayJoin([l.number, r.number, 1]), l.number > 0) FORMAT Null;
SELECT 1 FROM numbers(3) AS l, numbers(3) AS r WHERE and(arrayJoin([l.number, r.number, 1]), r.number > 0) FORMAT Null;

-- Subquery with arrayJoin from both sides
SELECT sum(x) FROM (SELECT arrayJoin([l.number, r.number]) AS x FROM numbers(2) AS l, numbers(2) AS r WHERE 1);

-- Same tests with legacy join step
SET query_plan_use_logical_join_step = 0;

SELECT 1 FROM numbers(1) AS l, numbers(2) AS r WHERE and(arrayJoin([*, 1]), 1);
SELECT count() FROM numbers(1) AS l, numbers(2) AS r WHERE arrayJoin([*, 1]);

SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_split_filter = 1;
-- The '[split]' step marker is only emitted when this is non-zero.
SET query_plan_max_step_description_length = 500;

-- The split must fire, and the split filter column must not survive in the branch output header.
SELECT countSubstrings(explain, '[split]') > 0 AS split_fired,
       length(JSONExtractArrayRaw(explain, 1, 'Plan', 'Header')) AS branch_header_columns
FROM (EXPLAIN json = 1, header = 1
      SELECT x FROM (SELECT arrayJoin([materialize(1), NULL]) AS x GROUP BY NULL) WHERE NULL);

SELECT 'intersect';
SELECT x FROM (SELECT arrayJoin([materialize(1), NULL]) AS x GROUP BY NULL) WHERE NULL
INTERSECT DISTINCT
SELECT 1;

SELECT 'except';
SELECT x FROM (SELECT arrayJoin([materialize(1), NULL]) AS x GROUP BY NULL) WHERE NULL
EXCEPT DISTINCT
SELECT 1;

SELECT 'union';
SELECT x FROM (SELECT arrayJoin([materialize(1), NULL]) AS x GROUP BY NULL) WHERE NULL
UNION ALL
SELECT 1;

-- The filter column is also an input name here and is consumed downstream, so a mis-resolved
-- name would surface as wrong values or NOT_FOUND_COLUMN_IN_BLOCK, not as a header leak.
SELECT 'values';
SELECT countSubstrings(explain, '[split]') > 0 AS split_fired
FROM (EXPLAIN json = 1, header = 1
      SELECT x, materialize(7) AS k, k + 100 FROM (SELECT arrayJoin([materialize(1), NULL]) AS x GROUP BY materialize(7)) WHERE materialize(7));
SELECT x, materialize(7) AS k, k + 100 FROM (SELECT arrayJoin([materialize(1), NULL]) AS x GROUP BY materialize(7)) WHERE materialize(7) ORDER BY x NULLS LAST;

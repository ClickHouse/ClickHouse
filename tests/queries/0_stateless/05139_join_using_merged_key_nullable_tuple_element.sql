-- The merged USING key column is materialized by converting each source key to the published type,
-- before the join filters anything. That type therefore has to hold every value the source keys hold,
-- including a NULL element that the common subtype dropped because the other side had none there.
-- https://github.com/ClickHouse/ClickHouse/pull/112951

SET enable_analyzer = 1;

SELECT 'A Tuple element that is Nullable on one side only';
SELECT count() FROM (SELECT tuple(if(number = 0, NULL, CAST(1, 'Nullable(Int64)'))) AS t FROM numbers(2)) AS a
INNER JOIN (SELECT tuple(CAST(1, 'UInt64')) AS t) AS b USING (t);
SELECT t, toTypeName(t) FROM (SELECT tuple(if(number = 0, NULL, CAST(1, 'Nullable(Int64)'))) AS t FROM numbers(2)) AS a
INNER JOIN (SELECT tuple(CAST(1, 'UInt64')) AS t) AS b USING (t);

SELECT 'The same join with ON keeps the key of each side';
SELECT a.t, b.t FROM (SELECT tuple(if(number = 0, NULL, CAST(1, 'Nullable(Int64)'))) AS t FROM numbers(2)) AS a
INNER JOIN (SELECT tuple(CAST(1, 'UInt64')) AS t) AS b ON a.t = b.t;

SELECT 'The Nullable element on the right, where the merged column reaches it';
SELECT b.t, toTypeName(b.t) FROM (SELECT tuple(CAST(1, 'UInt64')) AS t) AS a
INNER JOIN (SELECT tuple(if(number = 0, NULL, CAST(1, 'Nullable(Int64)'))) AS t FROM numbers(2)) AS b USING (t);

SELECT 'Only the element that needs it becomes Nullable';
SELECT t, toTypeName(t) FROM (SELECT (if(number = 0, NULL, CAST(1, 'Nullable(Int64)')), CAST(2, 'Int64')) AS t FROM numbers(2)) AS a
INNER JOIN (SELECT (CAST(1, 'UInt64'), CAST(2, 'UInt64')) AS t) AS b USING (t);

SELECT 'SEMI JOIN takes the key of the preserved side';
SELECT t FROM (SELECT tuple(if(number = 0, NULL, CAST(1, 'Nullable(Int64)'))) AS t FROM numbers(2)) AS a
SEMI LEFT JOIN (SELECT tuple(CAST(1, 'UInt64')) AS t) AS b USING (t);

SELECT 'An element out of the common range still matches nothing';
SELECT t FROM (SELECT tuple(CAST(number - 1, 'Int64')) AS t FROM numbers(3)) AS a
INNER JOIN (SELECT tuple(CAST(1, 'UInt64')) AS t) AS b USING (t) ORDER BY ALL;

SELECT 'Nullable on both sides keeps the type the subtype already carries';
SELECT t, toTypeName(t) FROM (SELECT tuple(CAST(1, 'Nullable(UInt64)')) AS t) AS a
INNER JOIN (SELECT tuple(CAST(1, 'Nullable(Int64)')) AS t) AS b USING (t);

-- Tags: stateful
SET optimize_move_to_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_empty_string_comparisons = 0;

SELECT trimBoth(explain)
FROM (
EXPLAIN actions=1
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
FROM test.hits
WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10
SETTINGS allow_reorder_prewhere_conditions = 0
)
WHERE explain like '%Prewhere filter column%';

SELECT trimBoth(explain)
FROM (
EXPLAIN actions=1
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
FROM test.hits
WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10
SETTINGS allow_reorder_prewhere_conditions = 1
)
WHERE explain like '%Prewhere filter column%';

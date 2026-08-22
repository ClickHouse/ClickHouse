DROP TABLE IF EXISTS rcte_pr;

CREATE TABLE rcte_pr (span_id UInt64, parent_span_id UInt64, op String) ENGINE = MergeTree ORDER BY span_id;
INSERT INTO rcte_pr VALUES (1, 0, 'query'), (2, 1, 'query'), (3, 2, 'query'), (4, 3, 'other');

SELECT '-- two reference sites: aliased self-join plus IN, scalar subquery in the outer WHERE';
WITH RECURSIVE d AS
(
    SELECT span_id FROM rcte_pr WHERE parent_span_id = 0 AND op = 'query'
    UNION ALL
    SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
)
SELECT countIf(op = 'query'), countIf(op = 'query' AND span_id IN (SELECT span_id FROM d))
FROM rcte_pr
WHERE span_id = (SELECT any(span_id) FROM rcte_pr WHERE op = 'query')
SETTINGS enable_analyzer = 1,
         allow_experimental_parallel_reading_from_replicas = 1,
         max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'parallel_replicas',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 10;

SELECT '-- the recursion terminates and yields the same rows with and without parallel replicas';
WITH RECURSIVE d AS
(
    SELECT span_id FROM rcte_pr WHERE parent_span_id = 0
    UNION ALL
    SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
)
SELECT span_id FROM d ORDER BY span_id
SETTINGS enable_analyzer = 1,
         allow_experimental_parallel_reading_from_replicas = 1,
         max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'parallel_replicas',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 10;

WITH RECURSIVE d AS
(
    SELECT span_id FROM rcte_pr WHERE parent_span_id = 0
    UNION ALL
    SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
)
SELECT span_id FROM d ORDER BY span_id
SETTINGS enable_analyzer = 1,
         allow_experimental_parallel_reading_from_replicas = 0,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 10;

SELECT '-- a recursive CTE referencing another recursive CTE, see issue 89844';
WITH RECURSIVE
    outer_cte AS (SELECT 1 AS lp UNION ALL SELECT lp + 1 FROM outer_cte WHERE lp < 5),
    inner_cte AS (SELECT 1 AS n UNION ALL SELECT inner_cte.n + outer_cte.lp FROM inner_cte, outer_cte WHERE outer_cte.lp = 1 AND inner_cte.n < 5)
SELECT max(n) FROM inner_cte
SETTINGS enable_analyzer = 1,
         allow_experimental_parallel_reading_from_replicas = 1,
         max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'parallel_replicas',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 20;

SELECT '-- a query over the same table without a recursive CTE is still sent to the replicas';
SELECT count() FROM (
    EXPLAIN
    SELECT span_id FROM rcte_pr
    SETTINGS enable_analyzer = 1,
             allow_experimental_parallel_reading_from_replicas = 1,
             max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             automatic_parallel_replicas_mode = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

DROP TABLE rcte_pr;

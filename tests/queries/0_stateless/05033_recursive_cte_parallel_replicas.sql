-- Recursive CTEs are analyzer-only (the old analyzer rejects them with UNSUPPORTED_METHOD), and
-- `enable_analyzer` may not differ between a subquery and its top-level query, so the analyzer is
-- pinned once for the file rather than per query.
SET enable_analyzer = 1;

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
SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
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
SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
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
SETTINGS allow_experimental_parallel_reading_from_replicas = 0,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 10;

SELECT '-- a recursive CTE referencing another recursive CTE, see issue 89844';
WITH RECURSIVE
    outer_cte AS (SELECT 1 AS lp UNION ALL SELECT lp + 1 FROM outer_cte WHERE lp < 5),
    inner_cte AS (SELECT 1 AS n UNION ALL SELECT inner_cte.n + outer_cte.lp FROM inner_cte, outer_cte WHERE outer_cte.lp = 1 AND inner_cte.n < 5)
SELECT max(n) FROM inner_cte
SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
         max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'parallel_replicas',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 20;

SELECT '-- the recursive CTE stays on the initiator even when only the subquery enables parallel replicas';
SELECT * FROM
(
    WITH RECURSIVE d AS
    (
        SELECT span_id FROM rcte_pr WHERE parent_span_id = 0 AND op = 'query'
        UNION ALL
        SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
    )
    SELECT countIf(op = 'query') AS a, countIf(op = 'query' AND span_id IN (SELECT span_id FROM d)) AS b
    FROM rcte_pr
    WHERE span_id = (SELECT any(span_id) FROM rcte_pr WHERE op = 'query')
    SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
             max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             automatic_parallel_replicas_mode = 0,
             max_recursive_cte_evaluation_depth = 10
)
SETTINGS allow_experimental_parallel_reading_from_replicas = 0;

SELECT '-- a query over the same table without a recursive CTE is still sent to the replicas';
SELECT count() FROM (
    EXPLAIN
    SELECT span_id FROM rcte_pr
    SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
             max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             automatic_parallel_replicas_mode = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- a sibling branch of the same union still ships, whichever branch is visited first';
SELECT count() FROM (
    EXPLAIN
    SELECT * FROM
    (
        (
            WITH RECURSIVE d AS
            (
                SELECT span_id FROM rcte_pr WHERE parent_span_id = 0
                UNION ALL
                SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
            )
            SELECT span_id FROM d
            SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
                     max_parallel_replicas = 3,
                     cluster_for_parallel_replicas = 'parallel_replicas',
                     parallel_replicas_for_non_replicated_merge_tree = 1,
                     automatic_parallel_replicas_mode = 0,
                     max_recursive_cte_evaluation_depth = 10
        )
        UNION ALL
        (
            SELECT span_id FROM rcte_pr
            SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
                     max_parallel_replicas = 3,
                     cluster_for_parallel_replicas = 'parallel_replicas',
                     parallel_replicas_for_non_replicated_merge_tree = 1,
                     automatic_parallel_replicas_mode = 0
        )
    )
    SETTINGS allow_experimental_parallel_reading_from_replicas = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

-- Locality: a query counted by ParallelReplicasQueryCount reached the replica-reading coordinator,
-- so the pair below distinguishes "ran on the initiator" from "did not run at all". Both aggregate
-- over the parts rather than answering from part counters, which never reach the coordinator.
--
-- Each measured query must be executed exactly once, so `ast_fuzzer_runs` is pinned: the stress
-- profile enables the server-side AST fuzzer for any query, and a re-execution inherits
-- `log_comment`. The `argMax` keeps the projection idempotent when the test itself is repeated
-- against one database, which the stress `--database` mode does.
SELECT '-- ParallelReplicasQueryCount: 0 for the recursive CTE, non-zero for the plain query';
SELECT sum(span_id) FROM rcte_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
         max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'parallel_replicas',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_mode = 0,
         ast_fuzzer_runs = 0,
         log_comment = 'rcte_pr_ships';

WITH RECURSIVE d AS
(
    SELECT span_id FROM rcte_pr WHERE parent_span_id = 0
    UNION ALL
    SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
)
SELECT sum(span_id) FROM d
SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
         max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'parallel_replicas',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_mode = 0,
         max_recursive_cte_evaluation_depth = 10,
         ast_fuzzer_runs = 0,
         log_comment = 'rcte_pr_stays_local';

SELECT '-- the same pair with the settings on the subquery instead of the top-level query';
SELECT * FROM
(
    SELECT sum(span_id) AS s FROM rcte_pr
    SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
             max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             automatic_parallel_replicas_mode = 0,
             ast_fuzzer_runs = 0
)
SETTINGS allow_experimental_parallel_reading_from_replicas = 0,
         ast_fuzzer_runs = 0,
         log_comment = 'rcte_pr_nested_ships';

SELECT * FROM
(
    WITH RECURSIVE d AS
    (
        SELECT span_id FROM rcte_pr WHERE parent_span_id = 0
        UNION ALL
        SELECT l.span_id FROM rcte_pr AS l INNER JOIN d AS dd ON l.parent_span_id = dd.span_id
    )
    SELECT sum(span_id) AS s FROM d
    SETTINGS allow_experimental_parallel_reading_from_replicas = 1,
             max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             automatic_parallel_replicas_mode = 0,
             max_recursive_cte_evaluation_depth = 10,
             ast_fuzzer_runs = 0
)
SETTINGS allow_experimental_parallel_reading_from_replicas = 0,
         ast_fuzzer_runs = 0,
         log_comment = 'rcte_pr_nested_stays_local';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment,
       argMax(ProfileEvents['ParallelReplicasQueryCount'], event_time_microseconds) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND event_time >= now() - 600
  AND type = 'QueryFinish'
  AND is_initial_query
  AND log_comment IN ('rcte_pr_ships', 'rcte_pr_stays_local',
                      'rcte_pr_nested_ships', 'rcte_pr_nested_stays_local')
GROUP BY log_comment
ORDER BY log_comment
SETTINGS allow_experimental_parallel_reading_from_replicas = 0,
         ast_fuzzer_runs = 0;

DROP TABLE rcte_pr;

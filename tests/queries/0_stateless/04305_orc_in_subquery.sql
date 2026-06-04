-- Tags: no-fasttest, no-random-settings
-- Tag no-random-settings: depends on ORC push-down defaults (filter_push_down).
-- IN (subquery) on ORC must push down via stripe statistics, same as a literal IN.
SET engine_file_truncate_on_insert = 1;

-- 10k rows, 100 stripes of 100 rows (small row_index_stride for granular pruning).
INSERT INTO FUNCTION file(current_database() || '_100743.orc', ORC, 'id UInt64, payload String')
SELECT number AS id, repeat('x', 16) AS payload
FROM numbers(10000)
ORDER BY id
SETTINGS
    output_format_orc_row_index_stride = 100,
    max_threads = 1;

SELECT count() FROM file(current_database() || '_100743.orc', ORC)
WHERE id IN (50, 5000, 9950)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_orc_literal';

SELECT count() FROM file(current_database() || '_100743.orc', ORC)
WHERE id IN (SELECT arrayJoin([50, 5000, 9950])::UInt64)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_orc_subquery';

WITH keys AS (SELECT arrayJoin([50::UInt64, 5000::UInt64, 9950::UInt64]) AS id)
SELECT count() FROM file(current_database() || '_100743.orc', ORC)
WHERE id IN (SELECT id FROM keys)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_orc_cte';

-- Verify the plan: for ORC, IN-subquery sets must be built eagerly (no CreatingSet step in EXPLAIN).
SELECT 'explain_no_creating_set',
    countIf(explain LIKE '%CreatingSet%') = 0 AS sets_built_eagerly
FROM (EXPLAIN SELECT count() FROM file(current_database() || '_100743.orc', ORC)
      WHERE id IN (SELECT arrayJoin([50])::UInt64));

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    read_rows < 5000 AS pushed_down
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment IN ('100743_orc_literal', '100743_orc_subquery', '100743_orc_cte')
ORDER BY log_comment;

-- Verify that the set skip index is used (and prunes granules) for the
-- `IS TRUE` / `IS FALSE` / `IS UNKNOWN` truth-value predicates and their
-- `IS NOT` variants on a `Nullable(Bool)` column.
-- Follow-up to https://github.com/ClickHouse/ClickHouse/pull/99997

DROP TABLE IF EXISTS bool_set_idx;

CREATE TABLE bool_set_idx
(
    id UInt32,
    b  Nullable(Bool),
    INDEX b_set_idx b TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8;

-- One part with three granules of 8 rows each: only true, only false, only NULL.
INSERT INTO bool_set_idx
SELECT number, multiIf(number < 8, true, number < 16, false, NULL)
FROM numbers(24)
SETTINGS max_insert_threads = 1;

OPTIMIZE TABLE bool_set_idx FINAL;

SELECT 'count'          AS predicate, count() FROM bool_set_idx                              SETTINGS enable_parallel_replicas = 0;
SELECT 'IS TRUE'        AS predicate, count() FROM bool_set_idx WHERE b IS TRUE              SETTINGS enable_parallel_replicas = 0, log_comment = '04290 IS TRUE';
SELECT 'IS FALSE'       AS predicate, count() FROM bool_set_idx WHERE b IS FALSE             SETTINGS enable_parallel_replicas = 0, log_comment = '04290 IS FALSE';
SELECT 'IS UNKNOWN'     AS predicate, count() FROM bool_set_idx WHERE b IS UNKNOWN           SETTINGS enable_parallel_replicas = 0, log_comment = '04290 IS UNKNOWN';
SELECT 'IS NOT TRUE'    AS predicate, count() FROM bool_set_idx WHERE b IS NOT TRUE          SETTINGS enable_parallel_replicas = 0, log_comment = '04290 IS NOT TRUE';
SELECT 'IS NOT FALSE'   AS predicate, count() FROM bool_set_idx WHERE b IS NOT FALSE         SETTINGS enable_parallel_replicas = 0, log_comment = '04290 IS NOT FALSE';
SELECT 'IS NOT UNKNOWN' AS predicate, count() FROM bool_set_idx WHERE b IS NOT UNKNOWN       SETTINGS enable_parallel_replicas = 0, log_comment = '04290 IS NOT UNKNOWN';

SYSTEM FLUSH LOGS query_log;

-- `SelectedMarks` counts the granules read after skip-index pruning;
-- `SelectedMarksTotal` counts the granules considered before pruning.
-- The set index should drop the two unrelated granules for every positive
-- form and the matching granule for every negative form.
SELECT
    splitByString('04290 ', log_comment)[2]  AS predicate,
    ProfileEvents['SelectedMarks']           AS granules_read,
    ProfileEvents['SelectedMarksTotal']      AS granules_total
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE '04290 %'
  AND type = 'QueryFinish'
ORDER BY predicate;

DROP TABLE bool_set_idx;

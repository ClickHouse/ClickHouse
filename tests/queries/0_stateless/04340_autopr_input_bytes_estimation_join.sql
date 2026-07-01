-- Input-byte autopr statistics for JOIN queries. A JoinStep never reads itself, but the JOIN-specific
-- logic in `findReadingStep` decides WHICH reading step gets instrumented: the parallelized side
-- (child 0, or child 1 for RIGHT), i.e. the table read across replicas. This test checks that the
-- collected `RuntimeDataflowStatisticsInputBytes` approximates the bytes actually read from that table.
-- The other (broadcast) side is kept tiny, so `ReadCompressedBytes` is dominated by the parallelized
-- table and the two are directly comparable. If the wrong side were instrumented, `input_bytes` would
-- reflect the tiny broadcast table and diverge from `ReadCompressedBytes` by far more than the
-- allowed factor -- this test flags that.
--
-- Self-contained (no stateful dataset) and light enough for the sanitizer matrix.

-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;

-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table='false';

-- Reading aggregation states / external sort spills from disk would inflate `ReadCompressedBytes`.
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET max_bytes_before_external_sort=0, max_bytes_ratio_before_external_sort=0;

SET max_threads=0;
SET use_uncompressed_cache=0;
SET use_query_condition_cache=0;

DROP TABLE IF EXISTS ij_big;
DROP TABLE IF EXISTS ij_small;

-- Big (parallelized) side: 1M rows with a varied, poorly-compressible payload, read in full.
CREATE TABLE ij_big (key UInt64, payload String) ENGINE = MergeTree ORDER BY key
AS SELECT number, toString(cityHash64(number)) FROM numbers(1000000);

-- Tiny broadcast side, so `ReadCompressedBytes` ~ the bytes read from `ij_big`.
CREATE TABLE ij_small (key UInt64) ENGINE = MergeTree ORDER BY key
AS SELECT number FROM numbers(200000);

-- INNER JOIN: the big left side is parallelized; selecting its payload makes the read substantial.
SELECT t1.payload FROM ij_big AS t1 INNER JOIN ij_small AS t2 USING (key) FORMAT Null SETTINGS log_comment='04340_join_inner';

-- LEFT JOIN: the big left side is parallelized.
SELECT t1.payload FROM ij_big AS t1 LEFT JOIN ij_small AS t2 USING (key) FORMAT Null SETTINGS log_comment='04340_join_left';

-- RIGHT JOIN: the big table is on the right (the parallelized side), exercising the RIGHT-join branch
-- (child 1) of `findReadingStep`.
SELECT t2.payload FROM ij_small AS t1 RIGHT JOIN ij_big AS t2 USING (key) FORMAT Null SETTINGS log_comment='04340_join_right';

DROP TABLE ij_big;
DROP TABLE ij_small;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Fail if the input-byte estimate is missing (0, i.e. no/wrong reading step instrumented) or deviates
-- from the actual `ReadCompressedBytes` (dominated by the big parallelized table) by more than 2x.
SELECT format('{} {} {}', log_comment, input_bytes, compressed_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes,
        ProfileEvents['ReadCompressedBytes'] AS compressed_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE '04340_join_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE input_bytes = 0
   OR greatest(input_bytes, compressed_bytes) / nullIf(least(input_bytes, compressed_bytes), 0) > 2;

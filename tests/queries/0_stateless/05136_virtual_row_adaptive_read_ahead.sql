-- Exercise coalesced announcements, buffered data at end of input, and cancellation.
SET max_threads = 4, max_block_size = 128;
SET optimize_read_in_order = 1, read_in_order_use_buffering = 1, max_parallel_replicas = 1;
SET read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1;
SET read_in_order_virtual_row_prefetch_window = 2, read_in_order_two_level_merge_threshold = 1000000;
SET optimize_move_to_prewhere = 0, use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;
SET use_skip_indexes = 0, log_queries = 1;

CREATE TABLE vrow_adaptive (k Nullable(Int64), tie UInt8, probe Int64)
ENGINE = MergeTree ORDER BY (k, tie)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, allow_nullable_key = 1,
         ratio_of_defaults_for_sparse_serialization = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES vrow_adaptive;

-- Eight overlapping parts, equal first keys, and a nullable last key.
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 0, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 1, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 2, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 3, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 4, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 5, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 6, number FROM numbers(4096);
INSERT INTO vrow_adaptive SELECT if(number = 4095, NULL, toNullable(toInt64(number))), 7, number FROM numbers(4096);

-- Exercise sparse data keys against the index-derived announcements.
SELECT countIf(serialization_kind = 'Sparse') > 0
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'vrow_adaptive' AND active AND column IN ('k', 'tie');

-- Demand alone can answer the first portion.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(0, 0), (0, 1), (0, 2), (0, 3), (0, 4)]
FROM (SELECT k, tie FROM vrow_adaptive ORDER BY k, tie LIMIT 5);

-- A long filtered prefix, then small surviving chunks.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5);

-- A zero window must still satisfy demand after crossing a filtered prefix.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5)
SETTINGS read_in_order_virtual_row_prefetch_window = 0;

-- Without per-block announcements, surviving data and EOF still provide feedback.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5)
SETTINGS read_in_order_use_virtual_row_per_block = 0;

-- Mandatory reads must proceed even with the minimal buffering budget.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5)
SETTINGS read_in_order_use_buffering = 0;

-- A window larger than the number of lanes is valid.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5)
SETTINGS read_in_order_virtual_row_prefetch_window = 64;

-- Preliminary merges are also lanes and may finish with buffered data.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5)
SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 2;

-- `PREWHERE` can discard the source block before downstream transforms see it.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(2147, 7), (2260, 7), (2373, 7), (2486, 7), (2599, 7)]
FROM (SELECT k, tie FROM vrow_adaptive PREWHERE probe >= 2048 AND probe % 113 = 0 AND tie = 7 ORDER BY k, tie LIMIT 5);

-- Without `LIMIT`, drain every queued result before finishing the merge.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(0, 0), (0, 3), (0, 6), (1024, 0), (1024, 3), (1024, 6), (2048, 0), (2048, 3), (2048, 6), (3072, 0), (3072, 3), (3072, 6)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe % 1024 = 0 AND tie % 3 = 0 ORDER BY k, tie);

-- Exhausted readers retain pending real rows and announcements in order.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(4094, 0), (4094, 1), (4094, 2), (4094, 3), (4094, 4), (4094, 5), (4094, 6), (4094, 7), (-1, 0), (-1, 1), (-1, 2), (-1, 3)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 4094 ORDER BY k, tie LIMIT 12);

-- Reverse comparisons preserve equal keys and null ordering.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(-1, 7), (-1, 6), (-1, 5), (-1, 4), (-1, 3), (-1, 2), (-1, 1), (-1, 0), (4094, 7), (4094, 6), (4094, 5), (4094, 4)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe >= 4094 ORDER BY k DESC NULLS FIRST, tie DESC LIMIT 12);

-- A fully filtered scan must release reader slots until every source finishes.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = []
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe > 4095 ORDER BY k, tie LIMIT 5);

-- Fewer surviving rows than the limit must drain before finishing.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(4094, 7)]
FROM (SELECT k, tie FROM vrow_adaptive WHERE probe = 4094 AND tie = 7 ORDER BY k, tie LIMIT 5);

-- Virtual-row conversions must not mutate the metadata delivered to the merge.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(1, 0), (1, 3), (1, 6), (1025, 0), (1025, 3), (1025, 6), (2049, 0), (2049, 3), (2049, 6), (3073, 0)]
FROM (SELECT k + 1 AS k, tie FROM vrow_adaptive WHERE probe % 1024 = 0 AND tie % 3 = 0 ORDER BY k, tie LIMIT 10);

-- Reverse virtual-row conversions are compared in query order.
SELECT groupArray(tuple(ifNull(k, -1), tie)) = [(-3072, 6), (-3072, 3), (-3072, 0), (-2048, 6), (-2048, 3), (-2048, 0), (-1024, 6), (-1024, 3), (-1024, 0), (0, 6)]
FROM (SELECT -k AS k, tie FROM vrow_adaptive WHERE probe % 1024 = 0 AND tie % 3 = 0 ORDER BY k, tie DESC LIMIT 10);

DROP TABLE vrow_adaptive;

-- With disjoint parts an immediate result must not start deferred readers.
CREATE TABLE vrow_adaptive_lazy (k UInt64, probe UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES vrow_adaptive_lazy;
INSERT INTO vrow_adaptive_lazy SELECT number + 0 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 1 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 2 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 3 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 4 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 5 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 6 * 4096, number FROM numbers(4096);
INSERT INTO vrow_adaptive_lazy SELECT number + 7 * 4096, number FROM numbers(4096);

SELECT k FROM vrow_adaptive_lazy WHERE probe >= 0 ORDER BY k LIMIT 1
SETTINGS log_comment = '05136_adaptive_lazy';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows <= 256
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05136_adaptive_lazy' AND event_date >= today() - 1
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE vrow_adaptive_lazy;

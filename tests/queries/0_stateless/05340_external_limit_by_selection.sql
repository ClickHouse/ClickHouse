-- The threshold is compared with the memory tracked by the query, so allocations must not linger in the
-- thread-local untracked buffers. At a 1-byte threshold a spilling `LIMIT BY` spills from its first chunk.
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;
-- Parallel replicas would split the `MergeTree` reads below and change their plans.
SET enable_parallel_replicas = 0;
-- An enabled adaptive memory scheduler selects the spilling transform even without a threshold.
SET enable_adaptive_memory_spill_scheduler = 0;

-- The spilling transform replaces the in-memory one only when a threshold is set or a memory scheduler can ask it
-- to spill. A ratio must be at least 0 and less than 1.
SELECT count() FROM (EXPLAIN PIPELINE SELECT number % 10 AS k FROM numbers(100) LIMIT 2 BY k SETTINGS max_bytes_before_external_limit_by = 0) WHERE explain LIKE '%ExternalLimitByTransform%';
SET enable_adaptive_memory_spill_scheduler = 1;
SELECT count() FROM (EXPLAIN PIPELINE SELECT number % 10 AS k FROM numbers(100) LIMIT 2 BY k SETTINGS max_bytes_before_external_limit_by = 0) WHERE explain LIKE '%ExternalLimitByTransform%';
SET enable_adaptive_memory_spill_scheduler = 0;
SELECT count() FROM (EXPLAIN PIPELINE SELECT number % 10 AS k FROM numbers(100) LIMIT 2 BY k) WHERE explain LIKE '%ExternalLimitByTransform%';
SELECT count() FROM (SELECT number % 10 AS k FROM numbers(100) LIMIT 2 BY k) SETTINGS max_bytes_ratio_before_external_limit_by = 1; -- { serverError BAD_ARGUMENTS }

-- A single stream sorted by the keys goes to `LimitBySortedStreamTransform`, which keeps one group at a time.
-- Constant keys put all rows into one group, and `LIMIT 0 BY` keeps no rows whatever its offset. None of them
-- needs state that could spill.
SELECT countIf(explain LIKE '%LimitBySortedStreamTransform%') > 0, countIf(explain LIKE '%ExternalLimitByTransform%') FROM (EXPLAIN PIPELINE SELECT number % 10 AS k, number AS v FROM numbers(30) ORDER BY k, v DESC LIMIT 1 OFFSET 1 BY k);
SELECT count() FROM (EXPLAIN PIPELINE SELECT number, 1 AS c FROM numbers(10) LIMIT 2 BY c) WHERE explain LIKE '%ExternalLimitByTransform%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT number % 10 AS k FROM numbers(100) LIMIT 0 OFFSET 5 BY k) WHERE explain LIKE '%ExternalLimitByTransform%';

-- A table partitioned by a function of the `LIMIT BY` key. With `allow_limit_by_partitions_independently`, no
-- group spans two streams, so every stream gets its own spilling `LIMIT BY`. Without it, the streams sorted
-- by the key are prefiltered in order and then merged into a single spilling `LIMIT BY`. Both keep as many
-- rows of each key as the in-memory `LIMIT BY`.
CREATE TABLE limit_by_partitions (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 4;
INSERT INTO limit_by_partitions SELECT number % 1000, number FROM numbers(10000);

SELECT countIf(explain LIKE '%ExternalLimitByTransform × %') FROM (EXPLAIN PIPELINE SELECT k, v FROM limit_by_partitions LIMIT 2 OFFSET 1 BY k SETTINGS allow_limit_by_partitions_independently = 1, optimize_limit_by_in_order = 0, max_threads = 4);
SELECT countIf(trimLeft(explain) = 'ExternalLimitByTransform'), countIf(explain LIKE '%LimitBySortedStreamTransform × %') FROM (EXPLAIN PIPELINE SELECT k, v FROM limit_by_partitions LIMIT 2 OFFSET 1 BY k SETTINGS allow_limit_by_partitions_independently = 0, optimize_limit_by_in_order = 1, read_in_order_two_level_merge_threshold = 100, max_threads = 4);

SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT k, v FROM limit_by_partitions LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 0;
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT k, v FROM limit_by_partitions LIMIT 2 OFFSET 1 BY k) SETTINGS allow_limit_by_partitions_independently = 1, optimize_limit_by_in_order = 0, max_threads = 4, log_comment = '05340_external_limit_by_selection/partitions';
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT k, v FROM limit_by_partitions LIMIT 2 OFFSET 1 BY k) SETTINGS allow_limit_by_partitions_independently = 0, optimize_limit_by_in_order = 1, read_in_order_two_level_merge_threshold = 100, max_threads = 4, log_comment = '05340_external_limit_by_selection/sorted_streams';

DROP TABLE limit_by_partitions;

-- Keys that the hash table stores serialized, such as `AggregateFunction` states, are compared through
-- 128-bit fingerprints in the spill runs, so keys without a value order spill too. Small chunks repeat the
-- keys across input and spill boundaries. The spilling query keeps as many rows of each key as the in-memory
-- one before it.
SET max_block_size = 2;

SELECT count(), arraySort(groupArray(finalizeAggregation(k))), uniqExact(number) = count() FROM (SELECT initializeAggregation('sumState', toUInt64(number % 3)) AS k, number FROM numbers(20) LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 0;
SELECT count(), arraySort(groupArray(finalizeAggregation(k))), uniqExact(number) = count() FROM (SELECT initializeAggregation('sumState', toUInt64(number % 3)) AS k, number FROM numbers(20) LIMIT 2 OFFSET 1 BY k) SETTINGS log_comment = '05340_external_limit_by_selection/state_window';

-- Hash-backed aggregate states can change their serialization when they are reconstructed. The fingerprints
-- come from the original rows, so repeated states match their spilled groups, also inside composite keys.
-- With `ORDER BY` the payload states come back with their keys in the sorted order. The expected rows of the
-- ordered query are those of the in-memory `LIMIT BY`.
CREATE VIEW limit_by_state_identity AS
SELECT
    number,
    number % 2 AS id,
    initializeAggregation('uniqExactArrayState', arrayMap(x -> cityHash64(x + id * 1000000), range(63))) AS s
FROM numbers(8);

SELECT count(), arraySort(groupArray(id)), uniqExactArrayMerge(s), uniqExact(number) = count() FROM (SELECT if(id = 0, NULL, repeat('x', id)) AS prefix, s, range(id) AS suffix, id, number FROM limit_by_state_identity LIMIT 2 OFFSET 1 BY prefix, s, suffix) SETTINGS max_bytes_before_external_limit_by = 0;
SELECT count(), arraySort(groupArray(id)), uniqExactArrayMerge(s), uniqExact(number) = count() FROM (SELECT if(id = 0, NULL, repeat('x', id)) AS prefix, s, range(id) AS suffix, id, number FROM limit_by_state_identity LIMIT 2 OFFSET 1 BY prefix, s, suffix) SETTINGS log_comment = '05340_external_limit_by_selection/identity_window';
SELECT groupArray(number), uniqExactArrayMerge(s) FROM (SELECT s, number FROM limit_by_state_identity ORDER BY number DESC LIMIT 2 OFFSET 1 BY s) SETTINGS log_comment = '05340_external_limit_by_selection/identity_ordered_window';

DROP VIEW limit_by_state_identity;

-- The `LIMIT BY` of a query with `ORDER BY` records the order requirement; without `ORDER BY` there is none.
SELECT count() FROM (EXPLAIN PLAN actions = 1 SELECT number % 10 AS k, number AS v FROM numbers(100) ORDER BY v DESC LIMIT 2 BY k) WHERE explain LIKE '%Preserve input order%';
SELECT count() FROM (EXPLAIN PLAN actions = 1 SELECT number % 10 AS k, number AS v FROM numbers(100) LIMIT 2 BY k) WHERE explain LIKE '%Preserve input order%';

-- The optimizer propagates the order of a sorted subquery through the outer `LIMIT BY` and records the order
-- requirement on it. The spilling `LIMIT BY` then returns the second row of each key in the order of the
-- subquery, as the in-memory one does. `query_plan_remove_redundant_sorting = 0` makes the check independent
-- of the removal of sorting steps.
SELECT count() FROM (EXPLAIN PLAN actions = 1 SELECT k, v FROM (SELECT number % 5 AS k, number AS v FROM numbers(20) ORDER BY v DESC) LIMIT 1 OFFSET 1 BY k SETTINGS query_plan_remove_redundant_sorting = 0) WHERE explain LIKE '%Preserve input order%';
SELECT k, v FROM (SELECT number % 5 AS k, number AS v FROM numbers(20) ORDER BY v DESC) LIMIT 1 OFFSET 1 BY k SETTINGS query_plan_remove_redundant_sorting = 0, log_comment = '05340_external_limit_by_selection/apply_order';

-- Spill columns stay separate from user columns with the same names. The generic key adds a fingerprint,
-- every run adds the seen count, and the order requirement adds an arrival number. The user column named like
-- the arrival number descends where the arrival numbers ascend, so restoring the order by the wrong column
-- reverses it. A second user column takes the next name that the arrival number could fall back to. The
-- expected rows are those of the in-memory `LIMIT BY`.
SELECT [number % 3] AS __key_fingerprint, number % 3 AS __seen_rows, number AS __arrival_number, number AS ___arrival_number FROM numbers(12) ORDER BY __arrival_number DESC LIMIT 2 BY __key_fingerprint, __seen_rows SETTINGS log_comment = '05340_external_limit_by_selection/service_names';

-- Larger blocks keep the scan of the query log fast.
SET max_block_size = DEFAULT;

-- Every query above with a `log_comment` spilled and merged its runs.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05340_external_limit_by_selection/') + 1) AS test_case,
    min(ProfileEvents['ExternalLimitByWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalLimitByMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05340_external_limit_by_selection/')
GROUP BY log_comment
ORDER BY test_case;

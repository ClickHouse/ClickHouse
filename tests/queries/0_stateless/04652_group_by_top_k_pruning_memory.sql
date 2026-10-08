-- The `GROUP BY` top-K heap promises that memory stays proportional to the
-- `LIMIT` rather than to the number of distinct keys.  Both queries below use a
-- strictly descending key stream under `ORDER BY k ASC`, which admits every new
-- key and evicts an older one, so nothing is ever skipped and the whole memory
-- saving has to come from pruning the hash table and recycling the evicted
-- aggregate states.  `ArenaAllocBytes` is asserted instead of `memory_usage`
-- because it measures the aggregate-state arena directly and is not perturbed
-- by block sizes or by the sanitizer builds' allocation overhead.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET enable_group_by_top_k_optimization = 1;
-- The optimization declines any aggregation carrying a `GROUP BY` row limit, and
-- the stateless test profile sets `max_rows_to_group_by` to a huge-but-non-zero
-- 10G (`tests/config/users.d/limits.yaml`), which is enough to gate it off.
SET max_rows_to_group_by = 0;
-- The cap is randomized by clickhouse-test; 0 disables it so the heap engages.
SET query_plan_max_limit_for_top_k_optimization = 0;
-- Do not let the profitability freeze drop the heap: these streams never skip a
-- row, so an unfrozen heap is exactly what is under test.
SET group_by_top_k_optimization_observation_rows = 0;
SET max_threads = 1;

SELECT 'evicted states are reused within one block';

-- One single block, so every admission and eviction happens inside the same
-- `executeImplBatch` call.  A slot reclaimed by a trim has to be handed back out
-- immediately; if reuse only started with the next block, this would allocate
-- one arena slot per admitted key.  `uniqExact` is used because its state really
-- lives in the arena (~64 B/group, 32 MiB unpruned here), whereas `sum` states
-- are small enough to leave too little margin to assert on.
SELECT k, uniqExact(v)
FROM (SELECT 500000 - number AS k, number % 7 AS v FROM numbers(500000))
GROUP BY k
ORDER BY k ASC
LIMIT 10
FORMAT Null
SETTINGS max_block_size = 500000, log_comment = '04652_intra_block_reuse';

SYSTEM FLUSH LOGS query_log;

-- Prove the eviction path ran, then that it cost almost no arena.
SELECT
    sum(ProfileEvents['AggregationTopKKeysEvicted']) > 400000 AS evicted,
    sum(ProfileEvents['ArenaAllocBytes']) < 4000000 AS arena_bounded
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04652_intra_block_reuse'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();

SELECT 'LowCardinality keys prune the hash table';

-- `LowCardinality` used to run the heap in skip-only mode: rows could be
-- skipped, but evicted groups stayed in the hash table and their states were
-- never destroyed, so memory still scaled with the distinct key count.  With
-- nothing skippable here, an unpruned table would keep all 400k groups.
SELECT k, uniqExact(v)
FROM (SELECT toLowCardinality(400000 - number) AS k, number % 7 AS v FROM numbers(400000))
GROUP BY k
ORDER BY k ASC
LIMIT 10
FORMAT Null
SETTINGS log_comment = '04652_low_cardinality_pruning';

SYSTEM FLUSH LOGS query_log;

-- `AggregationTopKKeysEvicted` counts heap evictions whether or not the hash table
-- could follow, so it cannot tell pruning from skip-only mode;
-- `AggregationTopKKeysPruned` counts only the keys actually erased, with their
-- aggregate states destroyed, which is the property under test.  Asserting on it
-- rather than on a memory or table-shape proxy keeps this immune to the settings
-- clickhouse-test randomizes.
SELECT
    sum(ProfileEvents['AggregationTopKKeysEvicted']) > 300000 AS evicted,
    sum(ProfileEvents['AggregationTopKRowsSkipped']) = 0 AS nothing_skipped,
    sum(ProfileEvents['AggregationTopKKeysPruned']) > 300000 AS table_pruned
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04652_low_cardinality_pruning'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();

SELECT 'results survive the churn';

-- Pruning must not change the answer.  Every group here holds several rows, so a
-- group whose state was destroyed and whose slot was reissued mid-block would
-- come back with rows missing rather than absent altogether.
SELECT k, count(), sum(v), uniqExact(v), groupArray(v)
FROM (SELECT toLowCardinality(100000 - intDiv(number, 3)) AS k, number % 11 AS v FROM numbers(300000))
GROUP BY k
ORDER BY k ASC
LIMIT 5;

SELECT k, count(), sum(v), groupArray(v)
FROM (SELECT 100000 - intDiv(number, 3) AS k, number % 11 AS v FROM numbers(300000))
GROUP BY k
ORDER BY k ASC
LIMIT 5;

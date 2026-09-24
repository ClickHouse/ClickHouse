-- Tags: no-parallel-replicas
-- no-parallel-replicas: both optimizations below stay off when the aggregation is split
-- between the replicas and the initiator (results would still be correct).

-- The `GROUP BY` top-K heap (`enable_group_by_top_k_optimization`) and the kept-keys cutoff
-- of the trivial `GROUP BY ... LIMIT` optimization target the same shape and exclude each
-- other (the heap bails out on `max_rows_to_group_by > 0`). The planner arbitrates by the
-- key types: fixed-width keys take the cutoff, which is several times faster there, while
-- `String`-like keys are left to the heap, which is faster on them. The cutoff also stays
-- in charge when the heap does not apply (disabled, or the LIMIT above its cap).

SET enable_analyzer = 1;
SET optimize_trivial_group_by_limit_query = 1;
SET enable_group_by_top_k_optimization = 1;
-- The heap does not apply above this cap (CI randomizes it down to 1) or to serialized plans.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET serialize_query_plan = 0;
-- The heap does not apply with a `max_rows_to_group_by` set (CI's profile sets it to 10G).
SET max_rows_to_group_by = 0;
SET max_threads = 4;
SET max_block_size = 8192;

-- `lower` is not injective, so the analyzer keeps the `String` key (`GROUP BY toString(x)`
-- would be rewritten to `GROUP BY x`, a fixed-width key).

SELECT 'string key: top-K heap';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN actions = 1
    SELECT lower(toString(number)) AS k, count() FROM numbers_mt(100000) GROUP BY k LIMIT 5)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

SELECT 'low cardinality string key: top-K heap';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN actions = 1
    SELECT toLowCardinality(lower(toString(number))) AS k, count() FROM numbers_mt(100000) GROUP BY k LIMIT 5)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

SELECT 'mixed keys with a string: top-K heap';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN actions = 1
    SELECT toUInt64(number) AS k1, lower(toString(number % 10)) AS k2, count() FROM numbers_mt(100000) GROUP BY k1, k2 LIMIT 5)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

SELECT 'fixed-width keys: kept-keys cutoff, no top-K';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT toUInt64(number) AS k1, toNullable(toDate(number % 1000)) AS k2, count() FROM numbers_mt(100000) GROUP BY k1, k2 LIMIT 5)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

SELECT 'string key with the heap disabled: kept-keys cutoff';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT lower(toString(number)) AS k, count() FROM numbers_mt(100000) GROUP BY k LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 0)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

SELECT 'string key with max_rows_to_group_by set: kept-keys cutoff';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT lower(toString(number)) AS k, count() FROM numbers_mt(100000) GROUP BY k LIMIT 5
    SETTINGS max_rows_to_group_by = 10000000000)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

-- The cutoff is observable at runtime: the aggregator caps the keys (`OverflowAny`).
SELECT toUInt64(number) AS k, count() FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
    SETTINGS enable_parallel_replicas = 0, log_comment = '05224_fixed_width_key';
SELECT lower(toString(number)) AS k, count() FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
    SETTINGS enable_parallel_replicas = 0, log_comment = '05224_string_key';
SELECT lower(toString(number)) AS k, count() FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
    SETTINGS enable_parallel_replicas = 0, enable_group_by_top_k_optimization = 0, log_comment = '05224_string_key_heap_off';
SELECT lower(toString(number)) AS k, count() FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
    SETTINGS enable_parallel_replicas = 0, max_rows_to_group_by = 10000000000, log_comment = '05224_string_key_max_rows_set';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['OverflowAny'] > 0 AS cutoff_fired,
    ProfileEvents['AggregationTopKRowsSkipped'] > 0 AS top_k_fired
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05224_fixed_width_key', '05224_string_key', '05224_string_key_heap_off', '05224_string_key_max_rows_set')
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;

-- A `set` skip index on an expression when the query spells the same expression with a wider
-- operand type: the index must not prune with the mismatched type and the query must still work.

DROP TABLE IF EXISTS t_widened_operand;

CREATE TABLE t_widened_operand (t UInt32, INDEX t_set t % 19 TYPE set(4) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO t_widened_operand SELECT number FROM numbers(100) SETTINGS max_insert_threads = 1;

SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0,
    secondary_indices_enable_bulk_filtering = 1, enable_parallel_replicas = 0;

-- `t % toInt64(19)` renders as `t MOD 19`, the same name as the index expression, but its result
-- type is UInt64 while the granule holds UInt8.
SELECT 'index_consulted', countIf(explain LIKE '%Name: t_set%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_widened_operand WHERE t % toInt64(19) = 16);

SELECT 'granules_plain', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_widened_operand WHERE t % 19 = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'granules_widened', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_widened_operand WHERE t % toInt64(19) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'granules_widened_nullable', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_widened_operand WHERE t % (SELECT toInt64(19)) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'plain', count() FROM t_widened_operand WHERE t % 19 = 16;
SELECT 'widened', count() FROM t_widened_operand WHERE t % toInt64(19) = 16;
SELECT 'widened_nullable', count() FROM t_widened_operand WHERE t % (SELECT toInt64(19)) = 16;
SELECT 'no_skip_indexes', count() FROM t_widened_operand WHERE t % toInt64(19) = 16 SETTINGS use_skip_indexes = 0;

SET secondary_indices_enable_bulk_filtering = 0;
SELECT 'nobulk_widened', count() FROM t_widened_operand WHERE t % toInt64(19) = 16;
SELECT 'nobulk_widened_nullable', count() FROM t_widened_operand WHERE t % (SELECT toInt64(19)) = 16;

DROP TABLE t_widened_operand;

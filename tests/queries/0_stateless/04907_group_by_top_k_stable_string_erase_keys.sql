-- Top-K keeps hash-table erase keys across source blocks. Exercise both the
-- legacy inline-string table and the arena-backed `PackedStringRef` method;
-- the 20-byte keys select the pointer-bearing packed representation while
-- remaining inline in `StringHashTable`.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;
-- The CI config (`users.d/limits.yaml`) sets a non-zero `max_rows_to_group_by`,
-- which disables the top-K optimization outright; pin it off.
SET max_rows_to_group_by = 0;
SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_trivial_group_by_limit_query = 0;
SET optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0;
SET max_threads = 1;
SET max_block_size = 1024;
SET log_queries = 1;

SELECT 'legacy string keys survive cross-block eviction';
SELECT
    arraySort(groupArray(k)) = arrayMap(n -> leftPad(toString(n), 20, '0'), range(199990, 200000)),
    sum(c) = 10
FROM
(
    SELECT leftPad(toString(number), 20, '0') AS k, count() AS c
    FROM numbers(200000)
    GROUP BY k
    ORDER BY k DESC
    LIMIT 10
    SETTINGS enable_packed_string_keys_in_aggregation = 0
)
SETTINGS log_comment = '04907_legacy';

SELECT 'packed string keys survive cross-block eviction';
SELECT
    arraySort(groupArray(k)) = arrayMap(n -> leftPad(toString(n), 20, '0'), range(199990, 200000)),
    sum(c) = 10
FROM
(
    SELECT leftPad(toString(number), 20, '0') AS k, count() AS c
    FROM numbers(200000)
    GROUP BY k
    ORDER BY k DESC
    LIMIT 10
    SETTINGS enable_packed_string_keys_in_aggregation = 1
)
SETTINGS log_comment = '04907_packed';

-- Multiple keys use the serialized-key method, whose key is another
-- arena-backed string view retained by the heap for later erase.
SELECT 'serialized keys survive cross-block eviction';
SELECT
    arraySort(groupArray(k)) = arrayMap(n -> leftPad(toString(n), 20, '0'), range(199990, 200000)),
    sum(c) = 10
FROM
(
    SELECT leftPad(toString(number), 20, '0') AS k, number % 3 AS suffix, count() AS c
    FROM numbers(200000)
    GROUP BY k, suffix
    ORDER BY k DESC, suffix DESC
    LIMIT 10
)
SETTINGS log_comment = '04907_serialized';

SYSTEM FLUSH LOGS query_log;

-- Positive proof that the heap actually ran (and evicted) for every method
-- family above: correct final rows alone would stay green even if a later
-- gate silently disabled top-K and stopped exercising the erase path.
SELECT 'each family evicted through the heap';
SELECT log_comment, max(ProfileEvents['AggregationTopKKeysEvicted']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment IN ('04907_legacy', '04907_packed', '04907_serialized')
GROUP BY log_comment
ORDER BY log_comment;

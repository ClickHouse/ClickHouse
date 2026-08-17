-- Top-K keeps hash-table erase keys across source blocks. Exercise both the
-- legacy inline-string table and the arena-backed `PackedStringRef` method;
-- the 20-byte keys select the pointer-bearing packed representation while
-- remaining inline in `StringHashTable`.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;
SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_trivial_group_by_limit_query = 0;
SET optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0;
SET max_threads = 1;
SET max_block_size = 1024;

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
);

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
);

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
);

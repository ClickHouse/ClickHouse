-- arraySlice and arrayElement consume a lazily replicated array produced by lazy ARRAY_JOIN
-- Results must be identical with lazy replication disabled and enabled.

SELECT '-- numeric array, dynamic offset';
SELECT x, big[x], big[-x], arraySlice(big, x, 2), length(big)
FROM (SELECT range(10 + number) AS big, [1, 2, 5, 100] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, big[x], big[-x], arraySlice(big, x, 2), length(big)
FROM (SELECT range(10 + number) AS big, [1, 2, 5, 100] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- string array, constant and dynamic slices';
SELECT x, arraySlice(big, 2, 3), arraySlice(big, -2), arraySlice(big, x), arraySlice(big, x, x), big[2]
FROM (SELECT arrayMap(i -> 's' || toString(i + number), range(5)) AS big, [1, -1, 0, 3] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, arraySlice(big, 2, 3), arraySlice(big, -2), arraySlice(big, x), arraySlice(big, x, x), big[2]
FROM (SELECT arrayMap(i -> 's' || toString(i + number), range(5)) AS big, [1, -1, 0, 3] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- nullable elements';
SELECT x, big[x], arrayElementOrNull(big, x), arraySlice(big, x, 2)
FROM (SELECT arrayMap(i -> if(i % 2 = 0, NULL, i + number), range(6))::Array(Nullable(UInt64)) AS big, [1, 2, 7, -1] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, big[x], arrayElementOrNull(big, x), arraySlice(big, x, 2)
FROM (SELECT arrayMap(i -> if(i % 2 = 0, NULL, i + number), range(6))::Array(Nullable(UInt64)) AS big, [1, 2, 7, -1] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- negative and nullable offsets, negative length';
SELECT x, arraySlice(big, -3, 2), arraySlice(big, 2, -1), arraySlice(big, if(x % 2 = 0, NULL, x)::Nullable(Int64), 2)
FROM (SELECT range(8 + number % 3) AS big, [1, 2, 3, -2] AS r FROM numbers(3)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, arraySlice(big, -3, 2), arraySlice(big, 2, -1), arraySlice(big, if(x % 2 = 0, NULL, x)::Nullable(Int64), 2)
FROM (SELECT range(8 + number % 3) AS big, [1, 2, 3, -2] AS r FROM numbers(3)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- tuple elements and replicated empty arrays';
SELECT x, big[x], arraySlice(big, 1, x), small_arr[x], arraySlice(small_arr, 1, 2)
FROM (SELECT arrayMap(i -> (i + number, 't' || toString(i)), range(4)) AS big, range(number % 2) AS small_arr, [1, 4, 5] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, big[x], arraySlice(big, 1, x), small_arr[x], arraySlice(small_arr, 1, 2)
FROM (SELECT arrayMap(i -> (i + number, 't' || toString(i)), range(4)) AS big, range(number % 2) AS small_arr, [1, 4, 5] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- large array is not materialized per joined row';
SELECT sum(length(arraySlice(big, x, 2))) + sum(length(big[x]))
FROM (SELECT arrayMap(i -> repeat('a', 100) || toString(i + number), range(1000)) AS big, range(50) AS r FROM numbers(100)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0, max_threads = 1, max_block_size = 65536, log_queries = 1, log_comment = '04628_replicated_eager'
FORMAT Null;

SELECT sum(length(arraySlice(big, x, 2))) + sum(length(big[x]))
FROM (SELECT arrayMap(i -> repeat('a', 100) || toString(i + number), range(1000)) AS big, range(50) AS r FROM numbers(100)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1, max_threads = 1, max_block_size = 65536, log_queries = 1, log_comment = '04628_replicated_lazy'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- With lazy replication the big array stays compact (one copy per source row) and only the
-- requested slices/elements are copied, so the peak memory usage must be much lower.
SELECT
    (SELECT max(memory_usage) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04628_replicated_lazy')
    < 0.5 * (SELECT max(memory_usage) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04628_replicated_eager');

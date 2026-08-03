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

SELECT '-- Array(LowCardinality(String)) elements and slices';
SELECT x, toTypeName(big), big[x], big[2], arraySlice(big, x, 2), arraySlice(big, 2, 3), arraySlice(big, -2)
FROM (SELECT arrayMap(i -> 'lc_' || toString((i + number) % 3), range(5))::Array(LowCardinality(String)) AS big, [1, 2, 5, -1] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, toTypeName(big), big[x], big[2], arraySlice(big, x, 2), arraySlice(big, 2, 3), arraySlice(big, -2)
FROM (SELECT arrayMap(i -> 'lc_' || toString((i + number) % 3), range(5))::Array(LowCardinality(String)) AS big, [1, 2, 5, -1] AS r FROM numbers(2)) ARRAY JOIN r AS x
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

SELECT '-- array produced by lazy JOIN replication';
SELECT x, big[x], arraySlice(big, x, 2)
FROM (SELECT 42 AS k, number + 1 AS x FROM numbers(4)) AS l
INNER JOIN (SELECT 42 AS k, materialize(arrayMap(i -> 's' || toString(i), range(5))) AS big) AS r ON l.k = r.k
ORDER BY x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, big[x], arraySlice(big, x, 2)
FROM (SELECT 42 AS k, number + 1 AS x FROM numbers(4)) AS l
INNER JOIN (SELECT 42 AS k, materialize(arrayMap(i -> 's' || toString(i), range(5))) AS big) AS r ON l.k = r.k
ORDER BY x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- replication indexes of type UInt16 (more than 255 rows in a block)';
SELECT sum(big[x + 1]), sum(length(arraySlice(big, x + 1, 3))), sum(arraySlice(big, x + 1, 3)[1])
FROM (SELECT range(number, number + 10) AS big, range(2) AS r FROM numbers(300)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT sum(big[x + 1]), sum(length(arraySlice(big, x + 1, 3))), sum(arraySlice(big, x + 1, 3)[1])
FROM (SELECT range(number, number + 10) AS big, range(2) AS r FROM numbers(300)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- arrayElement with a non-constant Nullable index';
SELECT x, big[if(x % 2 = 0, NULL, x)::Nullable(Int64)], arrayElementOrNull(big, if(x % 2 = 0, NULL, x)::Nullable(Int64))
FROM (SELECT arrayMap(i -> 's' || toString(i + number), range(5)) AS big, [1, 2, 5, -1] AS r FROM numbers(2)) ARRAY JOIN r AS x
SETTINGS enable_lazy_columns_replication = 0;
SELECT x, big[if(x % 2 = 0, NULL, x)::Nullable(Int64)], arrayElementOrNull(big, if(x % 2 = 0, NULL, x)::Nullable(Int64))
FROM (SELECT arrayMap(i -> 's' || toString(i + number), range(5)) AS big, [1, 2, 5, -1] AS r FROM numbers(2)) ARRAY JOIN r AS x
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

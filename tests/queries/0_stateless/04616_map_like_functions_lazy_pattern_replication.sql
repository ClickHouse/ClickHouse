-- mapContainsKeyLike, mapContainsValueLike, mapExtractKeyLike and mapExtractValueLike build an
-- internal lambda-like ColumnFunction that captures the pattern column
-- With enable_lazy_columns_replication the captured pattern must be replicated lazily (wrapped into ColumnReplicated)

SELECT '-- correctness with a non-constant pattern';
SELECT
    mapContainsKeyLike(m, p),
    mapContainsValueLike(m, replaceOne(p, 'k', 'v')),
    mapExtractKeyLike(m, p),
    mapExtractValueLike(m, replaceOne(p, 'k', 'v'))
FROM
(
    SELECT
        mapFromArrays(arrayMap(i -> 'k' || toString(i), range(number % 4)), arrayMap(i -> 'v' || toString(i), range(number % 4))) AS m,
        'k' || toString(number % 3) || '%' AS p
    FROM numbers(6)
)
SETTINGS enable_lazy_columns_replication = 0;

SELECT
    mapContainsKeyLike(m, p),
    mapContainsValueLike(m, replaceOne(p, 'k', 'v')),
    mapExtractKeyLike(m, p),
    mapExtractValueLike(m, replaceOne(p, 'k', 'v'))
FROM
(
    SELECT
        mapFromArrays(arrayMap(i -> 'k' || toString(i), range(number % 4)), arrayMap(i -> 'v' || toString(i), range(number % 4))) AS m,
        'k' || toString(number % 3) || '%' AS p
    FROM numbers(6)
)
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- large non-constant pattern is not physically replicated per map entry';
WITH
    repeat('a', 100000) || toString(number) AS pattern,
    mapFromArrays(arrayMap(i -> 'key' || toString(i), range(20)), range(20)) AS m
SELECT sum(mapContainsKeyLike(m, pattern)) FROM numbers(100)
SETTINGS enable_lazy_columns_replication = 0, max_threads = 1, max_block_size = 65536, log_queries = 1, log_comment = '04616_map_like_eager'
FORMAT Null;

WITH
    repeat('a', 100000) || toString(number) AS pattern,
    mapFromArrays(arrayMap(i -> 'key' || toString(i), range(20)), range(20)) AS m
SELECT sum(mapContainsKeyLike(m, pattern)) FROM numbers(100)
SETTINGS enable_lazy_columns_replication = 1, max_threads = 1, max_block_size = 65536, log_queries = 1, log_comment = '04616_map_like_lazy'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Lazy replication saves one full copy of the pattern exploded per map entry (~200 MB here),
-- so the peak memory usage must be noticeably lower. The queries are identical otherwise.
SELECT
    (SELECT max(memory_usage) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04616_map_like_lazy')
    < 0.9 * (SELECT max(memory_usage) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04616_map_like_eager');

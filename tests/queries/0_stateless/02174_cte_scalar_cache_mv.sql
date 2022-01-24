-- TEST CACHE
CREATE TABLE t1 (i Int64, j Int64) ENGINE = Memory;
INSERT INTO t1 SELECT number, number FROM system.numbers LIMIT 100;
CREATE TABLE t2 (k Int64, l Int64, m Int64, n Int64) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv1 TO t2 AS
    WITH
    (SELECT max(i) FROM t1) AS t1
    SELECT
           t1 as k, -- Using local cache x 4
           t1 as l,
           t1 as m,
           t1 as n
    FROM t1
    LIMIT 5;

-- FIRST INSERT
INSERT INTO t1
WITH
    (SELECT max(i) FROM t1) AS t1
SELECT
       number as i,
       t1 + t1 + t1 AS j -- Using global cache
FROM system.numbers
LIMIT 100
SETTINGS
    min_insert_block_size_rows=5,
    max_insert_block_size=5,
    min_insert_block_size_rows_for_materialized_views=5,
    max_block_size=5,
    max_threads=1;

SELECT k, l, m, n, count()
FROM t2
GROUP BY k, l, m, n
ORDER BY k, l, m, n;

SYSTEM FLUSH LOGS;
-- The main query should have a cache miss and 3 global hits
-- The MV is executed 20 times (100 / 5) and each run does 1 miss and 4 hits to the LOCAL cache
-- In addition to this, to prepare the MV, there is an extra preparation to get the list of columns via
-- InterpreterSelectQuery, which adds 1 miss and 4 global hits (since it uses the global cache)
-- So in total we have:
-- Main query:  1  miss, 3 global
-- Preparation: 1  miss, 4 global
-- Blocks (20): 20 miss, 0 global, 80 local hits

-- TOTAL:       22 miss, 7 global, 80 local
SELECT
    '02177_MV',
    ProfileEvents['ScalarSubqueriesGlobalCacheHit'] as scalar_cache_global_hit,
    ProfileEvents['ScalarSubqueriesLocalCacheHit'] as scalar_cache_local_hit,
    ProfileEvents['ScalarSubqueriesCacheMiss'] as scalar_cache_miss
FROM system.query_log
WHERE
      current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '-- FIRST INSERT\nINSERT INTO t1\n%'
  AND event_date >= yesterday() AND event_time > now() - interval 10 minute;





-- MV SOURCE SHOULD USE LOCAL CACHE
-- MV SOURCE DEEP IN THE CALL SHOULD USE LOCAL CACHE
-- CHECK PERF TEST (EXISTING FOR SCALAR AND MAYBE ADD ONE WITH MVS)

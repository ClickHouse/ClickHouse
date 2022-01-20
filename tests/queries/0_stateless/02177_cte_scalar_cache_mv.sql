-- TEST CACHE
CREATE TABLE t1 (i Int64, j Int64) ENGINE = Memory;
INSERT INTO t1 SELECT number, number FROM system.numbers LIMIT 100;
CREATE TABLE t2 (i Int64, j Int64, k Int64, l Int64) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv1 TO t2 AS
    WITH
    (SELECT max(i) FROM t1) AS t1
    SELECT
           t1 as i,
           t1 as j,
           t1 as k,
           t1 as l
    FROM t1
    LIMIT 5;

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
        max_block_size=5;

-- INSERT INTO t1 SELECT number as i, (SELECT max(i) FROM t1) AS j FROM system.numbers LIMIT 10 OFFSET 100 SETTINGS min_insert_block_size_rows=5, max_insert_block_size=5, max_block_size=5;
-- SELECT max(i) FROM t1;

SELECT i, j, k, l, count() FROM t2 GROUP BY i, j, k, l ORDER BY i, j, k, l;
SYSTEM FLUSH LOGS;
SELECT
    '02177_MV',
    ProfileEvents['ScalarSubqueriesGlobalCacheHit'] as scalar_cache_global_hit,
    ProfileEvents['ScalarSubqueriesLocalCacheHit'] as scalar_cache_local_hit,
    ProfileEvents['ScalarSubqueriesCacheMiss'] as scalar_cache_miss
FROM system.query_log
WHERE
      current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE 'INSERT INTO t1\n%'
  AND event_date >= yesterday() AND event_time > now() - interval 10 minute;

-- MV SOURCE SHOULD USE LOCAL CACHE
-- MV SOURCE DEEP IN THE CALL SHOULD USE LOCAL CACHE
-- CHECK PERF TEST (EXISTING FOR SCALAR AND MAYBE ADD ONE WITH MVS)

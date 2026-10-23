-- Joins with a Nullable right key must return the same rows with the serial and the parallel layout.
-- `parallel_hash_join_threshold = 18446744073709551615` builds the serial layout and `0` the parallel one,
-- also for an ASOF join, which never has a row estimate. Each ASOF run stores its rows in a table.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right_nullable;
DROP TABLE IF EXISTS t_asof_left;
DROP TABLE IF EXISTS t_asof_right;

CREATE TABLE t_left (k UInt64, v String) ENGINE = Memory;
CREATE TABLE t_right_nullable (k Nullable(UInt64), v String) ENGINE = Memory;
CREATE TABLE t_asof_left (k UInt64, ts DateTime, v String) ENGINE = Memory;
CREATE TABLE t_asof_right (k UInt64, ts Nullable(DateTime), tag String) ENGINE = Memory;

INSERT INTO t_left SELECT number AS k, concat('l', toString(number)) AS v FROM numbers(20);
INSERT INTO t_right_nullable
    SELECT if(number % 5 = 0, NULL, toUInt64(number)) AS k, concat('r', toString(number)) AS v
    FROM numbers(25);

INSERT INTO t_asof_left
    SELECT toUInt64(number % 4)         AS k,
           toDateTime('2025-01-01 00:00:00') + number * 60 AS ts,
           concat('al', toString(number)) AS v
    FROM numbers(20);

INSERT INTO t_asof_right
    SELECT toUInt64(number % 4) AS k,
           if(number % 7 = 0, CAST(NULL, 'Nullable(DateTime)'),
              CAST(toDateTime('2025-01-01 00:00:00') + number * 30, 'Nullable(DateTime)')) AS ts,
           concat('ar', toString(number)) AS tag
    FROM numbers(20);

SET join_algorithm = 'hash';
SET max_threads = 4;
SET query_plan_join_swap_table = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;

SELECT '--- INNER nullable right ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
) SETTINGS log_comment = '04107_inner';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
) SETTINGS log_comment = '04107_inner_reverse';

SELECT '--- LEFT nullable right ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
) SETTINGS log_comment = '04107_left';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
) SETTINGS log_comment = '04107_left_reverse';

SELECT '--- RIGHT nullable right (exercises NotJoinedHash partition iteration) ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
) SETTINGS log_comment = '04107_right';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
) SETTINGS log_comment = '04107_right_reverse';

SELECT '--- FULL nullable right ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
) SETTINGS log_comment = '04107_full';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=0
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS parallel_hash_join_threshold=18446744073709551615
) SETTINGS log_comment = '04107_full_reverse';

SELECT '--- ASOF LEFT nullable timestamp ---';
CREATE TABLE asof_serial ENGINE = Memory AS
SELECT l.k AS k, l.ts AS ts, l.v AS v, r.ts AS r_ts, r.tag AS tag
FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
SETTINGS parallel_hash_join_threshold=18446744073709551615, log_comment='04107_asof_left_serial';
CREATE TABLE asof_parallel ENGINE = Memory AS
SELECT l.k AS k, l.ts AS ts, l.v AS v, r.ts AS r_ts, r.tag AS tag
FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
SETTINGS parallel_hash_join_threshold=0, log_comment='04107_asof_left_parallel';
SELECT count() FROM (SELECT * FROM asof_serial EXCEPT SELECT * FROM asof_parallel);
SELECT count() FROM (SELECT * FROM asof_parallel EXCEPT SELECT * FROM asof_serial);

SELECT '--- parallel_hash exact rows: INNER ---';
SELECT l.k, l.v, r.k, r.v
FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
ORDER BY l.k, r.v
SETTINGS parallel_hash_join_threshold=0;

SELECT '--- parallel_hash exact rows: RIGHT (right-only rows have NULL/empty left) ---';
SELECT l.k, l.v, r.k, r.v
FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
ORDER BY r.k NULLS LAST, r.v
SETTINGS join_use_nulls=1, parallel_hash_join_threshold=0;

SELECT '--- parallel_hash exact rows: ASOF (rows where r.ts was NULL must NOT appear) ---';
SELECT l.k, l.ts, r.ts, r.tag
FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
ORDER BY l.k, l.ts
SETTINGS parallel_hash_join_threshold=0;

-- Each EXCEPT query with a Nullable right key must have built one serial and one parallel join, and
-- the serial and the parallel ASOF query must each have built one join of their layout. A `Replicated`
-- database also logs each `CREATE TABLE ... AS SELECT` as a secondary query of its DDL worker, with the
-- same log comment, so only initial queries count.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('04107_inner', '04107_inner_reverse', '04107_left', '04107_left_reverse',
        '04107_right', '04107_right_reverse', '04107_full', '04107_full_reverse',
        '04107_asof_left_serial', '04107_asof_left_parallel')
ORDER BY event_time_microseconds;

DROP TABLE asof_serial;
DROP TABLE asof_parallel;
DROP TABLE t_left;
DROP TABLE t_right_nullable;
DROP TABLE t_asof_left;
DROP TABLE t_asof_right;

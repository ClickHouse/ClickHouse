-- every query is run with the default (`hash`) algorithm and again with
-- `parallel_hash` forced from the very first block (parallel_hash_join_threshold = 1)
-- across multiple partitions; both must produce identical rows.

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

-- Sanity: the workload actually scatters across partitions.
SET max_threads = 4;
SET parallel_hash_join_threshold = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;

-- For each query: run with hash, then parallel_hash, compare with EXCEPT in both directions.
-- A correct implementation prints zero rows for every diff section.

SELECT '--- INNER nullable right ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
);
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
);

SELECT '--- LEFT nullable right ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
);
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l LEFT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
);

SELECT '--- RIGHT nullable right (exercises NotJoinedHash partition iteration) ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
);
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
);

SELECT '--- FULL nullable right ---';
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
);
SELECT count() FROM (
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='parallel_hash'
    EXCEPT
    SELECT l.k, l.v, r.k, r.v FROM t_left l FULL JOIN t_right_nullable r ON l.k = r.k
    SETTINGS join_algorithm='hash'
);

SELECT '--- ASOF LEFT nullable timestamp ---';
SELECT count() FROM (
    SELECT l.k, l.ts, l.v, r.ts, r.tag
    FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
    SETTINGS join_algorithm='hash'
    EXCEPT
    SELECT l.k, l.ts, l.v, r.ts, r.tag
    FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
    SETTINGS join_algorithm='parallel_hash'
);
SELECT count() FROM (
    SELECT l.k, l.ts, l.v, r.ts, r.tag
    FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
    SETTINGS join_algorithm='parallel_hash'
    EXCEPT
    SELECT l.k, l.ts, l.v, r.ts, r.tag
    FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
    SETTINGS join_algorithm='hash'
);

-- Spot-check exact rows (small, hand-verifiable) under parallel_hash:
SELECT '--- parallel_hash exact rows: INNER ---';
SELECT l.k, l.v, r.k, r.v
FROM t_left l INNER JOIN t_right_nullable r ON l.k = r.k
ORDER BY l.k, r.v
SETTINGS join_algorithm='parallel_hash';

SELECT '--- parallel_hash exact rows: RIGHT (right-only rows have NULL/empty left) ---';
SELECT l.k, l.v, r.k, r.v
FROM t_left l RIGHT JOIN t_right_nullable r ON l.k = r.k
ORDER BY r.k NULLS LAST, r.v
SETTINGS join_algorithm='parallel_hash', join_use_nulls=1;

SELECT '--- parallel_hash exact rows: ASOF (rows where r.ts was NULL must NOT appear) ---';
SELECT l.k, l.ts, r.ts, r.tag
FROM t_asof_left l ASOF LEFT JOIN t_asof_right r ON l.k = r.k AND l.ts >= r.ts
ORDER BY l.k, l.ts
SETTINGS join_algorithm='parallel_hash';

DROP TABLE t_left;
DROP TABLE t_right_nullable;
DROP TABLE t_asof_left;
DROP TABLE t_asof_right;

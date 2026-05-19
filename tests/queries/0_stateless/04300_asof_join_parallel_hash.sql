-- Tests that ASOF JOIN with join_algorithm = 'parallel_hash' produces the same
-- result as with join_algorithm = 'hash'. Previously parallel_hash was opted
-- out for ASOF in allowParallelHashJoin.
--
-- Strategy: per-row mutual EXCEPT in both directions. This is strictly stronger
-- than comparing aggregate count/sum and avoids float-summation-order
-- non-determinism (ConcurrentHashJoin materializes rows in a different order
-- than HashJoin, which changes the bit-level result of sum() over floats —
-- semantically identical, but EXCEPT would treat the rows as different).

DROP TABLE IF EXISTS asof_left;
DROP TABLE IF EXISTS asof_right;

CREATE TABLE asof_left  (k UInt32, t UInt32, v Float64) ENGINE = MergeTree ORDER BY (k, t);
CREATE TABLE asof_right (k UInt32, t UInt32, v Float64) ENGINE = MergeTree ORDER BY (k, t);

-- Multi-key, multi-timestamp dataset large enough to actually exercise
-- the parallel build.
INSERT INTO asof_left
SELECT
    toUInt32(keys.k)            AS k,
    toUInt32(tt.t * 7)           AS t,
    toFloat64(keys.k) + toFloat64(tt.t) / 1000 AS v
FROM (SELECT number AS k FROM numbers(500)) AS keys
CROSS JOIN (SELECT number AS t FROM numbers(200)) AS tt;

INSERT INTO asof_right
SELECT
    toUInt32(keys.k)            AS k,
    toUInt32(tt.t * 13)          AS t,
    -toFloat64(keys.k) - toFloat64(tt.t) / 1000 AS v
FROM (SELECT number AS k FROM numbers(500)) AS keys
CROSS JOIN (SELECT number AS t FROM numbers(100)) AS tt;

-- ASOF INNER JOIN: per-row identity in both directions.
SELECT count() FROM (
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF INNER JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'hash'
    EXCEPT
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF INNER JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'parallel_hash'
);

SELECT count() FROM (
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF INNER JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'parallel_hash'
    EXCEPT
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF INNER JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'hash'
);

-- ASOF LEFT JOIN: same shape.
SELECT count() FROM (
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF LEFT JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'hash'
    EXCEPT
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF LEFT JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'parallel_hash'
);

SELECT count() FROM (
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF LEFT JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'parallel_hash'
    EXCEPT
    SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
    FROM asof_left AS l
    ASOF LEFT JOIN asof_right AS r
        ON l.k = r.k AND l.t >= r.t
    SETTINGS join_algorithm = 'hash'
);

DROP TABLE asof_left;
DROP TABLE asof_right;

-- An ASOF JOIN must return the same rows with the serial and the parallel layout. Each join runs once with
-- `parallel_hash_join_threshold = 18446744073709551615` (serial) and once with `0` (parallel), and stores its rows.
-- The two results are compared row by row with EXCEPT in both directions. Comparing count() and sum()
-- would not work: the two builds return rows in different orders, and that changes sum() over floats.

SET max_threads = 4;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS asof_left;
DROP TABLE IF EXISTS asof_right;

CREATE TABLE asof_left  (k UInt32, t UInt32, v Float64) ENGINE = MergeTree ORDER BY (k, t);
CREATE TABLE asof_right (k UInt32, t UInt32, v Float64) ENGINE = MergeTree ORDER BY (k, t);

-- Multi-key, multi-timestamp dataset.
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
CREATE TABLE inner_serial ENGINE = Memory AS
SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left AS l
ASOF INNER JOIN asof_right AS r
    ON l.k = r.k AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '04319_inner_serial';

CREATE TABLE inner_parallel ENGINE = Memory AS
SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left AS l
ASOF INNER JOIN asof_right AS r
    ON l.k = r.k AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04319_inner_parallel';

SELECT count() FROM (SELECT * FROM inner_serial EXCEPT SELECT * FROM inner_parallel);
SELECT count() FROM (SELECT * FROM inner_parallel EXCEPT SELECT * FROM inner_serial);

-- ASOF LEFT JOIN: same shape.
CREATE TABLE left_serial ENGINE = Memory AS
SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left AS l
ASOF LEFT JOIN asof_right AS r
    ON l.k = r.k AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '04319_left_serial';

CREATE TABLE left_parallel ENGINE = Memory AS
SELECT l.k AS k, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left AS l
ASOF LEFT JOIN asof_right AS r
    ON l.k = r.k AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04319_left_parallel';

SELECT count() FROM (SELECT * FROM left_serial EXCEPT SELECT * FROM left_parallel);
SELECT count() FROM (SELECT * FROM left_parallel EXCEPT SELECT * FROM left_serial);

DROP TABLE asof_left;
DROP TABLE asof_right;

-- ASOF JOIN with two equality keys. The parallel build must split the rows by the equality keys
-- only, so that rows with the same (a, b) and different t go to the same bucket. Otherwise probe
-- rows miss their ASOF matches. With one equality key, as above, the ASOF column is never part
-- of the split, so only this case checks it.

DROP TABLE IF EXISTS asof_left2;
DROP TABLE IF EXISTS asof_right2;

CREATE TABLE asof_left2  (a UInt32, b UInt32, t UInt32, v Float64) ENGINE = MergeTree ORDER BY (a, b, t);
CREATE TABLE asof_right2 (a UInt32, b UInt32, t UInt32, v Float64) ENGINE = MergeTree ORDER BY (a, b, t);

INSERT INTO asof_left2
SELECT toUInt32(number % 100), toUInt32((number / 100) % 50), toUInt32(number), toFloat64(number) / 1000
FROM numbers(50000);

INSERT INTO asof_right2
SELECT toUInt32(number % 100), toUInt32((number / 100) % 50), toUInt32(number * 2), -toFloat64(number) / 1000
FROM numbers(50000);

CREATE TABLE two_keys_inner_serial ENGINE = Memory AS
SELECT l.a AS a, l.b AS b, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left2 AS l
ASOF INNER JOIN asof_right2 AS r
    ON l.a = r.a AND l.b = r.b AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '04319_two_keys_inner_serial';

CREATE TABLE two_keys_inner_parallel ENGINE = Memory AS
SELECT l.a AS a, l.b AS b, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left2 AS l
ASOF INNER JOIN asof_right2 AS r
    ON l.a = r.a AND l.b = r.b AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04319_two_keys_inner_parallel';

SELECT count() FROM (SELECT * FROM two_keys_inner_serial EXCEPT SELECT * FROM two_keys_inner_parallel);
SELECT count() FROM (SELECT * FROM two_keys_inner_parallel EXCEPT SELECT * FROM two_keys_inner_serial);

CREATE TABLE two_keys_left_serial ENGINE = Memory AS
SELECT l.a AS a, l.b AS b, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left2 AS l
ASOF LEFT JOIN asof_right2 AS r
    ON l.a = r.a AND l.b = r.b AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 18446744073709551615, log_comment = '04319_two_keys_left_serial';

CREATE TABLE two_keys_left_parallel ENGINE = Memory AS
SELECT l.a AS a, l.b AS b, l.t AS t, l.v AS lv, r.v AS rv
FROM asof_left2 AS l
ASOF LEFT JOIN asof_right2 AS r
    ON l.a = r.a AND l.b = r.b AND l.t >= r.t
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04319_two_keys_left_parallel';

SELECT count() FROM (SELECT * FROM two_keys_left_serial EXCEPT SELECT * FROM two_keys_left_parallel);
SELECT count() FROM (SELECT * FROM two_keys_left_parallel EXCEPT SELECT * FROM two_keys_left_serial);

DROP TABLE asof_left2;
DROP TABLE asof_right2;

-- The first build of each pair has the serial layout and the second the parallel one. A `Replicated`
-- database also logs each `CREATE TABLE ... AS SELECT` as a secondary query of its DDL worker, so only
-- initial queries count.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('04319_inner_serial', '04319_inner_parallel', '04319_left_serial', '04319_left_parallel',
        '04319_two_keys_inner_serial', '04319_two_keys_inner_parallel', '04319_two_keys_left_serial', '04319_two_keys_left_parallel')
ORDER BY event_time_microseconds;

DROP TABLE inner_serial;
DROP TABLE inner_parallel;
DROP TABLE left_serial;
DROP TABLE left_parallel;
DROP TABLE two_keys_inner_serial;
DROP TABLE two_keys_inner_parallel;
DROP TABLE two_keys_left_serial;
DROP TABLE two_keys_left_parallel;

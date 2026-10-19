-- The Join table engine builds its table on the partitioned hash join. Every result is checked
-- against the same data in a Memory table joined with `hash`, as an order-independent fingerprint:
-- the row count and the sum of the row hashes.

SET join_algorithm = 'hash';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS pbnp_ref;
DROP TABLE IF EXISTS pbnp_left;
DROP TABLE IF EXISTS pbnp_j_all;
DROP TABLE IF EXISTS pbnp_j_inner_all;
DROP TABLE IF EXISTS pbnp_j_any;
DROP TABLE IF EXISTS pbnp_j_any_last;
DROP TABLE IF EXISTS pbnp_j_all_nulls;
DROP TABLE IF EXISTS pbnp_j_right;
DROP TABLE IF EXISTS pbnp_j_full;
DROP TABLE IF EXISTS pbnp_j_inner_any;
DROP TABLE IF EXISTS pbnp_j_semi;
DROP TABLE IF EXISTS pbnp_j_anti;
DROP TABLE IF EXISTS pbnp_j_str;
DROP TABLE IF EXISTS pbnp_j_two_keys;

-- 500 keys with 20 rows each; `v` is the insertion order, so the first row of a key is `min(v)` and
-- the last `max(v)`. The left side has twice as many keys, so half of them find nothing.
CREATE TABLE pbnp_ref (k UInt32, s String, v Int64) ENGINE = Memory;
INSERT INTO pbnp_ref SELECT number % 500, concat('s', toString(number)), toInt64(number) FROM numbers(10000);
CREATE TABLE pbnp_left (k UInt32, x Int32) ENGINE = Memory;
INSERT INTO pbnp_left SELECT number, toInt32(number * 3) FROM numbers(1000);

CREATE TABLE pbnp_j_all (k UInt32, s String, v Int64) ENGINE = Join(ALL, LEFT, k);
CREATE TABLE pbnp_j_inner_all (k UInt32, s String, v Int64) ENGINE = Join(ALL, INNER, k);
CREATE TABLE pbnp_j_any (k UInt32, s String, v Int64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE pbnp_j_any_last (k UInt32, s String, v Int64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_any_take_last_row = 1;
CREATE TABLE pbnp_j_all_nulls (k UInt32, s String, v Int64) ENGINE = Join(ALL, LEFT, k) SETTINGS join_use_nulls = 1;
CREATE TABLE pbnp_j_right (k UInt32, s String, v Int64) ENGINE = Join(ALL, RIGHT, k);
CREATE TABLE pbnp_j_full (k UInt32, s String, v Int64) ENGINE = Join(ALL, FULL, k);
CREATE TABLE pbnp_j_inner_any (k UInt32, s String, v Int64) ENGINE = Join(ANY, INNER, k);
CREATE TABLE pbnp_j_semi (k UInt32, s String, v Int64) ENGINE = Join(SEMI, LEFT, k);
CREATE TABLE pbnp_j_anti (k UInt32, s String, v Int64) ENGINE = Join(ANTI, LEFT, k);

-- Several small blocks, in insertion order, so the rows of a key arrive in separate inserts and the
-- chains of a key grow across blocks.
INSERT INTO pbnp_j_all SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_inner_all SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_any SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_any_last SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_all_nulls SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_right SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_full SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_inner_any SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_semi SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;
INSERT INTO pbnp_j_anti SELECT * FROM pbnp_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 700;

SELECT 'counts', count() FROM pbnp_j_all;
SELECT 'stored rows', count() FROM (SELECT * FROM pbnp_j_all);

SELECT 'all left',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN pbnp_j_all AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN pbnp_ref AS r USING (k)));

SELECT 'all inner',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL INNER JOIN pbnp_j_inner_all AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL INNER JOIN pbnp_ref AS r USING (k)));

-- ANY keeps the first row of a key, or the last under `join_any_take_last_row`.
SELECT 'any left first',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ANY LEFT JOIN pbnp_j_any AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s_agg AS s, r.v_agg AS v FROM pbnp_left AS l ANY LEFT JOIN (SELECT k, argMin(s, v) AS s_agg, min(v) AS v_agg FROM pbnp_ref GROUP BY k) AS r USING (k)));

SELECT 'any left last',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ANY LEFT JOIN pbnp_j_any_last AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s_agg AS s, r.v_agg AS v FROM pbnp_left AS l ANY LEFT JOIN (SELECT k, argMax(s, v) AS s_agg, max(v) AS v_agg FROM pbnp_ref GROUP BY k) AS r USING (k)));

SELECT 'any inner',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ANY INNER JOIN pbnp_j_inner_any AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s_agg AS s, r.v_agg AS v FROM pbnp_left AS l ANY INNER JOIN (SELECT k, argMin(s, v) AS s_agg, min(v) AS v_agg FROM pbnp_ref GROUP BY k) AS r USING (k)));

SELECT 'all left nulls',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN pbnp_j_all_nulls AS r USING (k) SETTINGS join_use_nulls = 1))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN pbnp_ref AS r USING (k) SETTINGS join_use_nulls = 1));

-- RIGHT and FULL: the rows never matched come out of the shared table through the query's own used flags.
SELECT 'all right',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT k, l.x, r.s, r.v FROM (SELECT * FROM pbnp_left WHERE k % 3 != 0) AS l ALL RIGHT JOIN pbnp_j_right AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT k, l.x, r.s, r.v FROM (SELECT * FROM pbnp_left WHERE k % 3 != 0) AS l ALL RIGHT JOIN pbnp_ref AS r USING (k)));

SELECT 'all full',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT k, l.x, r.s, r.v FROM (SELECT * FROM pbnp_left WHERE k % 3 != 0) AS l ALL FULL JOIN pbnp_j_full AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT k, l.x, r.s, r.v FROM (SELECT * FROM pbnp_left WHERE k % 3 != 0) AS l ALL FULL JOIN pbnp_ref AS r USING (k)));

SELECT 'semi left',
    (SELECT (count(), sum(cityHash64(k, x))) FROM (SELECT l.k, l.x FROM pbnp_left AS l SEMI LEFT JOIN pbnp_j_semi AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x))) FROM (SELECT l.k, l.x FROM pbnp_left AS l SEMI LEFT JOIN pbnp_ref AS r USING (k)));

SELECT 'anti left',
    (SELECT (count(), sum(cityHash64(k, x))) FROM (SELECT l.k, l.x FROM pbnp_left AS l ANTI LEFT JOIN pbnp_j_anti AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x))) FROM (SELECT l.k, l.x FROM pbnp_left AS l ANTI LEFT JOIN pbnp_ref AS r USING (k)));

-- Reading a Join table lists its stored rows: all of them for ALL, one per key for ANY.
SELECT 'read all', (SELECT (count(), sum(cityHash64(k, s, v))) FROM pbnp_j_all) = (SELECT (count(), sum(cityHash64(k, s, v))) FROM pbnp_ref);
SELECT 'read any first', (SELECT (count(), sum(cityHash64(k, s, v))) FROM pbnp_j_any) = (SELECT (count(), sum(cityHash64(k, s_agg, v_agg))) FROM (SELECT k, argMin(s, v) AS s_agg, min(v) AS v_agg FROM pbnp_ref GROUP BY k));
SELECT 'read any last', (SELECT (count(), sum(cityHash64(k, s, v))) FROM pbnp_j_any_last) = (SELECT (count(), sum(cityHash64(k, s_agg, v_agg))) FROM (SELECT k, argMax(s, v) AS s_agg, max(v) AS v_agg FROM pbnp_ref GROUP BY k));

-- A mutation rebuilds the table from the rows it keeps.
ALTER TABLE pbnp_j_all DELETE WHERE k % 7 = 0;
SELECT 'after delete', count() FROM pbnp_j_all;
SELECT 'all left after delete',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN pbnp_j_all AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN (SELECT * FROM pbnp_ref WHERE k % 7 != 0) AS r USING (k)));

-- Detaching and attaching replays the persisted blocks into a new table.
DETACH TABLE pbnp_j_all;
ATTACH TABLE pbnp_j_all;
SELECT 'after attach', count() FROM pbnp_j_all;
SELECT 'all left after attach',
    (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN pbnp_j_all AS r USING (k)))
    = (SELECT (count(), sum(cityHash64(k, x, s, v))) FROM (SELECT l.k, l.x, r.s, r.v FROM pbnp_left AS l ALL LEFT JOIN (SELECT * FROM pbnp_ref WHERE k % 7 != 0) AS r USING (k)));

TRUNCATE TABLE pbnp_j_all;
SELECT 'after truncate', count() FROM pbnp_j_all;
SELECT 'joined after truncate', count() FROM (SELECT * FROM pbnp_left AS l ALL LEFT JOIN pbnp_j_all AS r USING (k) WHERE r.v != 0);
INSERT INTO pbnp_j_all SELECT * FROM pbnp_ref;
SELECT 'refilled', count() FROM pbnp_j_all;

-- String and composite keys take other map types; the string key includes the empty string, which is
-- the zero key of its map.
CREATE TABLE pbnp_j_str (s String, v Int64) ENGINE = Join(ALL, LEFT, s);
INSERT INTO pbnp_j_str SELECT if(number % 100 = 0, '', concat('k', toString(number % 300))), toInt64(number) FROM numbers(3000);
SELECT 'string key',
    (SELECT (count(), sum(cityHash64(s, v))) FROM (SELECT l.s, r.v FROM (SELECT if(number % 50 = 0, '', concat('k', toString(number))) AS s FROM numbers(600)) AS l ALL LEFT JOIN pbnp_j_str AS r USING (s)))
    = (SELECT (count(), sum(cityHash64(s, v))) FROM (SELECT l.s, r.v FROM (SELECT if(number % 50 = 0, '', concat('k', toString(number))) AS s FROM numbers(600)) AS l ALL LEFT JOIN (SELECT if(number % 100 = 0, '', concat('k', toString(number % 300))) AS s, toInt64(number) AS v FROM numbers(3000)) AS r USING (s)));
SELECT 'read string key', (SELECT (count(), sum(cityHash64(s, v))) FROM pbnp_j_str) = (SELECT (count(), sum(cityHash64(s, v))) FROM (SELECT if(number % 100 = 0, '', concat('k', toString(number % 300))) AS s, toInt64(number) AS v FROM numbers(3000)));

CREATE TABLE pbnp_j_two_keys (a UInt64, b UInt64, v Int64) ENGINE = Join(ALL, INNER, a, b);
INSERT INTO pbnp_j_two_keys SELECT number % 40, number % 30, toInt64(number) FROM numbers(2400);
SELECT 'two keys',
    (SELECT (count(), sum(cityHash64(a, b, v))) FROM (SELECT l.a, l.b, r.v FROM (SELECT number % 50 AS a, number % 35 AS b FROM numbers(700)) AS l ALL INNER JOIN pbnp_j_two_keys AS r USING (a, b)))
    = (SELECT (count(), sum(cityHash64(a, b, v))) FROM (SELECT l.a, l.b, r.v FROM (SELECT number % 50 AS a, number % 35 AS b FROM numbers(700)) AS l ALL INNER JOIN (SELECT number % 40 AS a, number % 30 AS b, toInt64(number) AS v FROM numbers(2400)) AS r USING (a, b)));
SELECT 'read two keys', (SELECT (count(), sum(cityHash64(a, b, v))) FROM pbnp_j_two_keys) = (SELECT (count(), sum(cityHash64(a, b, v))) FROM (SELECT number % 40 AS a, number % 30 AS b, toInt64(number) AS v FROM numbers(2400)));

DROP TABLE pbnp_ref;
DROP TABLE pbnp_left;
DROP TABLE pbnp_j_all;
DROP TABLE pbnp_j_inner_all;
DROP TABLE pbnp_j_any;
DROP TABLE pbnp_j_any_last;
DROP TABLE pbnp_j_all_nulls;
DROP TABLE pbnp_j_right;
DROP TABLE pbnp_j_full;
DROP TABLE pbnp_j_inner_any;
DROP TABLE pbnp_j_semi;
DROP TABLE pbnp_j_anti;
DROP TABLE pbnp_j_str;
DROP TABLE pbnp_j_two_keys;

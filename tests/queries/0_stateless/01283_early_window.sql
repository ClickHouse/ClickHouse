-- Prepare data
DROP TABLE IF EXISTS vv;
CREATE TABLE vv ( `id` Int32, `lcns` LowCardinality(Nullable(String)), `ni64` Nullable(Int64), `ns` Nullable(String), `ls` LowCardinality(String), `dt` DateTime, `d` Decimal(18, 5), `s` String, `alns` Array(LowCardinality(Nullable(String))), `ans` Array(Nullable(String)), `ui64` UInt64, `i32` Int32, `i64` Int64, f float) ENGINE = MergeTree() PARTITION BY id ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO vv VALUES (1, 'a', 1, 'a', 'a', 1, 1.1, 'a', ['a','b'], ['a','b'], 1, 1, 1, 1.1);
INSERT INTO vv VALUES (1, 'b', 2, 'b', 'b', 2, 2.2, 'b', ['b','c'], ['b','c'], 2, 2, 2, 2.2);

DROP TABLE IF EXISTS vv_dist;
CREATE TABLE vv_dist (`id` Int32, `lcns` LowCardinality(Nullable(String)), `ni64` Nullable(Int64), `ns` Nullable(String), `ls` LowCardinality(String), `dt` DateTime, `d` Decimal(18, 5), `s` String, `alns` Array(LowCardinality(Nullable(String))), `ans` Array(Nullable(String)), `ui64` UInt64, `i32` Int32, `i64` Int64, f float) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 'vv');

-- Supported types
SELECT windowLast(dt, dt, dt) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, id, id) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, lcns, lcns) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, ni64, ni64) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, ns, ns) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, ls, ls) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, dt, dt) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, s, s) AS last FROM vv ORDER BY last;
-- SELECT windowLast(dt, d, d) AS last FROM vv;        -- not supported
-- SELECT windowLast(dt, alns, alns) AS last FROM vv;  -- not supported
-- SELECT windowLast(dt, ans , ans) AS last FROM vv;   -- not supported
SELECT windowLast(dt, ui64, ui64) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, i32, i32) AS last FROM vv ORDER BY last;
SELECT windowLast(dt, i64, i64) AS last FROM vv ORDER BY last;

-- Group by & Order by & Where
SELECT windowLast(dt, id, id), count() FROM vv GROUP BY windowLast(dt, id, id);
SELECT windowLast(dt, id, id) AS last, uniq(id) FROM vv GROUP BY last;
SELECT windowLast(dt, id, id) AS last, uniq(id) FROM vv GROUP BY last ORDER BY last;

SELECT windowLast(dt, dt, dt) FROM vv ORDER BY windowLast(dt, dt, dt);
SELECT windowLast(dt, dt, dt) AS last FROM vv ORDER BY last;

SELECT windowLast(dt, dt, dt), f AS last FROM vv WHERE f > 1.5;

SELECT windowLast(dt, id, id) AS last, uniq(id) FROM vv WHERE f > 1.5 GROUP BY last ORDER BY last;

SELECT '1', windowLast(dt, id + 10, id) AS last, id FROM vv ORDER BY last;
SELECT '2', windowLast(dt, id + 10, id) AS last, * FROM vv ORDER BY dt;
SELECT '3', windowLast(dt, id + 10, id) AS last, id, * FROM vv ORDER BY dt;
SELECT '4', windowLast(dt, id + 10, id) AS last, * FROM vv WHERE f > 1.5;

-- Multiple functions
SELECT 'mf1', windowFirst(dt, dt, dt) AS first, windowLast(dt, id, id) FROM vv ORDER BY first;
SELECT 'mf2', windowFirst(dt, dt, dt) AS first, windowLast(dt, id, id) AS second FROM vv ORDER BY first;
SELECT 'mf3', windowFirst(dt, dt, dt) AS first, windowLast(dt, id, id) AS second, count() FROM vv GROUP BY first, second;

-- (DIST) Supported types
SELECT windowLast(dt, dt, dt) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, id, id) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, lcns, lcns) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, ni64, ni64) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, ns, ns) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, ls, ls) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, dt, dt) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, s, s) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, ui64, ui64) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, i32, i32) AS last FROM vv_dist ORDER BY last;
SELECT windowLast(dt, i64, i64) AS last FROM vv_dist ORDER BY last;

-- (DIST) Group by & Order by & Where
SELECT windowLast(dt, id, id), count() FROM vv_dist GROUP BY windowLast(dt, id, id);
SELECT windowLast(dt, id, id) AS last, uniq(id) FROM vv_dist GROUP BY last;
SELECT windowLast(dt, id, id) AS last, uniq(id) FROM vv_dist GROUP BY last ORDER BY last;

SELECT windowLast(dt, dt, dt) FROM vv_dist ORDER BY windowLast(dt, dt, dt);
SELECT windowLast(dt, dt, dt) AS last FROM vv_dist ORDER BY last;

SELECT windowLast(dt, dt, dt), f AS last FROM vv_dist WHERE f > 1.5;

SELECT windowLast(dt, id, id) AS last, uniq(id) FROM vv_dist WHERE f > 1.5 GROUP BY last ORDER BY last;

SELECT '1', windowLast(dt, id + 10, id) AS last, id FROM vv_dist;
SELECT '2', windowLast(dt, id + 10, id) AS last, * FROM vv_dist  ORDER BY dt;
SELECT '3', windowLast(dt, id + 10, id) AS last, id, * FROM vv_dist ORDER BY dt;
SELECT '4', windowLast(dt, id + 10, id) AS last, * FROM vv_dist WHERE f > 1.5;
SELECT '5', windowLast(dt, id + 10, id) AS last, * FROM vv_dist WHERE f > 1.5 ORDER BY last;

-- (DIST) Multiple functions
SELECT 'mf1', windowFirst(dt, dt, dt) AS first, windowLast(dt, id, id) FROM vv_dist ORDER BY first;
SELECT 'mf2', windowFirst(dt, dt, dt) AS first, windowLast(dt, id, id) AS second FROM vv_dist ORDER BY second;
SELECT 'mf3', windowFirst(dt, dt, dt) AS first, windowLast(dt, id, id) AS second, count() FROM vv_dist GROUP BY first, second ORDER BY first;
WITH sum(i64) AS _sum SELECT 'mf4', windowFirst(dt, id, id) AS first, windowLast(dt, id, id) AS last, uniq(id) _uniq, _sum FROM vv_dist GROUP BY first, last ORDER BY _sum desc;

-- Validate data
DROP TABLE IF EXISTS early_window_test;
CREATE TABLE early_window_test (`dt` DateTime, `id` String, `v` Int32) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;
INSERT INTO early_window_test VALUES (1, 'a', 1) (2, 'a', 2) (3, 'a', 3) (1, 'b', 3) (2, 'b', 2) (3, 'b', 1);
INSERT INTO early_window_test VALUES (1, 'A', 10) (2, 'A', 20) (3, 'A', 30) (1, 'B', 10) (2, 'B', 20) (3, 'B', 30);

SELECT windowLast(dt, v, id) AS last, count(), uniq(id) AS uv, arraySort(groupArray(id)) FROM early_window_test WHERE v > 0 GROUP BY last ORDER BY uv;

DROP TABLE IF EXISTS early_window_test;
DROP TABLE IF EXISTS vv_dist;
DROP TABLE IF EXISTS vv;

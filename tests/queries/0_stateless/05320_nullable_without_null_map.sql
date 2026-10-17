-- A `Nullable` column without NULLs may have no null map in memory.
-- Check that the columns with and without a null map are processed consistently,
-- including the blocks with and without NULLs mixed in one query.

DROP TABLE IF EXISTS t_nullable;

CREATE TABLE t_nullable (id UInt64, x Nullable(UInt64), s Nullable(String), y Nullable(UInt32), arr Array(Nullable(UInt8)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 100;

-- The first part has no NULLs.
INSERT INTO t_nullable SELECT number, number % 7, toString(number % 5), number % 3, [number % 2, number % 3] FROM numbers(1000);
-- The second part has NULLs in some granules only.
INSERT INTO t_nullable SELECT 1000 + number, if(number >= 500 AND number % 11 = 0, NULL, number % 7), if(number >= 700 AND number % 13 = 0, NULL, if(number % 9 = 0, '', toString(number % 5))), if(number >= 900, NULL, number % 3), [if(number % 17 = 0, NULL, number % 2)] FROM numbers(1000);

SET max_block_size = 100, max_threads = 1;

SELECT 'filter';
SELECT count(), sum(x), countIf(x IS NULL), countIf(s IS NULL), countIf(y IS NULL) FROM t_nullable WHERE x > 2;
SELECT count(), sum(x), count(x), count(s), count(y) FROM t_nullable;

SELECT 'group by one key';
SELECT x, count() FROM t_nullable GROUP BY x ORDER BY x;
SELECT s, count() FROM t_nullable GROUP BY s ORDER BY s;

SELECT 'group by two keys';
SELECT x, y, count() FROM t_nullable GROUP BY x, y ORDER BY x, y;
SELECT x, s, count() FROM t_nullable GROUP BY x, s ORDER BY x, s;
SELECT s, arr, count() FROM t_nullable GROUP BY s, arr ORDER BY s, arr;

SELECT 'group by two level';
SELECT x, s, count() FROM t_nullable GROUP BY x, s ORDER BY x, s SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

SELECT 'distinct';
SELECT DISTINCT x FROM t_nullable ORDER BY x;
SELECT DISTINCT x, s FROM t_nullable ORDER BY x, s;
SELECT uniqExact(x), uniqExact(s), uniqExact(x, s), uniq(x, y) FROM t_nullable;

SELECT 'order by';
SELECT id, x FROM t_nullable ORDER BY x NULLS FIRST, id LIMIT 5;
SELECT id, x FROM t_nullable ORDER BY x DESC NULLS LAST, id LIMIT 5;
SELECT id, s FROM t_nullable ORDER BY s DESC NULLS FIRST, id DESC LIMIT 5;
SELECT id, x, y FROM t_nullable ORDER BY y NULLS FIRST, x, id DESC LIMIT 5;

SELECT 'aggregate functions';
SELECT sum(x), avg(x), min(x), max(x), min(s), max(s), arraySort(groupUniqArray(y)), sumIf(x, id % 2 = 0), argMax(s, (x, id)), round(corr(x, y), 6) FROM t_nullable;
SELECT id % 3 AS k, sum(x), max(s), count(y), avg(y) FROM t_nullable GROUP BY k ORDER BY k;

SELECT 'functions';
SELECT sum(x + 1), sum(x * y), countIf(isNull(x + y)), sum(coalesce(x, 100)), sum(ifNull(y, 1000)), sum(length(concat(s, 'a'))) FROM t_nullable;
SELECT sum(cityHash64(x)), sum(cityHash64(s)), sum(cityHash64(x, s)) FROM t_nullable;
SELECT x, s, x + 1, concat(s, '!'), isNull(x), toString(x) FROM t_nullable WHERE id IN (0, 1, 1500, 1511, 1989) ORDER BY id;

SELECT 'in and join';
SELECT count() FROM t_nullable WHERE x IN (SELECT x FROM t_nullable WHERE id < 1000 AND x < 3);
SELECT count() FROM t_nullable WHERE (x, s) IN (SELECT x, s FROM t_nullable WHERE id >= 1000 AND x < 3);
SELECT count(), sum(t2.y) FROM t_nullable AS t1 INNER JOIN (SELECT x, y FROM t_nullable WHERE id % 100 = 0) AS t2 ON t1.x = t2.x;
SELECT count(), countIf(t2.x IS NULL) FROM t_nullable AS t1 LEFT JOIN (SELECT DISTINCT x FROM t_nullable WHERE id < 1000 AND x < 3) AS t2 ON t1.x = t2.x;

SELECT 'union of blocks with and without NULLs';
SELECT k, count() FROM
(
    SELECT toNullable(number % 3) AS k FROM numbers(10)
    UNION ALL
    SELECT if(number % 2 = 0, NULL, number % 3) AS k FROM numbers(10)
)
GROUP BY k ORDER BY k;

SELECT k, s, count() FROM
(
    SELECT toNullable(number % 3) AS k, toNullable(toString(number % 2)) AS s FROM numbers(10)
    UNION ALL
    SELECT if(number % 2 = 0, NULL, number % 3) AS k, if(number % 3 = 0, NULL, toString(number % 2)) AS s FROM numbers(10)
)
GROUP BY k, s ORDER BY k, s;

SELECT 'string functions and comparisons that can be read from subcolumns';
SELECT countIf(s = ''), countIf(s != ''), countIf(empty(s)), countIf(notEmpty(s)), sum(length(s)), countIf(length(s) IS NULL), countIf((s = '') IS NULL) FROM t_nullable;
SELECT id % 3 AS k, avg(length(s)), count() FROM t_nullable WHERE s != '' GROUP BY k ORDER BY k;

SELECT 'sum of an expression with a constant';
SELECT sum(x + 1), sum(x - 1), sum(1 + x), sum(10 - x), sum(y + 2) FROM t_nullable;
SELECT id % 2 AS k, sum(x + 1), sum(x), count(x) FROM t_nullable WHERE id >= 1500 AND x IS NULL GROUP BY k ORDER BY k;

SELECT 'subcolumns';
SELECT count(), sum(x.null), sum(s.null), sum(y.null) FROM t_nullable;
SELECT sum(x.null) FROM t_nullable WHERE id < 1000;

SELECT 'merge';
OPTIMIZE TABLE t_nullable FINAL;
SELECT count(), sum(x), count(x), count(s), count(y), sum(x.null) FROM t_nullable;
SELECT x, count() FROM t_nullable GROUP BY x ORDER BY x;

DROP TABLE t_nullable;

SELECT 'no null map is created for a table without NULLs';
DROP TABLE IF EXISTS t_nullable_no_nulls;
CREATE TABLE t_nullable_no_nulls (x Nullable(UInt64), s Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_no_nulls SELECT number, toString(number % 10) FROM numbers(10000);
SELECT sum(x), count(x), max(s), sum(x + 1), countIf(s = '5') FROM t_nullable_no_nulls WHERE x > 10 SETTINGS log_comment = '05320_nullable_without_null_map_no_nulls';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['NullableNullMapMaterializations'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05320_nullable_without_null_map_no_nulls' AND type = 'QueryFinish' AND event_date >= yesterday();
DROP TABLE t_nullable_no_nulls;

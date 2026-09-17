-- A hash table far smaller than the L2 cache is probed without prefetching
-- (`getMinBytesForPrefetchInJoin`). That is one table read per probe row and a linear walk on a
-- collision. Sequential and hashed probe keys place the rows differently in the table. Queries that
-- read no right column and queries that read `b.v` take different code paths. Results must match
-- `hash`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS t_fl_build;
DROP TABLE IF EXISTS t_fl_seq;
DROP TABLE IF EXISTS t_fl_hashed;

CREATE TABLE t_fl_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT number * 3 AS k, number + 1000000 AS v FROM numbers(1000);
-- Sequential keys: about one third hit. Hashed keys: the same share, spread over the cells.
CREATE TABLE t_fl_seq ENGINE = MergeTree ORDER BY tuple() AS
SELECT number % 3000 AS k, number AS p FROM numbers(200000);
CREATE TABLE t_fl_hashed ENGINE = MergeTree ORDER BY tuple() AS
SELECT cityHash64(number) % 3000 AS k, number AS p FROM numbers(200000);

SELECT 'sequential keys, inner, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, left, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, semi left, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, anti left, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_seq AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, inner, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, left, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, right, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'sequential keys, full, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_seq AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, inner, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, left, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, semi left, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, anti left, no right columns', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.p)) FROM t_fl_hashed AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, inner, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, left, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, right, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'hashed keys, full, right columns read', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_fl_hashed AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT '-- first rows';
SELECT p.p, b.k, b.v FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k ORDER BY p.p LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT p.p, b.k, b.v FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k ORDER BY p.p LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';

DROP TABLE t_fl_build;
DROP TABLE t_fl_seq;
DROP TABLE t_fl_hashed;

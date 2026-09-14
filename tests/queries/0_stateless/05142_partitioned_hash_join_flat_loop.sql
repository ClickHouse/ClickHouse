-- A build far below the prefetch threshold is probed by the flat loop, not the ring: one cell per
-- probe row, placement from the top bits of the hash, the linear walk on a busy cell. Sequential
-- and hashed probe keys exercise both placement patterns; the queries with no right column output
-- take the loop's no-refs instantiation, the ones reading `b.v` the recording one. Results must match `hash`.
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

SELECT '-- sequential keys, no right columns';
SELECT count() FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(p.p) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(p.p) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count() FROM t_fl_seq AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_fl_seq AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count() FROM t_fl_seq AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_fl_seq AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';

SELECT '-- sequential keys, right columns read';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_seq AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';

SELECT '-- hashed keys, no right columns';
SELECT count() FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(p.p) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(p.p) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count() FROM t_fl_hashed AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_fl_hashed AS p SEMI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count() FROM t_fl_hashed AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_fl_hashed AS p ANTI LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';

SELECT '-- hashed keys, right columns read';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p LEFT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p RIGHT JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_fl_hashed AS p FULL JOIN t_fl_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';

SELECT '-- first rows';
SELECT p.p, b.k, b.v FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k ORDER BY p.p LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT p.p, b.k, b.v FROM t_fl_hashed AS p INNER JOIN t_fl_build AS b ON p.k = b.k ORDER BY p.p LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';

DROP TABLE t_fl_build;
DROP TABLE t_fl_seq;
DROP TABLE t_fl_hashed;


DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (s String) ENGINE = MergeTree ORDER BY s
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t1 SELECT if (number % 13 = 0, toString(number), '') FROM numbers(2000);

CREATE TABLE t2 (s String) ENGINE = MergeTree ORDER BY s
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t2 SELECT if (number % 14 = 0, toString(number), '') FROM numbers(2000);

SELECT countIf(ignore(*) == 0) FROM t1 JOIN t2 ON t1.s = t2.s;

SET join_algorithm = 'full_sorting_merge', max_rows_in_set_to_optimize_join = 100_000;

SELECT countIf(ignore(*) == 0) FROM t1 JOIN t2 ON t1.s = t2.s;

DROP TABLE t1;
DROP TABLE t2;

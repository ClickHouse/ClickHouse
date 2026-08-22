DROP TABLE IF EXISTS t_sparse_pk;
DROP TABLE IF EXISTS t_full_pk;

CREATE TABLE t_sparse_pk (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0, index_granularity = 1;

INSERT INTO t_sparse_pk VALUES (0, 'a'), (0, 'b'), (1, ''), (2, ''), (2, 'e'), (3, 'f'), (4, 'g');

SET force_primary_key = 1;

SELECT k, s FROM t_sparse_pk WHERE k = 2 ORDER BY k, s;
SELECT k, s FROM t_sparse_pk WHERE k = 0 OR k = 3 ORDER BY k, s;

DROP TABLE IF EXISTS t_sparse_pk;

CREATE TABLE t_sparse_pk (k UInt64, v UInt64 CODEC(NONE))
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0, index_granularity = 30;

CREATE TABLE t_full_pk (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.1, index_granularity = 30;

INSERT INTO t_sparse_pk SELECT number % 10, number % 4 = 0 FROM numbers(1000);
INSERT INTO t_full_pk SELECT number % 10, number % 4 = 0 FROM numbers(1000);

INSERT INTO t_sparse_pk SELECT number % 10, number % 6 = 0 FROM numbers(1000);
INSERT INTO t_full_pk SELECT number % 10, number % 6 = 0 FROM numbers(1000);

SELECT count(v), sum(v) FROM t_sparse_pk WHERE k = 0;
SELECT count(v), sum(v) FROM t_full_pk WHERE k = 0;

SELECT count(v), sum(v) FROM t_sparse_pk WHERE k = 0 OR k = 3 OR k = 7 OR k = 8;
SELECT count(v), sum(v) FROM t_full_pk WHERE k = 0 OR k = 3 OR k = 7 OR k = 8;

SET force_primary_key = 0;

SELECT (k = NULL) OR (k = 1000) FROM t_sparse_pk LIMIT 3;
SELECT range(k) FROM t_sparse_pk ORDER BY k LIMIT 3;

DROP TABLE IF EXISTS t_sparse_pk;
DROP TABLE IF EXISTS t_full_pk;

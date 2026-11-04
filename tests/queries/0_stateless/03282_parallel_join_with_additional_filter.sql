CREATE TABLE t1 (
    key UInt32,
    a UInt32,
    attr String
) ENGINE = MergeTree ORDER BY key;

CREATE TABLE t2 (
    key UInt32,
    a UInt32,
    attr String
) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 (key, a, attr) VALUES (1, 10, 'alpha'), (2, 15, 'beta'), (3, 20, 'gamma');
INSERT INTO t2 (key, a, attr) VALUES (1, 5, 'ALPHA'), (2, 10, 'beta'), (4, 25, 'delta');

SET allow_experimental_join_condition = 1;
SET enable_analyzer = 1;
SET max_threads = 16;

SELECT '---- HASH';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.key < t2.a OR t1.a % 2 = 0) ORDER BY ALL SETTINGS join_algorithm = 'hash';

SELECT '---- PARALLEL HASH';
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.key < t2.a OR t1.a % 2 = 0) ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

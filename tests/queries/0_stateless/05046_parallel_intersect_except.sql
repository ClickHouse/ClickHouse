-- INTERSECT / EXCEPT hash-scatter both inputs by the whole row into max_threads partitions and
-- process the partitions in parallel. The results must not depend on the number of threads.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (a Nullable(UInt64), s String, l LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
CREATE TABLE t_right (a Nullable(UInt64), s String, l LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

-- Values 0..9 with multiplicity a % 3 + 1 on the left, and 5..14 with multiplicity 2 on the right;
-- `s` and `l` are functions of `a`. `s` is mostly empty so that it is stored as Sparse.
INSERT INTO t_left SELECT a, if(a = 7, 'x', ''), toString(a % 2) FROM (SELECT number % 10 AS a FROM numbers_mt(30) WHERE intDiv(number, 10) <= number % 10 % 3);
INSERT INTO t_right SELECT a, if(a = 7, 'x', ''), toString(a % 2) FROM (SELECT number % 10 + 5 AS a FROM numbers_mt(20));
INSERT INTO t_left VALUES (NULL, '', '0'), (NULL, '', '0'), (NULL, '', '1');
INSERT INTO t_right VALUES (NULL, '', '0'), (NULL, '', '1'), (NULL, '', '1');

SELECT 'INTERSECT ALL';
SELECT * FROM (SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right) ORDER BY ALL SETTINGS max_threads = 4;
SELECT 'INTERSECT DISTINCT';
SELECT * FROM (SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right) ORDER BY ALL SETTINGS max_threads = 4;
SELECT 'EXCEPT ALL';
SELECT * FROM (SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right) ORDER BY ALL SETTINGS max_threads = 4;
SELECT 'EXCEPT DISTINCT';
SELECT * FROM (SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right) ORDER BY ALL SETTINGS max_threads = 4;

SELECT 'same result for 1, 4 and 16 threads';
SELECT
    (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 16),
    (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 16),
    (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 16),
    (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM (SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right) ORDER BY ALL) SETTINGS max_threads = 16);

SELECT 'multiset semantics on a large input';
SELECT count(), sum(number) FROM (SELECT number % 1000 AS number FROM numbers_mt(100000) INTERSECT ALL SELECT number % 700 FROM numbers_mt(50000)) SETTINGS max_threads = 16;
SELECT count(), sum(number) FROM (SELECT number % 1000 AS number FROM numbers_mt(100000) EXCEPT ALL SELECT number % 700 FROM numbers_mt(50000)) SETTINGS max_threads = 16;
SELECT count(), sum(number) FROM (SELECT number % 1000 AS number FROM numbers_mt(100000) INTERSECT DISTINCT SELECT number % 700 FROM numbers_mt(50000)) SETTINGS max_threads = 16;
SELECT count(), sum(number) FROM (SELECT number % 1000 AS number FROM numbers_mt(100000) EXCEPT DISTINCT SELECT number % 700 FROM numbers_mt(50000)) SETTINGS max_threads = 16;

SELECT 'empty sides';
SELECT count() FROM (SELECT number FROM numbers_mt(1000) INTERSECT ALL SELECT number FROM numbers_mt(0)) SETTINGS max_threads = 4;
SELECT count() FROM (SELECT number FROM numbers_mt(0) INTERSECT ALL SELECT number FROM numbers_mt(1000)) SETTINGS max_threads = 4;
SELECT count() FROM (SELECT number FROM numbers_mt(1000) EXCEPT ALL SELECT number FROM numbers_mt(0)) SETTINGS max_threads = 4;
SELECT count() FROM (SELECT number FROM numbers_mt(0) EXCEPT ALL SELECT number FROM numbers_mt(1000)) SETTINGS max_threads = 4;

SELECT 'constant columns';
SELECT * FROM (SELECT 1 AS c, number FROM numbers_mt(10) INTERSECT ALL SELECT materialize(1), number FROM numbers_mt(5, 10)) ORDER BY ALL SETTINGS max_threads = 4;
SELECT * FROM (SELECT 1 AS c, number FROM numbers_mt(10) EXCEPT ALL SELECT 1, number FROM numbers_mt(5, 10)) ORDER BY ALL SETTINGS max_threads = 4;
SELECT * FROM (SELECT 1 AS c, 2 AS d INTERSECT ALL SELECT 1, 2) ORDER BY ALL SETTINGS max_threads = 4;
SELECT * FROM (SELECT 1 AS c, 2 AS d EXCEPT ALL SELECT 1, 3) ORDER BY ALL SETTINGS max_threads = 4;

SELECT 'chains and mixes with UNION';
SELECT * FROM (SELECT number FROM numbers_mt(20) EXCEPT ALL SELECT number FROM numbers_mt(5) EXCEPT ALL SELECT number * 2 FROM numbers_mt(10)) ORDER BY ALL SETTINGS max_threads = 4;
SELECT * FROM (SELECT number FROM numbers_mt(10) INTERSECT ALL SELECT number FROM numbers_mt(5, 10) INTERSECT ALL SELECT number FROM numbers_mt(7, 10)) ORDER BY ALL SETTINGS max_threads = 4;
SELECT * FROM (SELECT number FROM numbers_mt(10) INTERSECT DISTINCT SELECT number FROM numbers_mt(5, 10) UNION ALL SELECT number FROM numbers_mt(3)) ORDER BY ALL SETTINGS max_threads = 4;
SELECT * FROM (SELECT number FROM numbers_mt(10) UNION ALL SELECT number FROM numbers_mt(3) EXCEPT DISTINCT SELECT number FROM numbers_mt(5)) ORDER BY ALL SETTINGS max_threads = 4;

SELECT 'totals';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers_mt(30) GROUP BY k WITH TOTALS INTERSECT ALL SELECT number % 3, 10 FROM numbers_mt(3)) ORDER BY ALL SETTINGS max_threads = 4;

SELECT 'distinct with size limits';
SELECT * FROM (SELECT number % 5 AS n FROM numbers_mt(100) INTERSECT DISTINCT SELECT number % 7 FROM numbers_mt(100)) ORDER BY ALL SETTINGS max_threads = 4, max_rows_in_distinct = 1000;

DROP TABLE t_left;
DROP TABLE t_right;

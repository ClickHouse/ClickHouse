SET allow_experimental_analyzer = 1;
SET optimize_fuse_sum_count_avg = 1;

DROP TABLE IF EXISTS fuse_tbl;

CREATE TABLE fuse_tbl(a Nullable(Int8), b Int8) Engine = Log;

INSERT INTO fuse_tbl VALUES (1, 1), (2, 2), (NULL, 3);

SELECT sum(a + 1), sum(b), count(b), avg(b), count(a + 1), sum(a + 2), count(a) from fuse_tbl SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(a + 1), sum(b), count(b), avg(b), count(a + 1), sum(a + 2), count(a) from fuse_tbl;

EXPLAIN QUERY TREE run_passes = 1 SELECT sum(a + 1), sum(b), count(b), avg(b), count(a + 1), sum(a + 2), count(a) from fuse_tbl;
EXPLAIN QUERY TREE run_passes = 1 SELECT sum(a), avg(b) from fuse_tbl;

SELECT sum(x), count(x), avg(x) FROM (SELECT number :: Decimal32(0) AS x FROM numbers(0)) SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(x), count(x), avg(x) FROM (SELECT number :: Decimal32(0) AS x FROM numbers(0));

SELECT sum(x), count(x), avg(x), toTypeName(sum(x)), toTypeName(count(x)), toTypeName(avg(x)) FROM (SELECT number :: Decimal32(0) AS x FROM numbers(10)) SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(x), count(x), avg(x), toTypeName(sum(x)), toTypeName(count(x)), toTypeName(avg(x)) FROM (SELECT number :: Decimal32(0) AS x FROM numbers(10));

DROP TABLE fuse_tbl;

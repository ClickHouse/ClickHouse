DROP TABLE IF EXISTS fuse_tbl__fuzz_35;

CREATE TABLE fuse_tbl__fuzz_35 (`a` UInt8, `b` Nullable(Int16)) ENGINE = Log;
INSERT INTO fuse_tbl__fuzz_35 SELECT number, number + 1 FROM numbers(1000);

set allow_experimental_analyzer = 0, optimize_syntax_fuse_functions = 1, optimize_fuse_sum_count_avg = 1;

SELECT quantile(0.5)(b), quantile(0.9)(b) FROM (SELECT x + 2147483648 AS b FROM (SELECT quantile(0.5)(b) AS x FROM fuse_tbl__fuzz_35) GROUP BY x) FORMAT Null;

DROP TABLE IF EXISTS fuse_tbl__fuzz_35;

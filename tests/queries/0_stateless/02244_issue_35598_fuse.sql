SELECT sum(x), count(x), avg(x) FROM (SELECT number :: Decimal32(0) AS x FROM numbers(0))
SETTINGS optimize_syntax_fuse_functions = 0, optimize_fuse_sum_count_avg = 0;

SELECT sum(x), count(x), avg(x) FROM (SELECT number :: Decimal32(0) AS x FROM numbers(0))
SETTINGS optimize_syntax_fuse_functions = 1, optimize_fuse_sum_count_avg = 1;
